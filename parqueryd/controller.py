import binascii
import gc
import logging
import os
import random
import socket
import time
import traceback

import pyarrow as pa
import redis
import zmq
from parquery.transport import deserialize_pa_table, serialize_pa_table

import parqueryd
import parqueryd.config
from parqueryd.exceptions import RPCError
from parqueryd.messages import msg_factory, Message, WorkerRegisterMessage, ErrorMessage, \
    BusyMessage, DoneMessage, StopMessage, TicketDoneMessage
from parqueryd.tool import ens_bytes, ens_unicode
from parqueryd.util import get_my_ip, bind_to_random_port

POLLING_TIMEOUT = 500  # timeout in ms : how long to wait for network poll, this also affects frequency of seeing new nodes
DEAD_WORKER_TIMEOUT = 60  # time in seconds that we wait for a worker to respond before being removed
HEARTBEAT_INTERVAL = 2  # time in seconds between doing heartbeats
MIN_CALCWORKER_COUNT = 0.1  # percentage of workers that should ONLY do calcs and never do downloads to prevent download swamping
RUNFILES_LOCATION = '/srv/'  # Location to write a .pid and .address file


class ControllerNode(object):
    def __init__(self, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.INFO, azure_conn_string=None):

        self.redis_url = redis_url
        self.redis_server = redis.from_url(redis_url)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 500)
        self.socket.setsockopt(zmq.ROUTER_MANDATORY, 1)  # Paranoid for debugging purposes
        self.socket.setsockopt(zmq.SNDTIMEO, 1000)  # Short timeout
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN | zmq.POLLOUT)

        self.node_name = socket.gethostname()
        self.address = ens_bytes(bind_to_random_port(self.socket, 'tcp://' + get_my_ip(),
                                                     min_port=14300, max_port=14399, max_tries=100))
        with open(os.path.join(RUNFILES_LOCATION, 'parqueryd_controller.address'), 'w') as F:
            F.write(str(self.address))
        with open(os.path.join(RUNFILES_LOCATION, 'parqueryd_controller.pid'), 'w') as F:
            F.write(str(os.getpid()))

        self.logger = parqueryd.logger.getChild('controller').getChild(ens_unicode(self.address))
        self.logger.setLevel(loglevel)

        self.msg_count_in = 0
        self.rpc_results = []  # buffer of results that are ready to be returned to callers
        self.rpc_segments = {}  # Certain RPC calls get split and divided over workers, this dict tracks the original RPCs
        self.worker_map = {}  # maintain a list of connected workers TODO get rid of unresponsive ones...
        self.files_map = {}  # shows on which workers a file is available on
        self.worker_out_messages = {None: []}  # A dict of buffers, used to round-robin based on message affinity
        self.worker_out_messages_sequence = [None]  # used to round-robin the outgoing messages
        self.is_running = True
        self.last_heartbeat = 0
        self.others = {}  # A dict of other Controllers running on other DQE nodes
        self.start_time = time.time()
        self.azure_conn_string = azure_conn_string

    def send(self, addr, msg_buf, is_rpc=False):
        try:
            if addr == self.address:
                self.handle_peer(addr, msg_factory(msg_buf))
                return
            if is_rpc:
                tmp = [addr, b'', msg_buf]
            else:
                tmp = [addr, msg_buf]
            self.socket.send_multipart(tmp)
        except zmq.ZMQError as ze:
            self.logger.warning("Problem with %s: %s" % (addr, ze))

    def connect_to_others(self):
        # Make sure own address is still registered
        self.redis_server.sadd(parqueryd.config.REDIS_SET_KEY, self.address)
        # get list of other running controllers and connect to them
        all_servers = self.redis_server.smembers(parqueryd.config.REDIS_SET_KEY)
        all_servers.remove(self.address)
        # Connect to new other controllers we don't know about yet
        for x in all_servers:
            if x not in self.others:
                self.logger.info('Connecting to %s', x)
                self.socket.connect(x)
                self.others[x] = {'connect_time': time.time()}
            else:
                msg = Message({'payload': 'info', 'result': self.get_info()})
                try:
                    self.socket.send_multipart([x, msg.to_json()])
                except zmq.error.ZMQError as e:
                    self.logger.critical('Removing %s due to %s' % (x, e))
                    self.redis_server.srem(parqueryd.config.REDIS_SET_KEY, x)
                    del self.others[x]

        # Disconnect from controllers not in current set
        for x in list(self.others.keys()):  # iterate over a copy of keys so we can remove entries
            if x not in all_servers:
                self.logger.critical('Disconnecting from %s' % x)
                try:
                    del self.others[x]
                    self.socket.disconnect(x)
                except zmq.error.ZMQError as e:
                    self.logger.exception(e)

    def heartbeat(self):
        if time.time() - self.last_heartbeat > HEARTBEAT_INTERVAL:
            self.connect_to_others()
            self.last_heartbeat = time.time()

    def find_free_worker(self, needs_local=False, filename=None):
        # Pick a random worker_id to send a message, TODO add some kind of load-balancing
        free_workers = []
        free_local_workers = []

        for worker_id, worker in self.worker_map.copy().items():
            # ignore downloader workers
            if worker.get('workertype') in ('download', 'moveparquet'):
                continue

            if worker.get('busy'):
                continue

            if filename and (worker_id not in self.files_map.get(filename, [])):
                continue

            free_workers.append(worker_id)
            if needs_local and worker.get('node') != self.node_name:
                continue
            if worker.get('node') == self.node_name:
                free_local_workers.append(worker_id)
        # if there are no free workers at all, just bail
        if not free_workers:
            return None

        if needs_local:
            if free_local_workers:
                return random.choice(free_local_workers)
            else:
                return None

        return random.choice(free_workers)

    def process_sink_results(self):
        while self.rpc_results:
            msg = self.rpc_results.pop()

            # If this was a message to be combined, there should be a parent_token
            if 'parent_token' in msg:
                parent_token = msg['parent_token']
                if parent_token not in self.rpc_segments:
                    logging.debug('Orphaned msg segment %s probably an error was raised elsewhere' % parent_token)
                    continue
                original_rpc = self.rpc_segments.get(parent_token)

                if isinstance(msg, ErrorMessage):
                    self.logger.debug('Error message %s' % msg.get('payload'))
                    # Delete this entire message segments, if it still exists
                    if parent_token in self.rpc_segments:
                        del self.rpc_segments[parent_token]
                        # If any of the segment workers return an error,
                        # Send the exception on to the calling RPC
                        msg['token'] = parent_token
                        msg_id = binascii.unhexlify(parent_token)
                        try:
                            self.send(msg_id, msg.to_json(), is_rpc=True)
                        except TypeError:
                            self.logger.debug('Bytes error with ' + str(msg))
                    # and continue the processing
                    continue

                args, kwargs = msg.get_args_kwargs()
                filename = args[0]

                if msg.get('data'):
                    result_table = deserialize_pa_table(msg['data'])
                    # check if the result is valid
                    if result_table.num_rows == 0:
                        result_table = None
                else:
                    result_table = None

                original_rpc['results'][filename] = result_table
                logging.debug('Received result for token ' + ens_unicode(parent_token) +
                              ' for file ' + ens_unicode(filename) + ' (' +
                              str(len(original_rpc['results'])) + '/' + str(len(original_rpc['filenames']))
                              + ')')

                if len(original_rpc['results']) == len(original_rpc['filenames']):
                    # Check to see that there are no filenames with no Result yet
                    # TODO as soon as any workers gives an error abort the whole enchilada

                    # We have received all the segment, send a reply to RPC caller
                    del msg
                    msg = original_rpc['msg']

                    # if finished, aggregate the result to a combined arrow table
                    pa_tables = [result_table for result_table in original_rpc['results'].values() if
                                 result_table is not None]

                    logging.debug('Last result received for token ' + ens_unicode(parent_token) +
                                  ' and sending the combined result of ' +
                                  str(len(pa_tables)) + ' relevant results')

                    if len(pa_tables) == 0:
                        msg['data'] = None
                    elif len(pa_tables) == 1:
                        msg['data'] = serialize_pa_table(pa_tables[0])
                    else:
                        msg['data'] = serialize_pa_table(pa.concat_tables(pa_tables))

                    del self.rpc_segments[parent_token]
                else:
                    # This was a segment result move on
                    continue

            msg_id = binascii.unhexlify(msg.get('token'))
            if 'data' in msg:
                if not msg['data']:
                    msg['data'] = b''
                self.send(msg_id, msg['data'], is_rpc=True)
                gc.collect()
            else:
                try:
                    self.send(msg_id, msg.to_json(), is_rpc=True)
                except TypeError:
                    self.logger.debug('Bytes error with ' + str(msg))
            self.logger.debug('RPC Msg handled: %s' % msg.get('payload', '?'))
            self.logger.debug('Pending results: %s' % str(len(self.rpc_segments)))

    def handle_out(self):
        # If there have been new affinity keys added, rotate them
        for k, v in self.worker_out_messages.items():
            if v and (k not in self.worker_out_messages_sequence):
                self.worker_out_messages_sequence.append(k)

        # remove unused affinities

        remove_list = [x for x in self.worker_out_messages_sequence if x not in self.worker_out_messages]
        for affinity in remove_list:
            self.worker_out_messages_sequence.remove(affinity)

        if not self.worker_out_messages_sequence:
            # nothing to do
            return

        nextq_key = self.worker_out_messages_sequence.pop(0)
        nextq = self.worker_out_messages.get(nextq_key)
        self.worker_out_messages_sequence.append(nextq_key)
        if not nextq:
            if nextq_key:  # Only delete those for which there is an affinity, the None q is default and should stay
                del self.worker_out_messages[nextq_key]
            return  # the next buffer is empty just return and try the next one in the next round

        msg = nextq[0]
        worker_id = msg.get('worker_id')

        # Take into account on which worker a filename is available
        filename = msg.get('filename')

        # find a worker that is free
        if worker_id == '__needs_local__':
            worker_id = self.find_free_worker(needs_local=True, filename=filename)
        elif worker_id is None:
            worker_id = self.find_free_worker(filename=filename)

        if not worker_id:
            # self.logger.debug('No free workers at this time')
            return

        msg = nextq.pop(0)

        # TODO Add a tracking of which requests have been sent out to the worker, and do retries with timeouts
        self.worker_map[worker_id]['last_sent'] = time.time()
        self.worker_map[worker_id]['busy'] = True
        try:
            self.send(worker_id, msg.to_json())
        except TypeError:
            self.logger.debug('Bytes error with ' + str(msg))

    def handle_in(self):
        self.msg_count_in += 1
        data = self.socket.recv_multipart()
        binary, sender = None, None  # initialise outside for edge cases
        if len(data) == 3:
            if data[1] == b'':  # This is a RPC call from a zmq.REQ socket
                sender, _blank, msg_buf = data
                self.handle_rpc(sender, msg_factory(msg_buf))
                return
            sender, msg_buf, binary = data
        elif len(data) == 2:  # This is an internode call from another zmq.ROUTER, a Controller or Worker
            sender, msg_buf = data
        msg = msg_factory(msg_buf)
        if binary:
            msg['data'] = binary
        if sender in self.others:
            self.handle_peer(sender, msg)
        else:
            try:
                self.handle_worker(sender, msg)
            except Exception as e:
                raise RPCError(e)


    def handle_peer(self, sender, msg):
        if msg.isa('loglevel'):
            args, kwargs = msg.get_args_kwargs()
            loglevel = {'info': logging.INFO, 'debug': logging.DEBUG}.get(args[0], logging.INFO)
            self.logger.setLevel(loglevel)
            self.logger.info("Set loglevel to %s" % loglevel)
        elif msg.isa('info'):
            # Another node registered with you and is sending some info
            data = msg.get('result', {})
            addr = data.get('address')
            node = data.get('node')
            uptime = data.get('uptime')
            if addr and node:
                self.others[ens_bytes(addr)]['node'] = node
                self.others[ens_bytes(addr)]['uptime'] = uptime
            else:
                self.logger.critical("bogus Info message received from %s %s", sender, msg)
        else:
            self.logger.debug("Got a msg but don't know what to do with it %s" % msg)

    def handle_worker(self, worker_id, msg):

        # TODO Make a distinction on the kind of message received and act accordingly
        msg['worker_id'] = worker_id

        # TODO If worker not in worker_map due to a dead_worker cull (calculation too a long time...)
        # request a new WorkerRegisterMessage from that worker...
        if not msg.isa(WorkerRegisterMessage) and worker_id not in self.worker_map:
            self.send(worker_id, Message({'payload': 'info'}).to_json())

        self.worker_map.setdefault(worker_id, {})['last_seen'] = time.time()

        if msg.isa(WorkerRegisterMessage):
            # self.logger.debug('Worker registered %s', ens_unicode(worker_id))
            for filename in msg.get('data_files', []):
                self.files_map.setdefault(filename, set()).add(worker_id)
            self.worker_map[worker_id]['node'] = msg.get('node', '...')
            self.worker_map[worker_id]['uptime'] = msg.get('uptime', 0)
            self.worker_map[worker_id]['pid'] = msg.get('pid', '?')
            self.worker_map[worker_id]['workertype'] = msg.get('workertype', '?')
            return

        if msg.isa(BusyMessage):
            self.logger.debug('Worker %s sent BusyMessage', ens_unicode(worker_id))
            self.worker_map[worker_id]['busy'] = True
            return

        if msg.isa(DoneMessage):
            self.logger.debug('Worker %s sent DoneMessage', ens_unicode(worker_id))
            self.worker_map[worker_id]['busy'] = False
            return

        if msg.isa(StopMessage):
            self.remove_worker(worker_id)
            return

        if msg.isa(TicketDoneMessage):
            # Go through download tickets and see if there are any local RPCs waiting for it
            # Check the ticket in this number, if it is in the self.rpc_segments[ticket] of this controller
            # there is a RPC call waiting for it, so also answer that one

            ticket = msg.get('ticket')
            if ticket in self.rpc_segments:
                msg2 = self.rpc_segments[ticket]
                if 'token' in msg2:
                    # TODO Consider passing in some extra information to this ticket so the worker
                    # that did the downloading can pass in diagnostics etc.
                    self.rpc_results.append(msg2)
                del self.rpc_segments[ticket]
            return

        if 'token' in msg:
            # A message might have been passed on to a worker for processing and needs to be returned to
            #  the relevant caller so it goes in the rpc_results list
            dbg_msg = 'Appending to queue a result for token ' + ens_unicode(msg['token'])
            if msg.get('parent_token'):
                dbg_msg += ' (' + ens_unicode(msg['parent_token']) + ')'
            self.logger.debug(dbg_msg)
            self.rpc_results.append(msg)

    def handle_rpc(self, sender, msg):
        # RPC calls have a binary identity set, hexlify it to make it readable and serializable
        msg_id = binascii.hexlify(sender).decode()
        msg['token'] = msg_id
        # self.logger.debug('RPC received %s' % msg_id)

        result = "Sorry, I don't understand you"
        if msg.isa('ping'):
            result = 'pong'
        elif msg.isa('loglevel'):
            args, kwargs = msg.get_args_kwargs()
            if not args:
                result = "You need to specify a loglevel as first arg"
            else:
                m = msg.copy()
                del m['token']
                for x in self.others:
                    self.send(x, m.to_json())
                for x in self.worker_map:
                    self.send(x, m.to_json())
                self.handle_peer(None, m)
                result = "OK, loglevel set passed along"
        elif msg.isa('info'):
            result = self.get_info()
        elif msg.isa('kill'):
            result = self.kill()
        elif msg.isa('killworkers'):
            result = self.killworkers()
        elif msg.isa('killall'):
            result = self.killall()
        elif msg.isa('download'):
            result = self.setup_download(msg)
        elif msg.isa('readfile'):
            self.worker_out_messages[None].append(msg.copy())
            result = None
        elif msg.isa('execute_code'):
            args, kwargs = msg.get_args_kwargs()
            if 'function' not in kwargs:
                result = 'Error, function kwarg is missing!'
            else:
                self.worker_out_messages[None].append(msg.copy())
                if kwargs.get('wait', False):
                    result = None
                else:
                    result = 'OK, %s dispatched' % kwargs['function']
        elif msg['payload'] in ('sleep',):
            args, kwargs = msg.get_args_kwargs()
            if args:
                if type(args[0]) is int:
                    self.worker_out_messages[None].append(msg.copy())
                    result = None
                else:
                    for x in args[0]:
                        mm = msg.copy()
                        del mm['token']
                        affinity = kwargs.get('affinity')
                        mm.set_args_kwargs([x], kwargs)
                        self.worker_out_messages.setdefault(affinity, []).append(mm)
                    result = 'Multi-sleep returning immediately'
            else:
                result = "Sleep needs an int or list of ints as arg[0]"
        elif msg['payload'] in ('groupby',):
            # if result is not None something happened, return to caller immediately
            result = self.handle_calc_message(msg)

        if result is not None:
            msg['result'] = result
            self.rpc_results.append(msg)

    def setup_download(self, msg):
        args, kwargs = msg.get_args_kwargs()
        filenames = kwargs.get('filenames')
        bucket = kwargs.get('bucket')
        wait = kwargs.get('wait', False)
        if not (filenames and bucket):
            return "A download needs kwargs: (filenames=, bucket=)"

        # Turn filenames into URLs
        if self.azure_conn_string:
            filenames = ['azure://%s/%s' % (bucket, filename) for filename in filenames]
        else:
            filenames = ['s3://%s/%s' % (bucket, filename) for filename in filenames]

        ticket = binascii.hexlify(os.urandom(8)).decode()  # track all downloads using a ticket
        for filename in filenames:

            # get all node names from others + self
            # TODO if the download happens to start when you are disconnected from others this is an issue...
            nodes = [x.get('node') for x in self.others.values()]
            nodes.append(self.node_name)

            for node in nodes:
                # A progress slot contains a timestamp_filesize
                progress_slot = '%s_%s' % (
                    time.time() - 60,
                    -1)  # give the slot a timestamp of now -1 minute so we can see when it was created
                node_filename_slot = '%s_%s' % (node, filename)
                self.redis_server.hset(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot,
                                       progress_slot)

        if wait:
            msg['result'] = ticket
            self.rpc_segments[ticket] = msg
            return None

        return ticket

    def handle_calc_message(self, msg):
        args, kwargs = msg.get_args_kwargs()
        affinity = kwargs.get('affinity')

        if len(args) != 4:
            return 'Error, No correct args given, expecting: ' + \
                   'path_list, groupby_col_list, measure_col_list, where_terms_list'

        # Make sure that all filenames are available before any messages are sent
        filenames = args[0]
        filenames = [filename for filename in filenames if filename and filename in self.files_map]
        if not filenames:
            # if we return an empty result
            return ''

        rpc_segment = {
            'msg': msg,
            'results': {},
            'filenames': {x: None for x in filenames}
        }

        parent_token = msg['token']

        for filename in filenames:
            # make a new message
            worker_msg = msg.copy()
            worker_msg['parent_token'] = parent_token
            worker_msg['filename'] = filename

            # make parameters
            params = {
                'args': list(args),
                'kwargs': kwargs
            }
            params['args'][0] = filename
            worker_msg['params'] = params

            # Make up a new token for the message sent to the workers, and collect the responses using that id
            new_token = binascii.hexlify(os.urandom(8)).decode()
            worker_msg['token'] = new_token
            rpc_segment['filenames'][filename] = new_token
            self.worker_out_messages.setdefault(affinity, []).append(worker_msg)

        self.rpc_segments[parent_token] = rpc_segment

    def killall(self):
        self.killworkers()
        m = Message({'payload': 'kill'})
        for x in self.others:
            self.send(x, m.to_json(), is_rpc=True)
        self.kill()
        return 'dood'

    def kill(self):
        # unregister with the Redis set
        self.redis_server.srem(parqueryd.config.REDIS_SET_KEY, self.address)
        self.is_running = False
        return 'harakiri...'

    def killworkers(self):
        # Send a kill message to each of our workers
        for x in self.worker_map:
            self.send(x, Message({'payload': 'kill', 'token': 'kill'}).to_json())
        return 'hai!'

    def get_info(self):
        data = {'msg_count_in': self.msg_count_in,
                'node': self.node_name,
                'workers': {k.decode(): v for k, v in self.worker_map.items()},
                'worker_out_messages': [(k, len(v)) for k, v in self.worker_out_messages.items()],
                'last_heartbeat': self.last_heartbeat,
                'address': self.address.decode(),
                'others': {k.decode(): v for k, v in self.others.items()},
                'rpc_results_len': len(self.rpc_results),
                'uptime': int(time.time() - self.start_time),
                'start_time': self.start_time
                }
        return data

    def remove_worker(self, worker_id):
        self.logger.warning("Removing worker %s", worker_id)
        if worker_id in self.worker_map:
            del self.worker_map[worker_id]
        for worker_set in self.files_map.values():
            if worker_id in worker_set:
                worker_set.remove(worker_id)

    def free_dead_workers(self):
        now = time.time()
        for worker_id, worker in self.worker_map.copy().items():
            if (now - worker.get('last_seen', now)) > DEAD_WORKER_TIMEOUT:
                self.remove_worker(worker_id)

    def go(self):
        self.logger.info('[#############################>. Starting .<#############################]')

        while self.is_running:
            try:
                time.sleep(0.001)
                self.heartbeat()
                self.free_dead_workers()
                for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                    if event & zmq.POLLIN:
                        self.handle_in()
                    if event & zmq.POLLOUT:
                        self.handle_out()
                self.process_sink_results()
            except KeyboardInterrupt:
                self.logger.debug('Keyboard Interrupt')
                self.kill()
            except RPCError as e:
                self.logger.exception(e)
                # raise in controller would be bad (?)
                # raise e 
            except:
                self.logger.exception("Exception %s" % traceback.format_exc())

        self.logger.info('Stopping')
        for x in (os.path.join(RUNFILES_LOCATION, 'parqueryd_controller.pid'),
                  os.path.join(RUNFILES_LOCATION, 'parqueryd_controller.address')):
            if os.path.exists(x):
                os.remove(x)
