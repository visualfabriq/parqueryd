import binascii
import datetime
import errno
import gc
import glob
import importlib
import json
import logging
import os
import random
import shutil
import signal
import socket
import time
import traceback
from ssl import SSLError

import boto3
import psutil
import redis
import smart_open
import zmq
from azure.storage.blob import BlobClient
from parquery.aggregate import aggregate_pq
from parquery.transport import serialize_pa_table

import parqueryd.config
from parqueryd.messages import msg_factory, WorkerRegisterMessage, ErrorMessage, BusyMessage, StopMessage, \
    DoneMessage, TicketDoneMessage
from parqueryd.tool import rm_file_or_dir, ens_bytes

DATA_FILE_EXTENSION = '.parquet'
# timeout in ms : how long to wait for network poll, this also affects frequency of seeing new controllers and datafiles
POLLING_TIMEOUT = 5000
# how often in seconds to send a WorkerRegisterMessage
WRM_DELAY = 20
MAX_MEMORY_KB = 2 * (2 ** 20)  # Max memory of 2GB, in Kilobytes
DOWNLOAD_DELAY = 5  # how often in seconds to check for downloads


class WorkerBase(object):
    def __init__(self, data_dir=parqueryd.config.DEFAULT_DATA_DIR, redis_url='redis://127.0.0.1:6379/0',
                 loglevel=logging.DEBUG,
                 restart_check=True, azure_conn_string=None):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception("Datadir %s is not a valid directory" % data_dir)
        self.worker_id = binascii.hexlify(os.urandom(8)).decode()
        self.node_name = socket.gethostname()
        self.data_dir = data_dir
        self.data_files = set()
        self.restart_check = restart_check
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 500)
        self.socket.identity = bytes((self.worker_id).encode("utf-8"))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN | zmq.POLLOUT)
        self.redis_server = redis.from_url(redis_url)
        self.controllers = {}  # Keep a dict of timestamps when you last spoke to controllers
        self.check_controllers()
        self.last_wrm = 0
        self.start_time = time.time()
        self.logger = parqueryd.logger.getChild('worker ' + str(self.worker_id))
        self.logger.setLevel(loglevel)
        self.msg_count = 0
        signal.signal(signal.SIGTERM, self.term_signal())
        self.running = False
        self.azure_conn_string = azure_conn_string

    def term_signal(self):
        def _signal_handler(signum, frame):
            self.logger.info("Received TERM signal, stopping.")
            self.running = False

        return _signal_handler

    def send(self, addr, msg):
        try:
            if 'data' in msg:
                data = msg['data']
                del msg['data']
                self.socket.send_multipart([addr, msg.to_json(), data])
            else:
                self.socket.send_multipart([addr, msg.to_json()])
        except zmq.ZMQError as ze:
            self.logger.critical("Problem with %s: %s" % (str(addr), ze))

    def check_controllers(self):
        # Check the Redis set of controllers to see if any new ones have appeared,
        # Also register with them if so.
        listed_controllers = list(self.redis_server.smembers(parqueryd.config.REDIS_SET_KEY))
        current_controllers = []
        new_controllers = []
        for k in list(self.controllers.keys()):
            if k not in listed_controllers:
                del self.controllers[k]
                self.socket.disconnect(k)
            else:
                current_controllers.append(k)

        new_controllers = [c for c in listed_controllers if c not in current_controllers]
        for controller_address in new_controllers:
            self.socket.connect(controller_address)
            self.controllers[controller_address] = {'last_seen': 0, 'last_sent': 0,
                                                    'address': controller_address.decode()}

    def check_datafiles(self):
        has_new_files = False
        replacement_data_files = set()
        for data_file in [filename for filename in os.listdir(self.data_dir) if filename.endswith(DATA_FILE_EXTENSION)]:
            if data_file not in self.data_files:
                has_new_files = True
            replacement_data_files.add(data_file)
        self.data_files = replacement_data_files
        return has_new_files

    def prepare_wrm(self):
        wrm = WorkerRegisterMessage()
        wrm['worker_id'] = self.worker_id
        wrm['node'] = self.node_name
        wrm['data_files'] = list(self.data_files)
        wrm['data_dir'] = self.data_dir
        wrm['controllers'] = list(self.controllers.values())
        wrm['uptime'] = int(time.time() - self.start_time)
        wrm['msg_count'] = self.msg_count
        wrm['pid'] = os.getpid()
        wrm['workertype'] = self.workertype
        return wrm

    def heartbeat(self):
        time.sleep(0.001)  # to prevent too tight loop
        since_last_wrm = time.time() - self.last_wrm
        if since_last_wrm > WRM_DELAY:
            self.check_controllers()
            has_new_files = self.check_datafiles()
            self.last_wrm = time.time()
            wrm = self.prepare_wrm()
            for controller, data in self.controllers.items():
                if has_new_files or (time.time() - data['last_seen'] > WRM_DELAY):
                    self.send(controller, wrm)
                    data['last_sent'] = time.time()
                    self.logger.debug("heartbeat to %s", data['address'])

    def handle_in(self):
        try:
            tmp = self.socket.recv_multipart()
        except zmq.Again:
            return
        if len(tmp) != 2:
            self.logger.critical('Received a msg with len != 2, something seriously wrong. ')
            return
        sender, msg_buf = tmp
        self.logger.info("Received message from sender %s", sender)
        msg = msg_factory(msg_buf)

        data = self.controllers.get(sender)
        if not data:
            self.logger.critical('Received a msg from %s - this is an unknown sender' % sender)
            return
        data['last_seen'] = time.time()
        self.logger.debug('Received from %s', sender)

        # TODO Notify Controllers that we are busy, no more messages to be sent
        # The above busy notification is not perfect as other messages might be on their way already
        # but for long-running queries it will at least ensure other controllers
        # don't try and overuse this node by filling up a queue
        busy_msg = BusyMessage()
        self.send_to_all(busy_msg)

        try:
            tmp = self.handle(msg)
        except Exception:
            tmp = ErrorMessage(msg)
            tmp['payload'] = traceback.format_exc()
            self.logger.exception("Unable to handle message [%s]", msg)
        if tmp:
            self.send(sender, tmp)

        self.send_to_all(DoneMessage())  # Send a DoneMessage to all controllers, this flags you as 'Done'. Duh

    def go(self):
        self.logger.info('Starting')
        self.running = True
        while self.running:
            self.heartbeat()
            for sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                if event & zmq.POLLIN:
                    self.handle_in()

        # Also send a message to all your controllers, that you are stopping
        self.send_to_all(StopMessage())
        for k in self.controllers:
            self.socket.disconnect(k)

        self.logger.info('Stopping')

    def send_to_all(self, msg):
        for controller in self.controllers:
            self.send(controller, msg)

    def handle(self, msg):
        if msg.isa('kill'):
            self.running = False
            return
        elif msg.isa('info'):
            msg = self.prepare_wrm()
        elif msg.isa('loglevel'):
            args, kwargs = msg.get_args_kwargs()
            if args:
                loglevel = {'info': logging.INFO, 'debug': logging.DEBUG}.get(args[0], logging.INFO)
                self.logger.setLevel(loglevel)
                self.logger.info("Set loglevel to %s" % loglevel)
            return
        elif msg.isa('readfile'):
            args, kwargs = msg.get_args_kwargs()
            msg['data'] = open(args[0]).read()
        elif msg.isa('sleep'):
            args, kwargs = msg.get_args_kwargs()
            time.sleep(float(args[0]))
            snore = 'z' * random.randint(1, 20)
            self.logger.debug(snore)
            msg['result'] = snore
        else:
            msg = self.handle_work(msg)
            self.msg_count += 1
            gc.collect()
            self._check_mem(msg)

        return msg

    def _check_mem(self, msg):
        # RSS is in bytes, convert to Kilobytes
        rss_kb = psutil.Process().memory_full_info().rss / (2 ** 10)
        self.logger.debug("RSS is: %s KB", rss_kb)
        if self.restart_check and rss_kb > MAX_MEMORY_KB:
            args = msg.get_args_kwargs()[0]
            self.logger.critical('args are: %s', args)
            self.logger.critical(
                'Memory usage (KB) %s > %s, restarting', rss_kb, MAX_MEMORY_KB)
            self.running = False

    def handle_work(self, msg):
        raise NotImplementedError


class WorkerNode(WorkerBase):
    workertype = 'calc'

    def execute_code(self, msg):
        args, kwargs = msg.get_args_kwargs()

        func_fully_qualified_name = kwargs['function'].split('.')
        module_name = '.'.join(func_fully_qualified_name[:-1])
        func_name = func_fully_qualified_name[-1]
        func_args = kwargs.get("args", [])
        func_kwargs = kwargs.get("kwargs", {})

        self.logger.debug('Importing module: %s' % module_name)
        mod = importlib.import_module(module_name)
        function = getattr(mod, func_name)
        self.logger.debug('Executing function: %s' % func_name)
        self.logger.debug('args: %s kwargs: %s' % (func_args, func_kwargs))
        result = function(*func_args, **func_kwargs)

        msg['result'] = result
        return msg

    def handle_work(self, msg):
        if msg.isa('execute_code'):
            return self.execute_code(msg)

        args, kwargs = msg.get_args_kwargs()
        self.logger.info('doing calc %s' % args)
        filename = args[0]
        groupby_col_list = args[1]
        aggregation_list = args[2]
        where_terms_list = args[3]
        aggregate = kwargs.get('aggregate', True)

        # create rootdir
        full_file_name = os.path.join(self.data_dir, filename)
        pa_table = aggregate_pq(
            full_file_name,
            groupby_col_list,
            aggregation_list,
            data_filter=where_terms_list,
            aggregate=aggregate,
            as_df=False
        )

        # create message
        if len(pa_table) == 0:
            msg['data'] = None
        else:
            msg['data'] = serialize_pa_table(pa_table)

        return msg


class DownloaderNode(WorkerBase):
    workertype = 'download'

    def __init__(self, *args, **kwargs):
        super(DownloaderNode, self).__init__(*args, **kwargs)
        self.last_download_check = 0

    def heartbeat(self):
        super(DownloaderNode, self).heartbeat()
        if time.time() - self.last_download_check > DOWNLOAD_DELAY:
            self.check_downloads()

    def check_downloads(self):
        # Note, the files being downloaded are stored per key on filename,
        # Yet files are grouped as being inside a ticket for downloading at the same time
        # this done so that a group of files can be synchronized in downloading
        # when called from rpc.download(filenames=[...]

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + '*')

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(parqueryd.config.REDIS_TICKET_KEY_PREFIX):]

            ticket_details_items = [(k, v) for k, v in ticket_details.items()]
            random.shuffle(ticket_details_items)

            for node_filename_slot, progress_slot in ticket_details_items:
                tmp = node_filename_slot.split('_')
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                filename = '_'.join(tmp[1:])

                tmp = progress_slot.split('_')
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                timestamp, progress = float(tmp[0]), tmp[1]

                if node != self.node_name:
                    continue

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress == 'DONE':
                    continue

                try:
                    # acquire a lock for this node_filename
                    lock_key = parqueryd.config.REDIS_DOWNLOAD_LOCK_PREFIX + self.node_name + ticket + filename
                    lock = self.redis_server.lock(lock_key, timeout=parqueryd.config.REDIS_DOWNLOAD_LOCK_DURATION)
                    if lock.acquire(False):
                        self.download_file(ticket, filename)
                        break  # break out of the loop so we don't end up staying in a large loop for giant tickets
                except:
                    self.logger.exception('Problem downloading %s %s', ticket, filename)
                    # Clean up the whole ticket if an error occured
                    self.remove_ticket(ticket)
                    break
                finally:
                    try:
                        lock.release()
                    except redis.lock.LockError:
                        pass

    def file_downloader_progress(self, ticket, filename, progress):
        node_filename_slot = '%s_%s' % (self.node_name, filename)
        # Check to see if the progress slot exists at all, if it does not exist this ticket has been cancelled
        # by some kind of intervention, stop the download and clean up.
        tmp = self.redis_server.hget(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        if not tmp:
            # Clean up the whole ticket contents from disk
            ticket_path = os.path.join(parqueryd.config.INCOMING, ticket)
            self.logger.debug('Now removing entire ticket %s', ticket_path)
            for filename in glob.glob(ticket_path + '*'):
                rm_file_or_dir(os.path.join(parqueryd.config.INCOMING, filename))
            raise Exception("Ticket %s progress slot %s not found, aborting download" % (ticket, node_filename_slot))
        # A progress slot contains a timestamp_filesize
        progress_slot = '%s_%s' % (time.time(), progress)
        self.redis_server.hset(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

    def _get_transport_params(self):
        return {}

    def download_file(self, ticket, fileurl):
        if self.azure_conn_string:
            self._download_file_azure(ticket, fileurl)
        else:
            self._download_file_aws(ticket, fileurl)

    def _download_file_aws(self, ticket, fileurl):
        tmp = fileurl.replace('s3://', '').split('/')
        bucket = tmp[0]
        filename = '/'.join(tmp[1:])

        incoming_file = self._get_temp_name(ticket, filename)

        if os.path.exists(incoming_file):
            self.logger.info("%s exists, skipping download" % incoming_file)
            self.file_downloader_progress(ticket, fileurl, 'DONE')
        else:
            self.logger.info("Downloading ticket [%s], bucket [%s], filename [%s]" % (ticket, bucket, filename))

            access_key, secret_key, s3_conn = self._get_s3_conn()
            object_summary = s3_conn.Object(bucket, filename)
            size = object_summary.content_length

            key = 's3://{}:{}@{}/{}'.format(access_key, secret_key, bucket, filename)

            with open(incoming_file, 'wb') as fd:
                # See: https://github.com/RaRe-Technologies/smart_open/commit/a751b7575bfc5cc277ae176cecc46dbb109e47a4
                # Sometime we get timeout errors on the SSL connections
                for x in range(3):
                    try:
                        transport_params = self._get_transport_params()
                        with smart_open.open(key, 'rb', transport_params=transport_params) as fin:
                            buf = True
                            progress = 0
                            while buf:
                                buf = fin.read(pow(2, 20) * 16)  # Use a bigger buffer
                                fd.write(buf)
                                progress += len(buf)
                                self.file_downloader_progress(ticket, fileurl, size)
                        break
                    except SSLError as e:
                        if x == 2:
                            raise e
                        else:
                            pass

        self.logger.debug('Download done %s: %s', ticket, fileurl)
        self.file_downloader_progress(ticket, fileurl, 'DONE')

    def _get_prod_name(self, file_name):
        return os.path.join(parqueryd.config.DEFAULT_DATA_DIR, file_name)

    def _get_temp_name(self, ticket, file_name):
        return os.path.join(parqueryd.config.INCOMING, ticket + '_' + file_name)

    def _get_s3_conn(self):
        """Create a boto3 """
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            raise ValueError('Missing S3 credentials')
        credentials = credentials.get_frozen_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        s3_conn = boto3.resource('s3')
        return access_key, secret_key, s3_conn

    def _download_file_azure(self, ticket, fileurl):
        tmp = fileurl.replace('azure://', '').split('/')
        container_name = tmp[0]
        blob_name = tmp[1]
        incoming_file = self._get_temp_name(ticket, blob_name)

        if os.path.exists(incoming_file):
            self.logger.info("%s exists, skipping download" % incoming_file)
            self.file_downloader_progress(ticket, fileurl, 'DONE')
        else:
            self.logger.info(
                "Downloading ticket [%s], container name [%s], blob name [%s]" % (ticket, container_name, blob_name))

            # Download blob
            with open(incoming_file, 'wb') as fh:
                blob_client = BlobClient.from_connection_string(
                    conn_str=self.azure_conn_string, container_name=container_name, blob_name=blob_name
                )
                download_stream = blob_client.download_blob()
                fh.write(download_stream.readall())

            self.logger.debug('Download done %s azure://%s/%s', ticket, container_name, blob_name)
            self.file_downloader_progress(ticket, fileurl, 'DONE')

    def remove_ticket(self, ticket):
        # Remove all Redis entries for this node and ticket
        # it can't be done per file as we don't have the bucket name from which a file was downloaded
        self.logger.debug('Removing ticket %s from redis', ticket)
        for node_filename_slot in self.redis_server.hgetall(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket):
            if node_filename_slot.startswith(self.node_name):
                self.logger.debug('Removing ticket_%s %s', ticket, node_filename_slot)
                self.redis_server.hdel(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        tdm = TicketDoneMessage({'ticket': ticket})
        self.send_to_all(tdm)


class MoveparquetNode(DownloaderNode):
    workertype = 'moveparquet'

    def moveparquet(self, ticket):
        # A notification from the controller that all files are downloaded on all nodes,
        # the files in this ticket can be moved into place
        ticket_path = os.path.join(parqueryd.config.INCOMING, ticket + '_*')
        file_name_list = glob.glob(ticket_path)
        if file_name_list:
            for filename in file_name_list:
                filename_without_ticket = filename[filename.index('_') + 1:]
                prod_path = self._get_prod_name(filename_without_ticket)
                if os.path.exists(prod_path):
                    rm_file_or_dir(prod_path)
                incoming_path = self._get_temp_name(ticket, filename_without_ticket)

                # Add a metadata file to the downloaded item
                metadata_filepath = self._get_prod_name(filename_without_ticket + '.metadata')
                metadata = {'ticket': ticket,
                            'timestamp': time.time(),
                            'localtime': time.ctime(),
                            'utc': str(datetime.datetime.utcnow())
                            }
                open(metadata_filepath, 'w').write(json.dumps(metadata, indent=2))

                self.logger.debug("Moving %s %s" % (incoming_path, prod_path))
                shutil.move(incoming_path, prod_path)
        else:
            self.logger.debug('Doing a moveparquet for files %s which do not exist', ticket_path)

    def check_downloads(self):
        # Check all the entries for a specific ticket over all the nodes
        # only if ALL the nodes are _DONE downloading, move the parquet files in this ticket into place.

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + '*')

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(parqueryd.config.REDIS_TICKET_KEY_PREFIX):]

            in_progress_count = 0
            ticket_details_items = ticket_details.items()

            ticket_on_this_node = False

            for node_filename_slot, progress_slot in ticket_details_items:
                tmp = node_filename_slot.split('_')
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                filename = '_'.join(tmp[1:])

                tmp = progress_slot.split('_')
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                timestamp, progress = float(tmp[0]), tmp[1]

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress != 'DONE':
                    in_progress_count += 1

                if node == self.node_name:
                    ticket_on_this_node = True

            if in_progress_count == 0 and ticket_on_this_node:
                try:
                    self.moveparquet(ticket)
                except:
                    self.logger.exception('Error occurred in moveparquet %s', ticket)
                finally:
                    self.remove_ticket(ticket)
