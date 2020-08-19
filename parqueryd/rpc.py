import base64
import binascii
import json
import logging
import os
import pickle
import random
import time

import redis
import zmq
from parquery.aggregate import aggregate_pa
from parquery.transport import deserialize_pa_table
from pyarrow import ArrowInvalid

import parqueryd.config
from parqueryd.messages import msg_factory, RPCMessage, ErrorMessage
from parqueryd.tool import ens_bytes

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


class RPCError(Exception):
    """Base class for exceptions in this module."""
    pass


class RPC(object):

    def __init__(self, address=None, timeout=120, redis_url='redis://127.0.0.1:6379/0', loglevel=logging.INFO,
                 retries=3):
        self.logger = parqueryd.logger.getChild('rpc')
        self.logger.setLevel(loglevel)
        self.context = zmq.Context()
        self.redis_url = redis_url
        redis_server = redis.from_url(redis_url)
        self.retries = retries
        self.timeout = timeout
        self.identity = binascii.hexlify(os.urandom(8)).decode()

        if not address:
            # Bind to a random controller
            controllers = list(redis_server.smembers(parqueryd.config.REDIS_SET_KEY))
            if len(controllers) < 1:
                raise Exception('No Controllers found in Redis set: ' + parqueryd.config.REDIS_SET_KEY)
            random.shuffle(controllers)
        else:
            controllers = [address]
        self.controllers = controllers
        self.connect_socket()

    def connect_socket(self):
        reply = None
        for c in self.controllers:
            self.logger.debug('Establishing socket connection to %s', c)
            tmp_sock = self.context.socket(zmq.REQ)
            tmp_sock.setsockopt(zmq.RCVTIMEO, 2000)
            tmp_sock.setsockopt(zmq.LINGER, 0)
            tmp_sock.identity = ens_bytes(self.identity)
            tmp_sock.connect(c)
            # first ping the controller to see if it responds at all
            msg = RPCMessage({'payload': 'ping'})
            tmp_sock.send_json(msg)
            try:
                reply = msg_factory(tmp_sock.recv_json())
                self.address = c
                break
            except:
                logging.exception("Unable to connect to %s", c)
                continue
        if reply:
            # Now set the timeout to the actual requested
            self.logger.debug("Connection OK, setting network timeout to %s milliseconds", self.timeout * 1000)
            self.controller = tmp_sock
            self.controller.setsockopt(zmq.RCVTIMEO, self.timeout * 1000)
        else:
            raise Exception('No controller connection')

    def __getattr__(self, name):

        def _rpc(*args, **kwargs):
            self.logger.debug('Call %s on %s' % (name, self.address))
            start_time = time.time()
            params = {}
            if args:
                params['args'] = args
            if kwargs:
                params['kwargs'] = kwargs

            # We do not want string args to be converted into unicode by the JSON machinery
            msg = RPCMessage({'payload': name})
            msg['params'] = params
            rep = None
            for x in range(self.retries):
                try:
                    self.controller.send_json(msg)
                    rep = self.controller.recv()
                    break
                except Exception as e:
                    self.controller.close()
                    self.logger.critical(e)
                    if x == self.retries:
                        raise e
                    else:
                        self.logger.debug("Error, retrying %s" % (x + 1))
                        self.connect_socket()
                        pass
            if not rep:
                raise RPCError("No response from DQE, retries %s exceeded" % self.retries)
            try:
                if name == 'groupby':
                    _, groupby_col_list, agg_list, where_terms_list = args[0], args[1], args[2], args[3]
                    result = self.uncompress_groupby_to_pq(rep, groupby_col_list, agg_list, where_terms_list,
                                                           aggregate=kwargs.get('aggregate', False))
                else:
                    rep = msg_factory(json.loads(rep))
                    result = rep.get('result', {})
            except (ValueError, TypeError):
                self.logger.exception('Could not use RPC method: {}/{}'.format(name, rep))
                result = rep
            if isinstance(rep, ErrorMessage):
                raise RPCError(rep.get('payload'))
            stop_time = time.time()
            self.last_call_duration = stop_time - start_time
            return result

        return _rpc

    def uncompress_groupby_to_pq(self, result, groupby_col_list, agg_list, where_terms_list, aggregate=False):
        # uncompress result returned by the groupby and convert it to a Pandas DataFrame
        try:
            pa_table = deserialize_pa_table(result)
        except ArrowInvalid:
            # if it's not a pyarrow table, an error must have happened and we should have a string message
            try:
                if isinstance(result, dict) and result.get('result'):
                    result = result['result']
                raise ValueError(result)
            except:
                raise

        result_df = aggregate_pa(
            pa_table,
            groupby_col_list,
            agg_list,
            data_filter=None,  # we can assume the filtering already happened
            aggregate=aggregate)

        del pa_table
        return result_df

    def get_download_data(self):
        redis_server = redis.from_url(self.redis_url)
        tickets = set(redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + '*'))
        data = {}
        for ticket in tickets:
            tmp = redis_server.hgetall(ticket)
            data[ticket] = tmp
        return data

    def downloads(self):
        data = self.get_download_data()
        buf = []
        for k, v in data.items():
            done_count = 0
            for kk, vv in v.items():
                if vv.endswith('_DONE'):
                    done_count += 1
            buf.append((k, '%s/%s' % (done_count, len(v))))
        return buf

    def delete_download(self, ticket):
        redis_server = redis.from_url(self.redis_url)
        tmp = redis_server.hgetall(ticket)
        count = 0
        for k, v in tmp.items():
            count += redis_server.hdel(ticket, k)
        return count
