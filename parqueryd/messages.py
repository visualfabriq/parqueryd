import base64
import json
import pickle
import time
from parqueryd.tool import ens_bytes


def msg_factory(msg):
    if isinstance(msg, bytes):
        try:
            msg = json.loads(msg.decode())
        except:
            msg is None
    if not msg:
        return Message()
    msg_mapping = {'calc': CalcMessage, 'rpc': RPCMessage, 'error': ErrorMessage,
                   'worker_register': WorkerRegisterMessage,
                   'busy': BusyMessage, 'done': DoneMessage,
                   'ticketdone': TicketDoneMessage,
                   'stop': StopMessage, None: Message}
    msg_class = msg_mapping.get(msg.get('msg_type'))
    return msg_class(msg)


class MalformedMessage(Exception):
    pass


class Message(dict):
    msg_type = None

    def __init__(self, datadict=None):
        if datadict is None:
            datadict = {}
        self.update(datadict)
        self['payload'] = datadict.get('payload')
        self['version'] = datadict.get('version', 1)
        self['msg_type'] = self.msg_type
        self['created'] = time.time()

    def copy(self):
        newme = super(Message, self).copy()
        return msg_factory(newme)

    def isa(self, payload_or_instance):
        if self.msg_type == getattr(payload_or_instance, 'msg_type', '_'):
            return True
        if self.get('payload') == payload_or_instance:
            return True
        return False

    def get_from_binary(self, key, default=None):
        buf = self.get(key)
        if not buf:
            return default
        return pickle.loads(base64.b64decode(buf))

    def to_json(self):
        # We could do some serializiation fixes in here for things like datetime or other binary non-json-serializabe members
        return ens_bytes(json.dumps(self))

    def set_args_kwargs(self, args, kwargs):
        params = {'args': args, 'kwargs': kwargs}
        self['params'] = params

    def get_args_kwargs(self):
        params = self.get('params', {})
        kwargs = params.get('kwargs', {})
        args = params.get('args', [])
        return args, kwargs


class WorkerRegisterMessage(Message):
    msg_type = 'worker_register'


class CalcMessage(Message):
    msg_type = 'calc'


class RPCMessage(Message):
    msg_type = 'rpc'


class ErrorMessage(Message):
    msg_type = 'error'


class BusyMessage(Message):
    msg_type = 'busy'


class DoneMessage(Message):
    msg_type = 'done'


class StopMessage(Message):
    msg_type = 'stop'


class TicketDoneMessage(Message):
    msg_type = 'ticketdone'
