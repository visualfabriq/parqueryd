import binascii
import os
import random
import time

import netifaces
import zmq


def get_my_ip():
    eth_interfaces = sorted(
        [ifname for ifname in netifaces.interfaces() if (ifname.startswith('eth') or ifname.startswith('en'))])
    if len(eth_interfaces) < 1:
        ifname = 'lo'
    else:
        ifname = eth_interfaces[-1]
    for x in netifaces.ifaddresses(ifname)[netifaces.AF_INET]:
        # Return first addr found
        return x['addr']


def bind_to_random_port(socket, addr, min_port=49152, max_port=65536, max_tries=100):
    "We can't just use the zmq.Socket.bind_to_random_port, as we wan't to set the identity before binding"
    for i in range(max_tries):
        try:
            port = random.randrange(min_port, max_port)
            socket.identity = bytes(('%s:%s' % (addr, port)).encode("utf-8"))
            socket.bind(bytes(('tcp://*:%s' % port).encode("utf-8")))
            # socket.bind('%s:%s' % (addr, port))
        except zmq.ZMQError as exception:
            en = exception.errno
            if en == zmq.EADDRINUSE:
                continue
            else:
                raise
        else:
            return socket.identity
    raise zmq.ZMQBindError("Could not bind socket to random port.")


def tree_checksum(path):
    allfilenames = set()
    for root, dirs, filenames in os.walk(path):
        for filename in filenames:
            allfilenames.add(os.path.join(root, filename))
    buf = ''.join(sorted(allfilenames))
    return hex(binascii.crc32(buf) & 0xffffffff)


###################################################################################################
# Various Utility methods for user-friendly info display

def show_workers(info_data, only_busy=False):
    'For the given info_data dict, show a humand-friendly overview of the current workers'
    nodes = {}
    for w in info_data.get('workers', {}).values():
        nodes.setdefault(w['node'], []).append(w)
    for k, n in nodes.items():
        print(k)
        for nn in n:
            if only_busy and not nn.get('busy'):
                continue
            print('   ', time.ctime(nn['last_seen']), nn.get('busy'))
