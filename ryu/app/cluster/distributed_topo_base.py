
import logging
import six
import struct
import time
import json

LOG = logging.getLogger(__name__)


class DPort(object):
    def __init__(self, dpid, port_no,id = None):
        super(DPort, self).__init__()
        if not id:
            self.id = id
        else:
            self.id = str(dpid) + ":" + str(port_no)
        self.dpid = dpid
        self.port_no = port_no



class DSwitch(object):
    def __init__(self, dpid, cid):
        super(DSwitch, self).__init__()

        self.dpid = dpid
        self.cid = cid

class DHost(object):
    def __init__(self, port_id, mac, ipv4=[], ipv6=[]):
        super(DHost, self).__init__()

        self.port_id = port_id
        self.mac = mac
        self.ipv4 = ipv4
        self.ipv6 = ipv6

class DLink(object):
    def __init__(self, src_port_id, dst_port_id):
        super(DLink, self).__init__()

        self.src_port_id = src_port_id
        self.dst_port_id = dst_port_id



def dict2dswitch(d):
    return DSwitch(d['dpid'], d['dpid'], d['id'])

def dict2dport(d):
    return DPort(d['dpid'], d['port_no'])

def dict2dhost(d):
    return DHost(d['port_id'], d['mac'], d['ipv4'], d['ipv6'])

def dict2dlink(d):
    return DLink(d['src_port_id'], d['dst_port_id'])

def dswitch2dict(dswitch):
    return json.dumps(dswitch, default=lambda obj: obj.__dict__)

def dtopobase2dict(data):
    return json.dumps(data, default=lambda obj: obj.__dict__)