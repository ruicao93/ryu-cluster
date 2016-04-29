
import logging
import six
import struct
import time
import json

LOG = logging.getLogger(__name__)


class DPort(object):
    def __init__(self, dpid, port_no):
        super(DPort, self).__init__()

        self.id = str(dpid) + ":" + str(port_no)
        self.dpid = dpid
        self.port = port_no



class DSwitch(object):
    def __init__(self, dpid, cid):
        super(DSwitch, self).__init__()

        self.dpid = dpid
        self.cid = cid


def dict2dswitch(d):
    return DSwitch(d['dpid'], d['cid'])

def dswitch2dict(dswitch):
    return json.dumps(dswitch, default=lambda obj: obj.__dict__)