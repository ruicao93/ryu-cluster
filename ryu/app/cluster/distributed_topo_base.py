
import logging
import six
import struct
import time
import json

FACTORY_ID=1

LOG = logging.getLogger(__name__)

FLOW_MOD = "flow-mod"
PACKET_OUT = "packet-out"
FLOOD = "flood"


class DFlow(object):
    CLASS_ID = 1
    def __init__(self,cid, dpid, eth_type,src_port_no,dst_port_no,src_ipv4, dst_ipv4,operation_type=FLOW_MOD,data=None):
        super(DFlow, self).__init__()
        self.cid = cid
        self.dpid = dpid
        self.eth_type = eth_type
        self.src_port_no = src_port_no
        self.dst_port_no = dst_port_no
        self.src_ipv4 = src_ipv4
        self.dst_ipv4 = dst_ipv4
        self.operation_type = operation_type
        self.data = data

    def is_flow_mod(self):
        return self.operation_type == FLOW_MOD

    def is_packet_out(self):
        return self.operation_type == PACKET_OUT
    def is_flood(self):
        return self.operation_type == FLOOD

    def to_dict(self):
        return {"cid":self.cid,
                "dpid":self.dpid,
                "eth_type":self.eth_type,
                "src_port_no":self.src_port_no,
                "dst_port_no":self.dst_port_no,
                "src_ipv4":self.src_ipv4,
                "dst_ipv4":self.dst_ipv4,
                "operation_type":self.operation_type,
                "data":self.data
        }

    def write_portable(self, writer):
        writer.write_int("cid", self.cid)
        writer.write_int("dpid", self.dpid)
        writer.write_int("eth_type", self.eth_type)
        writer.write_int("src_port_no", self.src_port_no)
        writer.write_int("dst_port_no", self.dst_port_no)
        writer.write_utf("src_ipv4", self.src_ipv4)
        writer.write_utf("dst_ipv4", self.dst_ipv4)
        writer.write_utf("operation_type", self.operation_type)
        writer.write_utf("data", self.data)

    def read_portable(self, reader):
        self.cid = reader.read_int("cid")
        self.dpid = reader.read_int("dpid")
        self.eth_type = reader.read_int("eth_type")
        self.src_port_no = reader.read_int("src_port_no")
        self.dst_port_no = reader.read_int("dst_port_no")
        self.src_ipv4 = reader.read_utf("src_ipv4")
        self.dst_ipv4 = reader.read_utf("dst_ipv4")
        self.operation_type = reader.read_utf("operation_type")
        self.data = reader.read_utf("data")

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID
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
    def __init__(self, port_id, mac, ipv4=None, ipv6=None):
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




def dict2dswitch(str):
    d = json.loads(str)
    return DSwitch(d['dpid'], d['dpid'], d['id'])

def dict2dport(d):
    d = json.loads(str)
    return DPort(d['dpid'], d['port_no'])

def dict2dhost(str):
    d = json.loads(str)
    return DHost(d['port_id'], d['mac'], d['ipv4'], d['ipv6'])

def dict2dlink(str):
    d = json.loads(str)
    return DLink(d['src_port_id'], d['dst_port_id'])
def dict2dflow(str):
    d = json.loads(str)
    return DFlow(d['cid'], d['dpid'], d['eth_type'], d['src_port_no'], d['dst_port_no'], d['src_ipv4'], d['dst_ipv4'], d['operation_type'],d['data'])

def dswitch2dict(dswitch):
    return json.dumps(dswitch, default=lambda obj: obj.__dict__)

def dtopobase2dict(data):
    if isinstance(data, DFlow):
        return data
    return json.dumps(data, default=lambda obj: obj.__dict__)
'''
dflow = DFlow(1,1,1,1,1,"123",None,"flow-mod",None)
dflow_json =  json.dumps(dflow, default=lambda obj: obj.__dict__)
print dflow_json
df = dict2dflow(dflow_json)
print df.cid'''