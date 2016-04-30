import logging

from ryu.base import app_manager
from ryu.topology import event, dumper
import random
from ryu.controller.handler import set_ev_cls
import ryu.base.app_manager
from ryu.lib import hub
import time
import Queue
import thread
from ryu import cfg
from distributed_topo_base import *
from ryu.controller import handler
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import CONFIG_DISPATCHER


LOG = logging.getLogger(__name__)
MAX_CID = 0xffffffff
#import hazelcast_client


DSWITCH_MAP = "distibuted-dswitch-map"
DPORT_MAP = "distributed-dport-map"
DLINK_MAP = "distributed-dlink-map"
DHOST_MAP = "distributed-dhost-map"


class TopoManager2(app_manager.RyuApp):

    _CONTEXTS = {
        'DiscoveryEventDumper': dumper.DiscoveryEventDumper,
    }

    def __init__(self, *args, **kwargs):
        super(TopoManager2, self).__init__(*args, **kwargs)
        #hub.patch()
        self.cid = random.randint(0, MAX_CID)
        self.hazelcast_manager = ryu.base.app_manager.hazelcastManager
        self.queue = Queue.Queue()
        print "------------------------------------------------------------------"
        print self.hazelcast_manager


    def start(self):
        super(TopoManager2, self).start()
        #self.show_topo_thread = hub.spawn(self._show_topo)
        thread.start_new_thread(self._show_topo, ())

    def _show_topo(self):
        dswitch_map = self.hazelcast_manager.get_map(DSWITCH_MAP)
        dport_map = self.hazelcast_manager.get_map(DPORT_MAP)
        dhost_map = self.hazelcast_manager.get_map(DHOST_MAP)
        dlink_map = self.hazelcast_manager.get_map(DLINK_MAP)
        while True:
            print "---------------------show topo:---------------------"
            print "switches in topo:"
            for key in dswitch_map:
                print "key:%d  --- value: %s" % (key, dswitch_map.get(key))
            print "ports in topo:"
            for key in dport_map:
                print "key:%s  --- value: %s" % (key, dport_map.get(key))
            print "hosts in topo:"
            for key in dhost_map:
                print "key:%s  --- value: %s" % (key, dhost_map.get(key))
            print "linkss in topo:"
            for key in dlink_map:
                print "key:%s  --- value: %s" % (key, dlink_map.get(key))

            hub.sleep(5)

    @set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self, ev):
        # print ev.dp
        LOG.info("switch enter~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        switch  = ev.switch
        LOG.info(type(switch))
        dpid = switch.dp.id
        dswitch = DSwitch(dpid,self.cid)
        data = dswitch2dict(dswitch)
        self.hazelcast_manager.update_map_value(DSWITCH_MAP, dpid, data)
        for port in switch.ports:
            self.add_port( port.dpid,port.port_no)

    @set_ev_cls(event.EventSwitchLeave)
    def switch_leave_handler(self, ev):
        LOG.info("switch leave~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # print ev.dp
        switch = ev.switch
        LOG.info(type(switch))
        dpid = switch.dp.id
        LOG.info(type(dpid))
        self.hazelcast_manager.remove_map_value(DSWITCH_MAP, dpid)

    _EVENTS = [event.EventSwitchEnter, event.EventSwitchLeave,
               event.EventSwitchReconnected,
               event.EventPortAdd, event.EventPortDelete,
               event.EventPortModify,
               event.EventLinkAdd, event.EventLinkDelete,
               event.EventHostAdd]

    @set_ev_cls(event.EventPortAdd)
    def port_add_handler(self, ev):
        port = ev.port
        port_no = port.port_no
        dpid = port.dpid
        port_id = str(dpid) + ":" +str(port_no)
        dport = DPort(dpid,port_no,port_id)
        data = dtopobase2dict(dport)
        self.hazelcast_manager.update_map_value(DPORT_MAP, port_id, data)
        pass

    @set_ev_cls(event.EventPortDelete)
    def port_leave_handler(self, ev):
        port = ev.port
        port_no = port.port_no
        dpid = port.dpid
        port_id = str(dpid) + ":" + str(port_no)
        self.hazelcast_manager.remove_map_value(DPORT_MAP, port_id)
        pass

    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        link = ev.link
        src_port = link.src
        dst_port = link.dst
        src_port_id = str(src_port.dpid) + ":" + str(src_port.port_no)
        dst_port_id = str(dst_port.dpid) + ":" + str(dst_port.port_no)
        dlink = DLink(src_port_id,dst_port_id)
        data = dtopobase2dict(dlink)
        self.hazelcast_manager.update_map_value(DLINK_MAP, src_port_id, data)

    @set_ev_cls(event.EventLinkDelete)
    def link_leave_handler(self, ev):
        link = ev.link
        src_port = link.src
        dst_port = link.dst
        src_port_id = str(src_port.dpid) + ":" + str(src_port.port_no)
        self.hazelcast_manager.remove_map_value(DLINK_MAP, src_port_id)
        pass

    @set_ev_cls(event.EventHostAdd)
    def host_add_handler(self, ev):
        host = ev.host
        port_id = str(host.port.dpid) + ":" + str(host.port.port_no)
        dhost = DHost(port_id, host.mac, host.ipv4, host.ipv6)
        data = dtopobase2dict(dhost)
        print "host-add:+++++++++++++++++++++++++++++++++++++++++++++++++++++data:"
        self.hazelcast_manager.update_map_value(DHOST_MAP, port_id, data)
        pass

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        msg = ev.msg
        LOG.info("switch:%s connected++++++++++++++++++++++++++++++++++++++++++++++++", datapath.id)

        # install table-miss flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_port(self, dpid, port_no):
        port_id = str(dpid) + ":" + str(port_no)
        dport = DPort(dpid, port_no, port_id)
        data = dtopobase2dict(dport)
        self.hazelcast_manager.update_map_value(DPORT_MAP, port_id, data)

    def update_map_value(self, map_name, key, data):
        self.hazelcast_manager.update_map_value(map_name, key, data)

    def remove_map_value(self, map_name, key):
        self.hazelcast_manager.remove_map_value(map_name, key)

    def add_flow(self, dp, p, match, actions, idle_timeout=0, hard_timeout=0):
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        mod = parser.OFPFlowMod(datapath=dp, priority=p,
                                idle_timeout=idle_timeout,
                                hard_timeout=hard_timeout,
                                match=match, instructions=inst)
        dp.send_msg(mod)