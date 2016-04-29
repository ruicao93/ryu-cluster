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
        test_map = self.hazelcast_manager.get_map(DSWITCH_MAP)
        while True:
            print "---------------------show topo:---------------------"
            for key in test_map:
                print "key:%d  --- value: %s" % (key, test_map.get(key))
            hub.sleep(5)

    @set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self, ev):
        # print ev.dp
        LOG.info("switch enter~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        switch  = ev.switch
        LOG.info(type(switch))
        dpid = switch.dp.id
        LOG.info(type(dpid))
        dswitch = DSwitch(dpid,self.cid)
        data = dswitch2dict(dswitch)
        #thread.start_new_thread(self.update_map_value, (DSWITCH_MAP, dpid, data))
        hub.spawn(self.update_map_value, DSWITCH_MAP, dpid, data)
        #self.hazelcast_manager.update_map_value(DSWITCH_MAP, dpid, data)

    @set_ev_cls(event.EventSwitchLeave)
    def switch_leave_handler(self, ev):
        LOG.info("switch leave~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # print ev.dp
        switch = ev.switch
        LOG.info(type(switch))
        dpid = switch.dp.id
        LOG.info(type(dpid))
        self.hazelcast_manager.remove_map_value(DSWITCH_MAP, dpid)

    def update_map_value(self, map_name, key, data):
        self.hazelcast_manager.update_map_value(map_name, key, data)