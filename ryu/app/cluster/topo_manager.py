import logging
import struct

from ryu.base import app_manager
from ryu.controller import mac_to_port
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
import random
from distributed_topo_base import DPort,DSwitch
import json
from ryu.lib import hub
from ryu.topology import event, dumper
import hazelcast_client

LOG = logging.getLogger(__name__)
MAX_CID = 0xffffffff




class TopoManager(app_manager.RyuApp):

    _CONTEXTS = {
        'DiscoveryEventDumper': dumper.DiscoveryEventDumper,
    }

    def __init__(self, *args, **kwargs):
        super(TopoManager, self).__init__(*args, **kwargs)

    @set_ev_cls(event.EventSwitchEnter)
    def switch_enter_test(self, ev):
        # print ev.dp
        print "switch leave"
        pass