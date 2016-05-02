import logging

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ipv4
from ryu.lib.packet import arp
from ryu.topology import event, dumper

LOG = logging.getLogger(__name__)

class Shortest_Forwarding(app_manager.RyuApp):
    _CONTEXTS = {
        'DiscoveryEventDumper': dumper.DiscoveryEventDumper,
    }
    def __init__(self, *args, **kwargs):
        super(Shortest_Forwarding, self).__init__(*args, **kwargs)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        print "ha----------------------------------------------------"
        msg = ev.msg
        datapath = msg.datapath
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        src_ip = None
        dst_ip = None
        eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
        if len(pkt.get_protocols(ethernet.ethernet)):
            eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
        if isinstance(arp_pkt, arp.arp):
            self.logger.debug("ARP processing")
            src_ip = arp_pkt.src_ip
            dst_ip = arp_pkt.dst_ip
        elif isinstance(ip_pkt, ipv4.ipv4):
            self.logger.debug("IPV4 processing")
            self.shortest_forwarding(msg, eth_type, ip_pkt.src, ip_pkt.dst)
            src_ip = ip_pkt.src_ip
            dst_ip = ip_pkt.dst_ip
        else:
            self.logger.debug("no processing")
            return
        action = parser.OFPActionOutput(1)
        print type(msg)
        print msg
        print type(msg.data)
        print msg.data
        print action
        print type(action)
        print in_port
        print type(in_port)
        print eth_type
        print type(eth_type)
        print src_ip
        print type(src_ip)
        print dst_ip
        print type(dst_ip)
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