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
from distributed_topo_base import *
from ryu.topology.api import get_switch, get_link
import topo_manager2
import thread
import networkx as nx

LOG = logging.getLogger(__name__)

class Shortest_Forwarding(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {
        'topo_manager': topo_manager2.TopoManager2,
    }
    def __init__(self, *args, **kwargs):
        super(Shortest_Forwarding, self).__init__(*args, **kwargs)
        self.topo_manager = kwargs['topo_manager']
        self.topology_api_app = self
        self.cid = self.topo_manager.get_cid()
        self.flow_queue = self.topo_manager.get_flow_queue()

    def start(self):
        super(Shortest_Forwarding, self).start()
        # self.show_topo_thread = hub.spawn(self._show_topo)
        thread.start_new_thread(self._receive_flow, ())

    def _receive_flow(self):
        while True:
            dflow = self.flow_queue.get()
            # flood local
            if dflow.operation_type == FLOOD:
                self.flood_local(dflow.data)
                continue
            # packet out
            if dflow.operation_type == PACKET_OUT:
                datapath = get_switch(self.topology_api_app,dflow.dpid).dp
                buffer_id = None
                src_port = None
                dst_port = dflow.dst_port_no
                data = dflow.data
                self.send_packet_out_local(datapath, buffer_id, src_port, dst_port, data)
                continue
            # install flow
            if dflow.operation_type == FLOW_MOD:
                self.send_flow_mod_local(self, datapath, dflow)
                continue

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        #handle arp request as arp proxy, try to send the packet directly to dst
        #handle ipv4 packet
        #1.find src_sw and dst_sw
        #   1.1 if can't find, return
        #   1.2 if src_sw == dst_sw, is's a local switch,just add flow and send packet out
        #2.if not,find shortest path between src_sw and dst_sw
        #   2.1 get all links from topo_manager and create a graph from links
        #   2.2 cuculate shortest path from graph, get path like:[0,1,2,3,4]
        #   2.3 transform the path to detail path info:[{"dpid":dpid,"src_port_no":src_port_no,"dst_port_no":dst_port_no},
        #                               {...},... ]
        #   2.4 according detail path info add/mod flows
        #   2.5 send packet out
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        src_ip = None
        dst_ip = None

        if len(pkt.get_protocols(ethernet.ethernet)):
            eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
        if isinstance(arp_pkt, arp.arp):
            self.logger.debug("ARP processing")
            src_ip = arp_pkt.src_ip
            dst_ip = arp_pkt.dst_ip
            self.arp_forwarding(self, msg, src_ip, dst_ip)
            return
        if isinstance(ip_pkt, ipv4.ipv4):
            self.logger.debug("IPV4 processing")
            if len(pkt.get_protocols(ethernet.ethernet)):
                eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
                self.shortest_forwarding(msg, eth_type, ip_pkt.src, ip_pkt.dst)
        else:
            return
        pass
    #install flows according path
    def install_flow(self, datapaths, link_to_port, access_table, path,
                     flow_info, buffer_id, data=None):
        pass

    #send a packet out to a port
    def send_packet_out_local(self, datapath, buffer_id, src_port, dst_port, data):
        out = self._build_packet_out(datapath, buffer_id,
                                     src_port, dst_port, data)
        if out:
            datapath.send_msg(out)

    #send flow to local switch
    def send_flow_mod_local(self, datapath, flow_info):
        parser = datapath.ofproto_parser
        eth_type = flow_info.get("eth_type")
        src_port = flow_info.get("src_port_no")
        dst_port = flow_info.get("dst_port_no")
        src_ipv4 = flow_info.get("src_ipv4")
        dst_ipv4 = flow_info.get("dst_ipv4")
        actions = []
        actions.append(parser.OFPActionOutput(dst_port))
        match = parser.OFPMatch(
            in_port=src_port, eth_type=eth_type,
            ipv4_src=src_ipv4, ipv4_dst=dst_ipv4)

        self.add_flow(datapath, 1, match, actions,
                      idle_timeout=15, hard_timeout=60)
    #send flow to other controller to add it
    def send_flow_mod_peer(self, flow_info):
        #TODO send it to peer controller
        self.topo_manager.add_flowinfo(flow_info)
        pass

    #handle arp request
    def arp_forwarding(self, msg, src_ip, dst_ip):
        datapath = msg.datapath
        src_dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        #1. get dst location by ip
        #   1.1 dst location not found,flood
        dport = self.topo_manager.get_host_location(dst_ip)
        if not dport:
            #tell other controllers to flood
            flow_info = DFlow(self.cid,None, None, None, None, None, None, FLOOD, msg.data)
            self.topo_manager.add_flowinfo(flow_info)
            #flood in local domain
            self.flood_local(msg.data)
            return
        #   1.2 dst location found, packet-out
        if dport.dpid == src_dpid:
            out = self._build_packet_out(datapath, ofproto.OFP_NO_BUFFER,
                                         ofproto.OFPP_CONTROLLER,
                                         dport.port_no, msg.data)
            datapath.send_msg(out)
        else:
            #TODO send to peer
            flow_info = DFlow(self.cid,dport.dpid,None,None,dport.port_no,None,None,PACKET_OUT,msg.data)
            self.topo_manager.add_flowinfo(flow_info)

    def flood_local(self, data):
        #switch_list = get_switch(self.topology_api_app, None)
        dhost_list = self.topo_manager.get_all_host()
        for dhost in dhost_list:
            port_info = dhost.port_id.split(":")
            dhost_dpid = int(port_info[0])
            dhost_port_no = int(port_info[1])
            switch = get_switch(self.topology_api_app, dhost_dpid)
            if switch:
                datapath = switch.dp
                ofproto = datapath.ofproto
                parser = datapath.ofproto_parser
                out = self._build_packet_out(datapath, ofproto.OFP_NO_BUFFER,ofproto.OFPP_CONTROLLER, dhost_port_no,data)
        self.logger.debug("Flooding msg")

    def shortest_forwarding(self, msg, eth_type, ip_src, ip_dst):
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        # 1.find dst_sw
        dst_dport = self.topo_manager.get_host_location(ip_dst)
        #   1.1 if can't find, return
        if not dst_dport:
            LOG.debug("can not find dst host location")
            return
        # 1.2 if src_sw == dst_sw, is's a local switch,just add flow and send packet out
        if datapath.id == dst_dport.dpid:
            dflow = DFlow(None, datapath.id, eth_type,in_port,dst_dport.port_no,ip_src, ip_dst,operation_type=FLOW_MOD,data=None)
            self.send_flow_mod_local(datapath, dflow)
        # 2.if not,find shortest path between src_sw and dst_sw
        #   2.1 get all links from topo_manager and create a graph from links
        graph = nx.DiGraph()
        dswitch_map = self.topo_manager.get_all_switch_map()
        dlink_list = self.topo_manager.get_all_link()
        dlink_list_new = {}
        for dlink in dlink_list:
            src_port_info = dlink.src_port_id.split(":")
            dst_port_info = dlink.dst_port_id.split(":")
            src_dpid = int(src_port_info[0])
            src_port_no = int(src_port_info[1])
            dst_dpid = int(dst_port_info[0])
            dst_port_no = int(dst_port_info[1])
            dlink_new = {"src_dpid":src_dpid,"src_port_no":src_port_no,"dst_dpid":dst_dpid,"dst_port_no":dst_port_no}
            dlink_list_new[(src_dpid,dst_dpid)] = dlink_new
            graph.add_edge(src_dpid,dst_dpid)
        # 2.2 cuculate shortest path from graph, get path like:[0,1,2,3,4]
        src_sw_dpid = datapath.id
        dst_sw_dpid = dst_dport.dpid
        path = nx.dijkstra_path(graph, src_sw_dpid, dst_sw_dpid)
        #   2.3 transform the path to detail path info:[{"dpid":dpid,"src_port_no":src_port_no,"dst_port_no":dst_port_no},
        #                               {...},... ]
        path_info = []
        for i in xrange(0, len(path)):
            flow_info = {}
            if i == 0:
                aft_link = dlink_list_new[(path[i], path[i + 1])]
                flow_info = {"dpid": path[i], "src_port_no": in_port,
                             "dst_port_no": aft_link["src_port_no"]}
            elif i == len(path) - 1:
                pre_link = dlink_list_new[(path[i - 1], path[i])]
                aft_link = dlink_list_new[(path[i], path[i + 1])]
                flow_info = {"dpid": path[i], "src_port_no": pre_link["dst_port_no"],
                             "dst_port_no": aft_link["src_port_no"]}
            else:
                pre_link = dlink_list_new[(path[i - 1], path[i])]
                aft_link = dlink_list_new[(path[i], path[i + 1])]
                flow_info = {"dpid": path[i], "src_port_no": pre_link["dst_port_no"],
                             "dst_port_no": aft_link["src_port_no"]}
            path_info.append(flow_info)
        #   2.4 according detail path info add/mod flows
        for flow_info in path_info:
            dpid = flow_info["dpid"]
            if dswitch_map[dpid] == self.cid:
                dp = get_switch(self.topology_api_app,dpid).dp
                dflow_mod = DFlow(self.cid, dpid, eth_type,flow_info["src_port_no"],flow_info["dst_port_no"],ip_src, ip_dst,operation_type=FLOW_MOD,data=msg.data)
                self.send_flow_mod_local(dp, dflow_mod)
            else:
                dflow_mod = DFlow(self.cid, dpid, eth_type, flow_info["src_port_no"], flow_info["dst_port_no"], ip_src,
                                  ip_dst, operation_type=FLOW_MOD, data=msg.data)
                self.send_flow_mod_peer(dflow_mod)
        #   2.5 send packet out
        self.send_packet_out_local(datapath, msg.buffer_id, path_info[0]["src_port_no"], path_info[0]["dst_port_no"],
                                   msg.data)

    def _build_packet_out(self, datapath, buffer_id, src_port, dst_port, data):
        actions = []
        if dst_port:
            actions.append(datapath.ofproto_parser.OFPActionOutput(dst_port))

        msg_data = None
        if buffer_id == datapath.ofproto.OFP_NO_BUFFER:
            if data is None:
                return None
            msg_data = data

        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id,
            data=msg_data, in_port=src_port, actions=actions)
        return out