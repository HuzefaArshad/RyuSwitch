from ryu.lib.packet import vlan
from ryu.base import app_manager
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.controller import ofp_event
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import ether_types
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER

class VLANSwitch(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self):
        super(VLANSwitch, self).__init__()
        self.datapaths = dict()
        self.mac_to_port = dict()

    def add_flow(self, datapath, match, priority, actions, buffer_id=None):
        of_protocol = datapath.ofproto
        of_protcol_parser = datapath.ofproto_parser

        instruction = [of_protcol_parser.OFPInstructionActions(of_protocol.OFPIT_APPLY_ACTIONS,actions)]

        if buffer_id:
            mod = of_protcol_parser.OFPFLowMod(datapath=datapath, match=match, priority=priority, instructions=instruction,buffer_id=buffer_id)
        else:
            mod = of_protcol_parser.OFPFlowMod(datapath=datapath, match=match, priority=priority, instructions=instruction)

        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, event):
        print("Received switch features!")

        datapath = event.msg.datapath
        of_proto = datapath.ofproto
        parser = datapath.ofproto_parser

        #Store the switch in a dictionary
        self.datapaths[datapath.id] = datapath

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(of_proto.OFPP_CONTROLLER)]

        self.add_flow(datapath,match,1,actions)
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, event):

        msg = event.msg
        datapath = msg.datapath
        of_protocol = datapath.ofproto
        of_protocol_parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        eth_src = eth.src
        eth_dst = eth.dst

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth_src] = in_port

        if eth_dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][eth_dst]
        else:
            out_port = of_protocol.OFPP_FLOOD

        actions = [of_protocol_parser.OFPActionOutput(out_port)]

        if out_port != of_protocol.OFPP_FLOOD:
            self.logger.info("Adding flow: %s (src_mac) --> %s (dst_mac)", eth_src, eth_dst)
            match = of_protocol_parser.OFPMatch(in_port=in_port, eth_dst=eth_dst, eth_src=eth_src)

            #means there is a buffer id
            if msg.buffer_id != of_protocol.OFP_NO_BUFFER:
                self.add_flow(datapath, match, 1, actions, msg.buffer_id)
            else:
                self.add_flow(datapath,match,1,actions)

        data = None
        if msg.buffer_id == of_protocol.OFP_NO_BUFFER:
            data = msg.data

        pkt_out = of_protocol_parser.OFPPacketOut(datapath=datapath,buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        
        datapath.send_msg(pkt_out)