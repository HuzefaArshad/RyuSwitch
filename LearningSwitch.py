from ryu.lib.packet import vlan
from ryu.base import app_manager
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.controller import ofp_event
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import ether_types
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER

port_to_vlan = {
    1: {
        1: [10],
        2: [10],
        3: [20],
        4: [20]
    }
}

vlan_members = {
    1: {
        10: [1,2],
        20: [3,4]
    }
}

vlan_to_group = {
    10: 10,
    20: 20
}

class VLANSwitch(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self):
        super(VLANSwitch, self).__init__()
        self.datapaths = dict()
        self.mac_to_port = dict()

    def make_group_tables(self, datapath):
        of_proto = datapath.ofproto
        parser = datapath.ofproto_parser

        for eachKey in vlan_members[datapath.id]:
            buckets = []
            for eachPort in vlan_members[datapath.id][eachKey]:
                actions = [parser.OFPActionOutput(eachPort)]
                buckets.append( parser.OFPBucket(actions=actions) )
            
            self.logger.info("GT for dpid: %s", datapath.id)
            req = parser.OFPGroupMod( datapath, of_proto.OFPGC_ADD, of_proto.OFPGT_ALL, eachKey, buckets )
            datapath.send_msg(req)


    def add_flow(self, datapath, match, priority, actions, buffer_id=None):
        of_protocol = datapath.ofproto
        of_protcol_parser = datapath.ofproto_parser

        instruction = [of_protcol_parser.OFPInstructionActions(of_protocol.OFPIT_APPLY_ACTIONS,actions)]

        if buffer_id:
            mod = of_protcol_parser.OFPFLowMod(datapath=datapath, match=match, priority=priority, instructions=instruction,buffer_id=buffer_id)
        else:
            mod = of_protcol_parser.OFPFlowMod(datapath=datapath, match=match, priority=priority, instructions=instruction)

        datapath.send_msg(mod)

    def flood(self, datapath, dpid, in_port):
        of_proto = datapath.ofproto
        parser = datapath.ofproto_parser


        vid_src = port_to_vlan[dpid][in_port][0]
        grp_id = vlan_to_group[vid_src]

        actions = [parser.OFPActionGroup(group_id=grp_id)]
        match = parser.OFPMatch(in_port=in_port)

        self.add_flow(datapath,match,1,actions)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, event):
        print("Received switch features!")
        self.make_group_tables(event.msg.datapath)

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

        to_flood = 0
        actions = []
        ports_to_flood = []
        src_vid = port_to_vlan[dpid][in_port][0]

        if eth_dst in self.mac_to_port[dpid]:
            out_ports = self.mac_to_port[dpid][eth_dst]
        else:
            to_flood = 1
            self.flood(datapath,dpid,in_port)

        if to_flood != 1:

            src_vid = port_to_vlan[dpid][in_port][0]
            dst_vid = port_to_vlan[dpid][out_ports][0]

            if src_vid == dst_vid:
                self.logger.info("Adding flow: %s (src_mac) --> %s (dst_mac)", eth_src, eth_dst)
                match = of_protocol_parser.OFPMatch(in_port=in_port, eth_src=eth_src, eth_dst=eth_dst)

                #means there is a buffer id
                if msg.buffer_id != of_protocol.OFP_NO_BUFFER:
                    self.add_flow(datapath, match, 1, actions, msg.buffer_id)
                else:
                    self.add_flow(datapath,match,1,actions)
            else:
                self.logger.info("VLAN's are not same")
                match = of_protocol_parser.OFPMatch(in_port=in_port, eth_src=eth_src, eth_dst=eth_dst)
                actions = []
                self.add_flow(datapath,match,1,actions)

        grp_id = vlan_to_group[src_vid]
        actions = [of_protocol_parser.OFPActionGroup(group_id=grp_id)]

        data = None
        if msg.buffer_id == of_protocol.OFP_NO_BUFFER:
            data = msg.data
        
        pkt_out = of_protocol_parser.OFPPacketOut(datapath=datapath,buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(pkt_out)