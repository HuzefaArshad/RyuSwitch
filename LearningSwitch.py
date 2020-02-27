from ryu.base import app_manager
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.controller import ofp_event
from ryu.ofproto import ofproto_v1_3, ofproto_v1_3_parser, ofproto_parser
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet

#port_to_vlan is saved in a format: {a: {b: c}}, where "a" = dpid, "b"=port, and "c"=vlanid
port_to_vlan = {
    1: {
        1: 1,
        2: 2
    },
    2: {
        1: 1,
        2: 2
    }
}

#port_to_vlan is saved in a format: {a: {b: c}}, where "a" = dpid, "b"=vlanid, and "c"=list of ports
ports_in_vlan = {
    1: {
        1: [1],
        2: [2]
    },
    2: {
        1: [1],
        2: [2]
    }
}

trunk_ports = {
    1: [3],
    2: [3]
}

class LearningSwitch(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self):
        super(LearningSwitch, self).__init__()
        self.mac_to_port = {}
        self.datapaths = []

    def get_vlan_members(self, dpid,vid):
        ports_to_flood = list()
        ports_to_flood = ports_in_vlan[dpid][vid]

        return ports_to_flood


    def flood(self, datapath, dpid, in_port):
        of_lib = datapath.ofproto
        of_parser = datapath.ofproto_parser

        vid = port_to_vlan[dpid][in_port]

        ports_to_flood = self.get_vlan_members(dpid, vid)

        for eachPort in ports_to_flood:
            match = of_parser.OFPMatch()
            actions = [of_parser.OFPActionOutput(eachPort)]
            self.add_flow(datapath,0,match,actions)
        
        for eachTrunkPort in trunk_ports[dpid]:
            match = of_parser.OFPMatch()
            actions = [of_parser.OFPActionPushVlan(33024), of_parser.OFPActionSetField(vlan_vid=vid),
                       of_parser.OFPActionOutput(eachPort)]
            self.add_flow(datapath,0,match,actions)


    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        of_lib = datapath.ofproto
        of_parsed = datapath.ofproto_parser

        instruction = [of_parsed.OFPInstructionActions(of_lib.OFPIT_APPLY_ACTIONS,actions)]

        if buffer_id:
            modify = of_parsed.OFPFlowMod(datapath=datapath, priority=priority,
                                        match=match, instructions=instruction, buffer_id=buffer_id)
        else:
            modify = of_parsed.OFPFlowMod(datapath=datapath, priority=priority, match=match,
                                        instructions=instruction)

        datapath.send_msg(modify)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _handle_Connection_Up(self, event):

        msg = event.msg
        datapath = msg.datapath
        dpid = datapath.id
        of_lib = datapath.ofproto
        of_parser = datapath.ofproto_parser

        self.datapaths.append(datapath)

        switch_capabilities = [
            {"OFPC_PORT_BLOCKED": 0},
            {"OFPC_QUEUE_STATS": 0},
            {"OFPC_IP_REASM": 0},
            {"OFPC_GROUP_STATS": 0},
            {"OFPC_PORT_STATS": 0},
            {"OFPC_TABLE_STATS": 0},
            {"OFPC_FLOW_STATS": 0}
        ]

        print("Dpid: " + str(dpid) + " connected!")
        print("Switch capabilities: ")
        for i in range(len(switch_capabilities)):
            for onlyKey in switch_capabilities[i]:
                print( str(onlyKey) + ": " + str( bin(msg.capabilities)[i] ) )
                switch_capabilities[i][onlyKey] = bin(msg.capabilities)[i]

        match = of_parser.OFPMatch()
        actions = [of_parser.OFPActionOutput(of_lib.OFPP_CONTROLLER,
                                             of_lib.OFPCML_NO_BUFFER)]

        self.add_flow(datapath,0,match,actions)
        

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _handle_Packet_In(self, event):
        print("Packet received")
        msg = event.msg
        datapath = msg.datapath
        ofproto_lib = datapath.ofproto
        of_parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ethernet.ether.ETH_TYPE_LLDP:
            return
        
        dst_mac = eth.dst
        src_mac = eth.src

        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src_mac] = in_port

        if dst_mac not in self.mac_to_port[dpid]:
            print("flooding..")
            self.flood(datapath,dpid, in_port)
            print("flooding done!")
        elif dst_mac in self.mac_to_port[dpid]:
            print("Dst Mac exist is mac table")
            out_port = self.mac_to_port[dpid][dst_mac]
            src_vid = port_to_vlan[dpid][in_port]
            dst_vid = port_to_vlan[dpid][out_port]

            if src_vid == dst_vid:
                match = of_parser.OFPMatch()
                actions = [of_parser.OFPActionOutput(out_port)]
                self.add_flow(datapath,0,match,actions)
            else:
                self.logger.info("VLAN's are not same")

