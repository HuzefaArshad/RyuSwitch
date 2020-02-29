#library to match vlan headers
from ryu.lib.packet import vlan
#app manager that registers our application to Ryu
from ryu.base import app_manager
#library to parse packet to use later
from ryu.lib.packet import packet
#library to match ethernet headers
from ryu.lib.packet import ethernet
#library to get openflow events (FlowMod, PacketIn)
from ryu.controller import ofp_event
#library to use OpenFlow 1.3
from ryu.ofproto import ofproto_v1_3
#library to match different ether types 
from ryu.lib.packet import ether_types
#handler library that handles the events and forwards the packets to our application functions 
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER


#retrieve a port vlan, a: b: [c], where "a"=dpid, "b"= port no, "c"=vlan id
port_to_vlan = {
    1: {
        1: [10],
        2: [10],
        3: [20],
        4: [20]
    }
}

#retrieve all the ports in a specific vlan, a: b: [c], where "a"=dpid, "b"=vlan id, "c"= list of ports
vlan_members = {
    1: {
        10: [1,2],
        20: [3,4]
    }
}

#retrieve group table for a vlan, a: b, where "a"=vlan id, "b"=group table id
vlan_to_group = {
    10: 10,
    20: 20
}

class VLANSwitch(app_manager.RyuApp):

    #define the openflow versions we are going to use
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]


    def __init__(self):
        #register our application name to ryu manager
        super(VLANSwitch, self).__init__()
        #save the all the switches in a dictionary
        self.datapaths = dict()
        #dictionary that stores mac addresses in the format, a: b: c, where "a"=dpid, "b": mac address, "c"=port no
        self.mac_to_port = dict()


    
    def make_group_tables(self, datapath):
        """
        Since we are working with vlan's, a broadcast of one vlan should not reach the other vlan.
        To address this issue we will make group tables, where all the ports belonging  to specific vlan are 
        grouped together. For this purpose buckets are used, an action is made outputting to a single port and 
        added to that bucket, another action is made outputting to a different port of same vlan is added to 
        the bucket. That bucket is then added to a group table and a id is given to that group table. 
        Therefore whenever a packet needs to be flooded its action is set to OFPActionGroup and given the 
        group table id to prevent the individual flooding of ports. 
        """
        of_proto = datapath.ofproto
        parser = datapath.ofproto_parser

        #create a separate bucket for each vlan
        for eachKey in vlan_members[datapath.id]:
            buckets = []
            #add ports in the same vlan to be flooded in the bucket
            for eachPort in vlan_members[datapath.id][eachKey]:
                actions = [parser.OFPActionOutput(eachPort)]
                buckets.append( parser.OFPBucket(actions=actions) )
            
            self.logger.info("GT for dpid: %s", datapath.id)
            #make a group table using ofpgc_add and tell the group table to flood to all ports not just one 
            #using ofpgt_all
            req = parser.OFPGroupMod( datapath, of_proto.OFPGC_ADD, of_proto.OFPGT_ALL, eachKey, buckets )
            datapath.send_msg(req)


    #add flows to the switch
    def add_flow(self, datapath, match, priority, actions, buffer_id=None):
        of_protocol = datapath.ofproto
        of_protcol_parser = datapath.ofproto_parser

        #create an instruction to send to the switch
        instruction = [of_protcol_parser.OFPInstructionActions(of_protocol.OFPIT_APPLY_ACTIONS,actions)]

        if buffer_id:
            mod = of_protcol_parser.OFPFLowMod(datapath=datapath, match=match, priority=priority, instructions=instruction,buffer_id=buffer_id)
        else:
            mod = of_protcol_parser.OFPFlowMod(datapath=datapath, match=match, priority=priority, instructions=instruction)

        datapath.send_msg(mod)

    #function to flood the packets to the same vlan
    def flood(self, datapath, dpid, in_port):
        of_proto = datapath.ofproto
        parser = datapath.ofproto_parser

        #get the source port vlan id
        vid_src = port_to_vlan[dpid][in_port][0]
        #use the vlan id to get the group table id
        grp_id = vlan_to_group[vid_src]

        #instead of sending individual actions set the action to group table
        actions = [parser.OFPActionGroup(group_id=grp_id)]
        #create a match using the input port
        match = parser.OFPMatch(in_port=in_port)

        #add flow to the switch
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
        #send the features received of the switch to the controller
        actions = [parser.OFPActionOutput(of_proto.OFPP_CONTROLLER)]

        self.add_flow(datapath,match,1,actions)
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, event):

        msg = event.msg
        datapath = msg.datapath
        of_protocol = datapath.ofproto
        of_protocol_parser = datapath.ofproto_parser
        #get the input port of the packet
        in_port = msg.match['in_port']

        #parse the raw data (or bits) into a packet
        pkt = packet.Packet(msg.data)
        #extract only the ethernet headers from the packet (leave ip,tcp headers)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        #get the source and destionation mac addresses
        eth_src = eth.src
        eth_dst = eth.dst

        #get the switch it and store against the [dpid][mac_src] its port
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth_src] = in_port

        #variable will be set if we need to flood
        to_flood = 0
        #initially actions and the ports to flood will be empty
        actions = []
        ports_to_flood = []
        #get the source port vlan id by providing dpid and input port
        src_vid = port_to_vlan[dpid][in_port][0]


        #if we know the output port for the destionation then we dont need to flood 
        if eth_dst in self.mac_to_port[dpid]:
            out_ports = self.mac_to_port[dpid][eth_dst]
        else:
            #if we dont know the output port we need to flood
            to_flood = 1
            self.flood(datapath,dpid,in_port)

        #if we dont need to flood then do the following 
        if to_flood != 1:
            
            #get the source and destination port vlan id
            src_vid = port_to_vlan[dpid][in_port][0]
            dst_vid = port_to_vlan[dpid][out_ports][0]

            #if both are in same vlan than proceed foward
            if src_vid == dst_vid:
                self.logger.info("Adding flow: %s (src_mac) --> %s (dst_mac)", eth_src, eth_dst)
                match = of_protocol_parser.OFPMatch(in_port=in_port, eth_src=eth_src, eth_dst=eth_dst)

                #means there is a buffer id
                if msg.buffer_id != of_protocol.OFP_NO_BUFFER:
                    self.add_flow(datapath, match, 1, actions, msg.buffer_id)
                else:
                    self.add_flow(datapath,match,1,actions)
            else:
                #if both are not in the same vlan create a flow with empty action list. empty action list cause the packet to be dropped
                self.logger.info("VLAN's are not same")
                match = of_protocol_parser.OFPMatch(in_port=in_port, eth_src=eth_src, eth_dst=eth_dst)
                actions = []
                self.add_flow(datapath,match,1,actions)

        #THE FOLLOWING WORK WILL BE PERFORMED FOR THE VERY FIRST PACKET WE RECIEVE. WE CREATE A RULE FOR IT
        #AND THEN SEND IT OUT USING THE PACKET OUT FUNCTION

        #get the group table it
        grp_id = vlan_to_group[src_vid]
        #set the action to group table to flood to same members of vlan
        actions = [of_protocol_parser.OFPActionGroup(group_id=grp_id)]

        #if switch cannot buffer the data than send the data along with the packet
        data = None
        if msg.buffer_id == of_protocol.OFP_NO_BUFFER:
            data = msg.data
        
        #create a packet out to send the packet to be send and send it to the switch
        pkt_out = of_protocol_parser.OFPPacketOut(datapath=datapath,buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(pkt_out)