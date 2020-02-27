#app manager manages all the ryu apps that we run, any program that we write is a ryu app
from ryu.base import app_manager
#All the Openflow events are in ofp_event such as ConnectionUp, PacketIn, LinkEvent, PacketOut
from ryu.controller import ofp_event
#config_dispatcher negotiates OF version and request switch features,
#main dispatcher receives switch features and sent switch configuration messages(port mod msg, flow mod msg)
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
#set_ev_cls is a function decorator, it turns any function into a event handler
from ryu.controller.handler import set_ev_cls
#from ofproto import OF 1.3 
from ryu.ofproto import ofproto_v1_3
#from packet library we import packet, ethernet and ether types
#packet helps us parse incoming packets into protocol packets
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types


class SimpleSwitch13(app_manager.RyuApp):
    #making an array of supported OF versions that is only 1.3
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]


    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        #creating a dictionary that maps a mac address to a port 
        self.mac_to_port = {}


    #Following function will handle switch features which will be dispatched by config dispatcher
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        #whatever OF protocol is negotiated between controller and switch
        #ofproto and ofproto_parser turns that into an object
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.


        #create a match that will match all incoming switch features packet
        match = parser.OFPMatch()
        #send the features to the controller AND do not buffer them 
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)


    #Adding the flow in the switch
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #OFPInstructionActions apply the actions passed to the function to the switch
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)

        #save the msg received in event in a variable
        msg = ev.msg
        #extract the datapath from that message 
        datapath = msg.datapath
        #get the protocol of that switch (whether it is 1.0 or 1.3)
        ofproto = datapath.ofproto
        #parse the procotol as a object to be used further if need be
        parser = datapath.ofproto_parser
        #set the incoming port of the packet
        in_port = msg.match['in_port']
        #Change the raw data bytes and parses it as a packet 
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        #we need to ignore lldp packets 
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        #get the switch id or datapath id
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in dpid: ", dpid, " MAC src: ", src, " MAC dst: ", dst," Packet in-port: " in_port)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            # verify if we have a valid buffer_id, if yes avoid to send both
            # flow_mod & packet_out
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)