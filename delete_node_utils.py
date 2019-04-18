from Message import Message
import socket
from coordination_utils import *
import os
import sys
import threading
import time

def init_delete(self):
    if self.is_sponsor:
        print("Cannot delete Node. Currently a sponsor node")
    else:
        if self.is_leader:
            #Get next highest key and broadcast new_ldr_id.
            key_list = list(self.network_dict.keys())
            key_list.sort()
            new_ldr_id = key_list[0]
            for n in self.network_dict:
                new_ldr_msg = Message(Msg_type['new_ldr_id'],msg_id = (self.node_id,threading.current_thread().ident))
                new_ldr_msg._source_host,new_ldr_msg._source_port = self.HOST,self.PORT
                new_ldr_msg._recv_host,new_ldr_msg._recv_port = self.network_dict[n][0],self.network_dict[n][1]
                new_ldr_msg._data_dict = {'type':'del_ldr','id':new_ldr_id,'ip': self.network_dict[new_ldr_id][0] ,'port':self.network_dict[new_ldr_id][1]}
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
                    soc.connect((new_ldr_msg._recv_host,new_ldr_msg._recv_port))
                    send_msg(soc,new_ldr_msg) #send message
            print("DEBUG_MSG: Sent new leader id: ", new_ldr_id)
            self.is_leader = False
            # print("---------")
            # print(new_ldr_msg.get_data('id'))
            # print(new_ldr_msg.get_data('ip'))
            # print(new_ldr_msg.get_data('port'))
            self.ldr_port = self.network_dict[new_ldr_id][1]
            self.ldr_ip = self.network_dict[new_ldr_id][0]
            time.sleep(1)
            

        #send delete_msg to leader and stop
        delete_msg = Message(Msg_type['delete_node'],msg_id = (self.node_id,threading.current_thread().ident))
        delete_msg._source_host,delete_msg._source_port=self.HOST,self.PORT
        delete_msg._recv_host,delete_msg._recv_port = self.ldr_ip,self.ldr_port
        delete_msg._data_dict = {'id':self.node_id}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
            soc.connect((self.ldr_ip,self.ldr_port))
            send_msg(soc,delete_msg) #send message

        #stop
        time.sleep(1)

        os._exit(0)
		

def del_from_network_dict(self,msg):
    #delete node from net directory					
    del self.network_dict[msg.get_data('id')]
    print("Network Directory:",self.network_dict)
    #broadcast if leader
    if self.is_leader:
        for n in self.network_dict:
            msg._recv_host,msg._recv_port = (self.network_dict[n][0],self.network_dict[n][1])
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
                soc.connect((self.network_dict[n][0],self.network_dict[n][1]))
                send_msg(soc,msg) #send message
            #send_msg(msg)