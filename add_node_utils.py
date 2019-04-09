from Message import Message
from coordination_utils import Msg_type
import socket
from coordination_utils import *

def add_node_protocol(self):
		'''
		Add the node to the network
		'''
		print("Add Node protocol started !")
		#send message "add_node" to standard IP's
		send_msg_add_node(self.config_table, self.PORT)

		sponsor_host = None
		sponsor_port = None

		while True:			# blocking - Waiting for reply from sponsor node 

			if not self.thread_msg_qs[self.main_thread_tid].empty():
				message = self.thread_msg_qs[self.main_thread_tid].get()
				if message._m_type is Msg_type['AN_ldr_info']:	#Sponsor node replies with leader info
					sponsor_host = message._source_host
					sponsor_port = message.get_data('port')		# Port on which sponsor server is listening							
					self.ldr_ip = message.get_data('ldr_ip')
					self.ldr_port = message.get_data('ldr_port')
					break
		print("Sponsor_host is : ",sponsor_host," Sponsor port is : ",sponsor_port)
		print("Message recieved: Leader ip :", self.ldr_ip, "Leader port :",self.ldr_port)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((self.ldr_ip,self.ldr_port))
			send_msg(s, Message(Msg_type['AN_assign_id'], data_dict = {'port': self.PORT }))	# Contacting leader to give a new id

		s.close()

		while True:

			if not self.thread_msg_qs[self.main_thread_tid].empty():
				message = self.thread_msg_qs[self.main_thread_tid].get()

				if message._m_type is Msg_type['AN_set_id']:		# Leader giving a unique id
					self.node_id = message.get_data('id')
					break
		print("Message recieved: New id assigned :", self.node_id)

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((sponsor_host, sponsor_port))
			send_msg(s, Message(Msg_type['AN_FS_data_req'], data_dict = {'port': self.PORT }))

		s.close()
		file_system = None

		while True:

			if not self.thread_msg_qs[self.main_thread_tid].empty():
				message = self.thread_msg_qs[self.main_thread_tid].get()

				if message._m_type is Msg_type['AN_FS_data']:		#file_system sent by sponsor node
					file_system_name = message.get_data('name')
					file_system = message.get_data('data')
					break
		print("File system data received")
		with open(file_system_name, "w") as f:
			f.write(file_system)

		f.close()

		# inform leader that the node is ready to be included in the network
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((self.ldr_ip,self.ldr_port))
			send_msg(s, Message(Msg_type['AN_ready'], data_dict = {'port': self.PORT, 'id': self.node_id }))

		s.close()

		while True:

			if not self.thread_msg_qs[self.main_thread_tid].empty():
				message = self.thread_msg_qs[self.main_thread_tid].get()

				if message._m_type is Msg_type['AN_success']:		# leader confirming added to network successful
					self.network_dict = message.get_data('network_dict')
					break
		print("Node added successfully !")
# Function to send leader ip and leader port to the new node that is being added
def send_AN_ldr_info(self, recv_host, recv_port):

	print("New node request: host:",recv_host," port:",recv_port)
	print("Sending leader info !")
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_ldr_info'], data_dict = {'port': self.PORT, 'ldr_ip': self.ldr_ip, 'ldr_port': self.ldr_port }))

	s.close()

# Function to give unique id to the new node being added. Must be called by leader only
def assign_new_id(self, recv_host, recv_port):

	self.last_node_id += 1
	#TODO: send other nodes that a new node is added with the assigned node_id
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_set_id'], data_dict = {'id': self.last_node_id}))

	s.close()

def send_file_system(self, recv_host, recv_port):

	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_FS_data'], data_dict = {'name': 'file_system','data': "Hello World"}))

	s.close()	

# Method to create 'add_node' message to be sent to all nodes in the standard_IP_table
# PORT is the server port on which it is listening. Initially not known to other nodes 
def send_msg_add_node(standard_IP_table, PORT):

	messages = []
	
	for key,value in standard_IP_table.items():
		source_host = None
		source_port = None
		recv_host = key
		recv_port = value
		data = {}
		data["port"] = PORT
		message = Message(Msg_type['add_node'],source_host = source_host, source_port = source_port,\
				recv_host = recv_host, recv_port = recv_port, data_dict = data)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((message._recv_host,message._recv_port))	
			send_msg(s, message)
		s.close()

def AN_to_network(self,message):

	#inform other nodes that a new node is added
	# for key,value in self.network_dict:

	# 	if value[2] is True:		#node alive
	# 		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	# 			s.connect(value[0], value[1])
	# 			send_msg(s, Message(Msg_type['AN_new_node'], data_dict = {'id': key, 'value': value,\
	# 															'last_node_id': self.last_node_id}))

	# 		s.close()

	#update node table. true stands for alive
	self.network_dict[message.get_data('id')] = (message._source_host,message.get_data('port'),True) 

	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((message._source_host, message.get_data('port')))
		send_msg(s, Message(Msg_type['AN_success'], data_dict = {'id': self.network_dict,\
																 'last_node_id': self.last_node_id}))

	s.close()





