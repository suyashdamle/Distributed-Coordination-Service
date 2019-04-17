from Message import Message
from coordination_utils import Msg_type
import socket
from coordination_utils import *
import os
import sys

def add_node_protocol(self):
	'''
	Add the node to the network
	'''
	print("Add Node protocol started !")
	#send message "add_node" to standard IP's
	send_msg_add_node(self.config_table, self.PORT)

	with self.AN_condition:
		self.AN_condition.wait()	# blocking - Waiting for reply from sponsor node 
	
	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.sponsor_host = message._source_host
	self.sponsor_port = message.get_data('port')		# Port on which sponsor server is listening							
	self.ldr_ip = message.get_data('ldr_ip')
	self.ldr_port = message.get_data('ldr_port')
	self.network_dict = message.get_data('network_dict')

	print("Sponsor_host is : ",self.sponsor_host," Sponsor port is : ",self.sponsor_port)
	print("Message recieved: Leader ip :", self.ldr_ip, "Leader port :",self.ldr_port)
	
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((self.ldr_ip,self.ldr_port))
		send_msg(s, Message(Msg_type['AN_assign_id'], data_dict = {'port': self.PORT }))	# Contacting leader to give a new id

	with self.AN_condition:
		self.AN_condition.wait()	# blocking - Waiting for leader to assign ID

	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.node_id = message.get_data('id')

	print("Message recieved: New id assigned :", self.node_id)

	#Get file system from sponsored node
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((self.sponsor_host, self.sponsor_port))
		send_msg(s, Message(Msg_type['AN_FS_data_req'], data_dict = {'port': self.PORT }))

	s.close()		
	
	with self.AN_condition:
		self.AN_condition.wait()
	
	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.file_system_name = message.get_data('name')
	file_system_size = message.get_data('size')

	print("File system name is ",self.file_system_name," size is ",file_system_size)

	file_pointer = open(self.file_system_name,'wb')
	current_size = 0

	while True:
		# with self.AN_condition:
		# 	self.AN_condition.wait()
		if self.thread_msg_qs[self.main_thread_tid].empty() is False:
			message = self.thread_msg_qs[self.main_thread_tid].get()
			file_pointer.write(message.get_data('data'))
			current_size+= sys.getsizeof(message.get_data('data'))
			print("File size transferred ",current_size)
			if current_size >= file_system_size:
				break

	file_pointer.close()


	self.add_node = True
	print("Node added successfully !")

# Function to send leader ip and leader port to the new node that is being added
def send_AN_ldr_info(self, recv_host, recv_port):

	print("New node request: host:",recv_host," port:",recv_port)
	print("Sending leader info !")
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_ldr_info'], data_dict = {'port': self.PORT, 'ldr_ip': self.ldr_ip, 'ldr_port': self.ldr_port,\
																 'network_dict': self.network_dict }))

	s.close()

# Function to give unique id to the new node being added. Must be called by leader only
def assign_new_id(self, recv_host, recv_port):

	self.last_node_id += 1

	#send other nodes that a new node is added with the assigned node_id
	for key,value in self.network_dict:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			if value[2] is 1:
				s.connect((value[0], value[1]))
				send_msg(s,Message(Msg_type['AN_add_to_network']), data_dict = {'key': self.last_node_id,\
																				'value': (recv_host,recv_port,1)})
		s.close()

	self.network_dict[self.last_node_id] = (recv_host,recv_port,1)	#populate own table
	
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_set_id'], data_dict = {'id': self.last_node_id, 'port': self.PORT}))

	s.close()

	

def send_file_system(self, recv_host, recv_port):

	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		file_system = open('file_system',"rb")
		file_size = os.path.getsize('file_system')
		s.connect((recv_host, recv_port))
		print("Preparing to send file system...")
		send_msg(s, Message(Msg_type['AN_FS_data'], data_dict = {'name':'file_system_duplicate','size': file_size}))
		while True:
			chunk = file_system.read(self.buffer_size)
			if not chunk:
				break  # EOF
			send_msg(s, Message(Msg_type['AN_FS_data'], data_dict = {'data': chunk}))

	file_system.close()
	s.close()	

# Method to create 'add_node' message to be sent to all nodes in the standard_IP_table
# PORT is the server port on which it is listening. Initially not known to other nodes 
def send_msg_add_node(standard_IP_table, PORT):

	messages = []
	
	for key,value in standard_IP_table.items():
		print(type(key),type(value))
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







