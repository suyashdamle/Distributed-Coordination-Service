from Message import Message
from coordination_utils import Msg_type
import socket
from coordination_utils import *
import os
import sys
import random
import _thread
import shutil
import zipfile


def exception_handler(IP,PORT):
	print("Could not connect to IP ",IP,' Port ',PORT)

def add_node_protocol(self):
	'''
	Add the node to the network
	'''
	timeout = False
	print("Add Node protocol started !")
	print("Asking Leader Information from Standard IP's ")
	#send message "add_node" to standard IP's
	send_msg_add_node(self.config_table, self.PORT)

	with self.AN_condition:
		timeout = self.AN_condition.wait(timeout = self.add_node_timeout)	# blocking - Waiting for reply from sponsor node 
	
	if timeout is False:
		print("No reply from any node... Sending request again")
		send_msg_add_node(self.config_table, self.PORT)
		with self.AN_condition:
			timeout = self.AN_condition.wait(timeout = self.add_node_timeout)	# blocking - Waiting for reply from sponsor node 
			
		if timeout is False:
			print("Still no response. Exiting...")
			os._exit(0)



	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.sponsor_host = message._source_host
	self.sponsor_port = message.get_data('port')		# Port on which sponsor server is listening							
	self.ldr_ip = message.get_data('ldr_ip')
	self.ldr_port = message.get_data('ldr_port')
	self.network_dict = message.get_data('network_dict')
	self.ldr_id = message.get_data('id')
	self.network_dict[message._msg_id[0]]=(self.sponsor_host,self.sponsor_port,1)

	self.pause_heartbeat = False

	print("network_dict: ",self.network_dict)
	print("Sponsor_host is : ",self.sponsor_host," Sponsor port is : ",self.sponsor_port)
	print("Message recieved: Leader ip :", self.ldr_ip, "Leader port :",self.ldr_port)
	
	try:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((self.ldr_ip,self.ldr_port))
			send_msg(s, Message(Msg_type['AN_assign_id'], data_dict = {'port': self.PORT }))	# Contacting leader to give a new id
	except:
		exception_handler(self.ldr_ip,self.ldr_port)

	with self.AN_condition:
		timeout = self.AN_condition.wait(timeout = self.add_node_timeout)	# blocking - Waiting for leader to assign ID

	if timeout is False:
		print("Leader not responding. Exiting...")	
		os._exit(0)

	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.node_id = message.get_data('id')

	print("Message recieved: New id assigned :", self.node_id)

	#Get file system from sponsored node
	try:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((self.sponsor_host, self.sponsor_port))
			send_msg(s, Message(Msg_type['AN_FS_data_req'], data_dict = {'port': self.PORT }))
		s.close()
	except:
		print("Not able to connect to sponsor node not responding. Exiting...")
		os._exit(0)
		
	
	with self.AN_condition:
		timeout = self.AN_condition.wait(timeout = 5*self.add_node_timeout)
	
	if timeout is False:
		print("Sponsor node not responding. Exiting...")
		os._exit(0)

	message = self.thread_msg_qs[self.main_thread_tid].get()
	self.file_system_name = message.get_data('name')
	file_system_size = message.get_data('size')
	self.meta_data = message.get_data('meta_data')
	print("File system name is ",self.file_system_name," size is ",file_system_size)

	file_pointer = open(self.file_system_name+".zip",'wb')
	current_size = 0

	try:
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
	except:
		print("Connection broken. File System transfer failed. Exiting...")
		os._exit(0)

	file_pointer.close()

	if os.path.exists("./"+self.file_system_name):
		shutil.rmtree("./"+self.file_system_name)
	os.makedirs("./"+self.file_system_name)

	shutil.unpack_archive("./"+self.file_system_name + ".zip",'./'+self.file_system_name)
	# zipfilePath = ("./"+self.file_system_name + ".zip")
	# zip1 = zipfile.ZipFile(zipfilePath)
	# zip1.extractall(".")
	# zip1.close()
	# self.inputs.remove(self.file_system_port)
	# self.file_system_port.close()
	self.add_node = True
	print("Node added successfully !")
	print("DEUBG_MSG: metadata",self.meta_data)


# Function to send leader ip and leader port to the new node that is being added
def send_AN_ldr_info(self, recv_host, recv_port):

	print("New node request: host:",recv_host," port:",recv_port)
	print("Sending leader info !")
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((recv_host, recv_port))
		send_msg(s, Message(Msg_type['AN_ldr_info'],msg_id =(self.node_id,), data_dict = {'port': self.PORT, 'ldr_ip': self.ldr_ip, 'ldr_port': self.ldr_port,\
																 'network_dict': self.network_dict,'id':self.ldr_id}))

	s.close()

# Function to give unique id to the new node being added. Must be called by leader only
def assign_new_id(self, recv_host, recv_port):

	self.last_node_id += 1

	#send other nodes that a new node is added with the assigned node_id
	for key,value in self.network_dict.items():
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				if value[2] is 1:
					s.connect((value[0], value[1]))
					send_msg(s,Message(Msg_type['AN_add_to_network'], data_dict = {'key': self.last_node_id,\
																					'value': (recv_host,recv_port,1)}))
			s.close()
		except:
			exception_handler(value[0],value[1])

	self.network_dict[self.last_node_id] = (recv_host,recv_port,1)	#populate own table
	print("***************Updated Network Dict***************")
	print(self.network_dict)
	try:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((recv_host, recv_port))
			send_msg(s, Message(Msg_type['AN_set_id'], data_dict = {'id': self.last_node_id, 'port': self.PORT}))

		s.close()
	except:
		exception_handler(value[0],value[1])
	

def send_file_system(self, recv_host, recv_port):

	#TODO: handle for multiple nodes trying to get the file system
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

		s.connect((recv_host, recv_port))
		print("Preparing to send file system...")

		if self.sponsor_node_count == 0:
			shutil.make_archive(self.file_system_name,"zip","./root")

		self.sponsor_node_count += 1	
		file_system = open(self.file_system_name+".zip","rb")
		file_size = os.path.getsize(self.file_system_name + ".zip")

		send_msg(s, Message(Msg_type['AN_FS_data'], data_dict = {'name': self.file_system_name ,'size': file_size,\
																'meta_data': self.meta_data}))

		while True:
			chunk = file_system.read(self.buffer_size)
			if not chunk:
				break  # EOF
			send_msg(s, Message(Msg_type['AN_FS_data'], data_dict = {'data': chunk}))

	print("File system sent successfully!")
	self.sponsor_node_count -= 1
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
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				s.connect((message._recv_host,message._recv_port))	
				send_msg(s, message)
		except:
			exception_handler(message._recv_host,message._recv_port)

		s.close()







