#https://stackoverflow.com/questions/17667903/python-socket-receive-large-amount-of-data
import select
import socket
import sys
import queue
import threading
from _thread import *
import pickle
import enum
import time

# importing custom utility functions
from coordination_utils import *
from add_node_utils import *
from Message import Message


# TODO: use UDP for heartbeats 

class Node(object):
	"""
	The Node object runs on each node/server and does all tasks relating to 
	crash detection, serving clients on this server, etc.
	"""
	

	def __init__(self, config_fname=None,host='127.0.0.1',port=64532, is_leader = False):

		# instance members
		# TODO : read config file a#delete node fromnd populate lists and metadata
		# TODO : create an dictionary object containing standard IP's as key and port number as value
		self.HOST = host  				# Standard loopback interface address (localhost)
		self.PORT = port       			# Port to listen on
		self.thread_msg_qs = {}			# Each thread gets a msg queue, key = its thread-id;val= a Queue obj
		self.network_dict={}			# A dict of type 	[node_id : <host_ip,host_port>]
		self.is_leader = is_leader		# whether this node is the leader or not
		self.heartbeat_delay = 5		# in seconds
		self.timeout_thresh = 3			# number of timeouts, after which a node is declared dead
		self.node_id = -1				# the node-id of this instance - changed during joining protocol
		self.config_fname = config_fname
		self.config_table = {'127.0.0.1': 64532} 		# TODO mentioned above
		# Thread-ids of some critical processes kept as instance variables
		self.coordinator_tid = None
		self.last_node_id = 0
		# present leader details
		self.ldr_id = 1
		self.ldr_ip = '127.0.0.1'
		self.ldr_port = 64532
		self.ldr_heartbeat_delay=5		# max how much delay could be expected from the leader bet heartbeats
		self.is_sponsor = False
		if self.is_leader:
			self.ldr_ip = host
			self.ldr_port = port

				

		self.ldr_heartbeat_delay=100	# max how much delay could be expected from the leader bet heartbeats

		# Creating the heartbeat handling thread
		heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
		heartbeat_thread.start()
		heartbeat_tid = heartbeat_thread.ident

		# TODO : decide parameters of each thread function
		coordination_thread =  threading.Thread(target = self.coordination_thread_fn, args=(heartbeat_tid,))
		coordination_thread.start()
		self.coordinator_tid = coordination_thread.ident
		print(type(self.is_leader))
		print(self.is_leader)
		if not self.is_leader:
			self.main_thread_tid = threading.current_thread().ident 	# find the tid of main_thread
			self.thread_msg_qs[self.main_thread_tid] = queue.Queue()	# this queue will have messages related to add_node
			self.add_node_protocol()									# get node added to the network			

		else:
			print("Server up at ip :",self.HOST," port: ",self.PORT)

		
		
	from add_node_utils import add_node_protocol,send_AN_ldr_info,assign_new_id,send_file_system,\
								AN_to_network
	

	def thread_manager(self):
		"""
		ensures that finished threads are removed from lists of threads
		"""
		pass

	def ldrelect_thread_fn(self):
		"""
		Tasked with the election of the 
		"""
		has_leader = False
		nodes = self.network_dict.keys().append(self.node_id)
		while not has_leader:
			nodes = sorted(nodes)
			for n_id in nodes:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					try:
						s.connect((self.network_dict[n_id][0],self.network_dict[n_id][1]))
					except:
						pass
					else:
						heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
						heartbeat_msg._recv_host,heartbeat_msg._recv_port = self.network_dict[n_id]
						heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
						send_msg(s, heartbeat_msg)
		# now, the coordinator is responsible to pass the heartbeat messages 


	def heartbeat_thread_fn(self):
		'''
		Does all processes related to heartbeat receiving and sending
		'''
		
		self.thread_msg_qs[threading.current_thread().ident] = queue.Queue()
		heartbeat_msg = Message(Msg_type['heartbeat'])

		# for a leader node
		if self.is_leader:
			# initiate time-out counts

			# dict of type   [node_id : count of time-outs]
			node_timeouts = {n_id:-1 for n_id in self.network_dict.keys()}

			while True:
				responded_nodes = []
				# Collect all messages from queue:
				q = self.thread_msg_qs[threading.current_thread().ident]

				while not q.empty():
					hmsg = q.get()
					print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
					responded_nodes.append(hmsg._msg_id[0])
				
				# correct time-out counts			
				for n_id in self.network_dict.keys():
					if n_id not in responded_nodes:
						node_timeouts[n_id] += 1
					else:
						node_timeouts[n_id] = 0


				# Check if someone has not responded for long:
				for n_id in self.network_dict.keys():
					if node_timeouts[n_id] >= self.timeout_thresh:
						print("NODE : ",n_id," found unresponsive")
						# TODO: what now? - initiate node deletion phase

				# Send a heartbeat to everyone and start a timer
				for n_id in self.network_dict.keys():
					# send messages to all using temporary port
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						try:
							s.connect((self.network_dict[n_id][0],self.network_dict[n_id][1]))
						except:
							pass
						else:
							heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
							heartbeat_msg._recv_host,heartbeat_msg._recv_port = self.network_dict[n_id]
							heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
							send_msg(s, heartbeat_msg)

				# re-starting timer
				time.sleep(self.heartbeat_delay)
		
		# for a non-leader node

		
		else:
			ldr_timeout_count = -1
			while True:
				q = self.thread_msg_qs[threading.current_thread().ident]
				got_ldr_hbeat = False

				while not q.empty():
					hmsg = q.get()
					print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
					ldr_timeout_count =  0

					# reply to heartbeat
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						hbeat_id = hmsg._msg_id
						hmsg_ip,hmsg_port = self.network_dict[hbeat_id]
						if hbeat_id == self.ldr_id:
							got_ldr_hbeat = True
						try:
							s.connect((hmsg_ip, hmsg_port))
						except:
							pass
						else:
							heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
							heartbeat_msg._recv_host,heartbeat_msg._recv_port = (hmsg_ip, hmsg_port)
							heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
							send_msg(s, heartbeat_msg)
					
				if not got_ldr_hbeat:
					ldr_timeout_count += 1

				# check if leader has failed
				if ldr_timeout_count >= self.timeout_thresh:
					print("Leader failure detected")
					leader_elect_thread = threading.Thread(target=self.ldr_elect_thread,args=())

				# re-rstarting timer
				time.sleep(self.ldr_heartbeat_delay)
	
	

	def coordination_thread_fn(self, heartbeat_tid):

		self.thread_msg_qs[threading.get_ident()] = queue.Queue()
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # prevents "already in use" errors
		server.setblocking(0)
		server.bind((self.HOST, self.PORT))
		server.listen(5)
		inputs = [server]
		outputs = []
		message_queues = {} # message queue dict

		while inputs:
			readable, writable, exceptional = select.select(inputs, outputs, inputs)
			for s in readable:
				if s is server:
					# for new connections
					connection, client_address = s.accept()
					connection.setblocking(0)
					inputs.append(connection)
					print("DEBUG_MSG: Received connection request from: ",client_address)
					# creating a message queue for each connection
					message_queues[connection] = queue.Queue() 
				else:
					# if some message has been received - be it in part
					msg = recv_msg(connection)	#server
					if msg:
						print("DEBUG_MSG: data received: ")
						msg._source_host = client_address[0]
						msg._source_port = client_address[1]
						# find message type and send to the right thread
						if Msg_type(msg._m_type) is Msg_type.heartbeat:
							self.thread_msg_qs[heartbeat_tid].put(msg)
						elif Msg_type(msg._m_type) is Msg_type.AN_ldr_info:
							self.thread_msg_qs[self.main_thread_tid].put(msg)

						elif Msg_type(msg._m_type) is Msg_type.AN_set_id:
							self.thread_msg_qs[self.main_thread_tid].put(msg)

						elif Msg_type(msg._m_type) is Msg_type.AN_FS_data:
							self.thread_msg_qs[self.main_thread_tid].put(msg)

						elif Msg_type(msg._m_type) is Msg_type.add_node:	#sponsor node on receiving 'add_node'
							add_node_thread = threading.Thread(target = self.send_AN_ldr_info, args=(msg._source_host,msg.get_data('port'), ))
							add_node_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.AN_assign_id:
							AN_assign_id_thread = threading.Thread(target = self.assign_new_id, args=(msg._source_host,msg.get_data('port'), ))
							AN_assign_id_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.AN_FS_data_req:
							send_file_system_thread = threading.Thread(target = self.send_file_system, args=(msg._source_host,msg.get_data('port'),))
							send_file_system_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.AN_success:
							self.thread_msg_qs[self.main_thread_tid].put(msg)

						elif Msg_type(msg._m_type) is Msg_type.AN_ready:
							self.AN_to_network(msg)
						
						elif Msg_type(msg._m_type) is Msg_type.delete_node:
							#delete node from net directory
							del self.network_dict[msg.dict_data]
							#broadcast if leader
							if self.is_leader:
								for n in self.network_dict:
									msg._recv_host,msg._recv_port = (self.network_dict[n](0),self.network_dict[n](1)  #if n is leader??
									with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
										soc.connect((self.network_dict[n](0),self.network_dict[n](1)))
										send_msg(soc,msg) #send message
									#send_msg(msg)
							
						elif Msg_type(msg._m_type) is Msg_type.init_delete:
							if self.is_sponsor:
								print("Cannot delete Node")
							else:
								if self.is_leader:
									#Get next highest key and broadcast new_ldr_id.
									key_list = list(self.network_dict.keys())
									key_list.sort()
									new_ldr_id = key_list[1]
									for n in self.network_dict and :
										new_ldr_msg = Message(Msg_type['new_ldr_id'])
										new_ldr_msg._source_host,new_ldr_msg._source_port = self.HOST,self.PORT
										new_ldr_msg.recv_host,new_ldr_msg._recv_port = self.network_dict[n](0),self.network_dict[n](1)
										delete_msg.dict_data = new_ldr_id
									#On recieving ack initiate delete


								#send delete_msg to leader and stop
								delete_msg = Message(Msg_type['delete_node'])
								delete_msg._source_host,delete_msg._source_port=self.HOST,self.PORT
								delete_msg._recv_host,delete_msg._recv_port = self.network_dict[ldr_id](0),self.network_dict[ldr_id](1)
								delete_msg.dict_data = msg.dict_data

								with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
									soc.connect((self.ldr_ip,self.ldr_port))
									send_msg(soc,delete_msg) #send message

								#stop
								exit(0)

						
						# # sending back ACK
						# data = ("ACK - data received: "+str(data)).encode()
						# message_queues[s].put(data)
						# # add s as a connection waiting to send messages
						# if s not in outputs:
						#     outputs.append(s)
						inputs.remove(s)
						s.close()

			for s in writable:
				# If something has to be sent - send it. Else, remove connection from output queue
				if not message_queues[s].empty():
					# if some item is present - send it
					next_msg = message_queues[s].get()
					send_msg(connection,next_msg)
					#s.send(next_msg)
				else:
					# indicate that server has nothing to send
					outputs.remove(s)

			for s in exceptional:
				# remove this connection and all its existences
				inputs.remove(s)
				if s in outputs:
					outputs.remove(s)
				s.close()
				del message_queues[s]