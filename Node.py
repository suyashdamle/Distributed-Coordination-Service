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

class Node(object):
	"""
	The Node object runs on each node/server and does all tasks relating to 
	crash detection, serving clients on this server, etc.
	"""
	

	def __init__(self, config_fname=None,host='127.0.0.1',port=64532, is_leader = False):

		# class members
		# TODO : read config file and populate lists and metadata
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
		self.last_node_id = 0			# Value of node id's used till now
		# present leader details
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
	

	def heartbeat_thread_fn(self):
		'''
		Does all processes related to heartbeat receiving and sending
		'''
		
		self.thread_msg_qs[threading.get_ident()] = queue.Queue()
		# for a leader node
		if self.is_leader:
			# initiate time-out counts

			# dict of type   [node_id : count of time-outs]
			node_timeouts = {n_id:-1 for n_id in self.network_dict.keys()}
			heartbeat_msg = Message(Msg_type['heartbeat'])

			while True:
				responded_nodes = []
				# Collect all messages from queue:
				q = self.thread_msg_qs[threading.current_thread().ident]

				while not q.empty():
					hmsg = q.get()
					hmsg = pickle.loads(hmsg)
					print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
					responded_nodes.append((hmsg._source_host,hmsg._source_port))
				
				# correct time-out counts			
				for n_id,val in self.network_dict.items():
					if val not in responded_nodes:
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
						s.connect((self.network_dict[n_id][0],self.network_dict[n_id][1]))
						heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
						heartbeat_msg._recv_host,heartbeat_msg._recv_port = self.network_dict[n_id]
						heartbeat_msg = pickle.dumps(heartbeat_msg)
						send_msg(s, heartbeat_msg)

				# re-starting timer
				time.sleep(self.heartbeat_delay)
		
		# for a non-leader node
		else:
			ldr_timeout_count = -1
			while True:
				q = self.thread_msg_qs[threading.current_thread().ident]
				if not q.empty():
					hmsg = q.get()
					print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
					ldr_timeout_count =  0
				else:
					ldr_timeout_count += 1

				# check if leader has failed
				if ldr_timeout_count >= self.timeout_thresh:
					print("Leader failure detected")
					# TODO: initiate leader election protocol

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
						# # sending back ACK
						# data = ("ACK - data received: "+str(data)).encode()
						# message_queues[s].put(data)
						# # add s as a connection waiting to send messages
						# if s not in outputs:
						#     outputs.append(s)

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