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
		# TODO : read config file and populate lists and metadata
		# TODO : create an dictionary object containing standard IP's as key and port number as value
		self.HOST = host  				# Standard loopback interface address (localhost)
		self.PORT = port       			# Port to listen on
		self.thread_msg_qs = {}			# Each thread gets a msg queue, key = its thread-id;val= a Queue obj
		self.network_dict={}			# A dict of type 	[node_id : <host_ip,host_port,state>]
		self.is_leader = is_leader		# whether this node is the leader or not
		self.heartbeat_delay = 5		# in seconds
		self.timeout_thresh = 3			# number of timeouts, after which a node is declared dead
		self.node_id = -1				# the node-id of this instance - changed during joining protocol
		self.config_fname = config_fname
		self.ldr_heartbeat_delay=10		# max how much delay could be expected from the leader bet heartbeats

		#self.config_table = {'127.0.0.1': 64531} 		# TODO mentioned above

		# Thread-ids of some critical processes kept as instance variables
		self.coordinator_tid = None
		self.ldr_elect_tid = None
		self.heartbeat_tid = None
		self.become_ldr_tid = None
		self.abort_heartbeat = False

		# Thread objects of some critical processes
		self.heartbeat_thread = None
		self.coordination_thread = None

		self.ldr_alive = True
		self.last_node_id = 1
		
		# present leader details
		self.ldr_id = 1
		self.ldr_ip = '127.0.0.1'
		self.ldr_port = 64532
		self.ldr_heartbeat_delay=5		# max how much delay could be expected from the leader bet heartbeats
		if self.is_leader:
			self.ldr_ip = host
			self.ldr_port = port

		# Creating the heartbeat handling thread
		self.heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
		self.heartbeat_thread.start()
		self.heartbeat_tid = self.heartbeat_thread.ident

		self.coordination_thread =  threading.Thread(target = self.coordination_thread_fn, args=(self.heartbeat_tid,))
		self.coordination_thread.start()
		self.coordinator_tid = self.coordination_thread.ident

		if not self.is_leader:
			self.main_thread_tid = threading.current_thread().ident 	# find the tid of main_thread
			self.thread_msg_qs[self.main_thread_tid] = queue.Queue()	# this queue will have messages related to add_node
			self.add_node_protocol()									# get node added to the network			

		else:
			self.node_id = 1
			print("Leader up at ip :",self.HOST," port: ",self.PORT)

		
		
	from add_node_utils import add_node_protocol,send_AN_ldr_info,assign_new_id,send_file_system,\
								AN_to_network
	from ldr_elect_utils import ldrelect_thread_fn, ldr_agreement_fn, become_ldr_thread_fn,\
								become_ldr_killer

	def thread_manager(self):
		"""
		ensures that finished threads are removed from lists of threads
		"""
		pass

	def heartbeat_thread_fn(self):
		'''
		Does all processes related to heartbeat receiving and sending
		'''
		self.abort_heartbeat = False
		self.thread_msg_qs[threading.current_thread().ident] = queue.Queue()
		heartbeat_msg = Message(Msg_type['heartbeat'],msg_id = (self.node_id, threading.current_thread().ident))

		# for a leader node
		if self.is_leader:
			# initiate time-out counts

			# dict of type   [node_id : count of time-outs]
			node_timeouts = {n_id:-1 for n_id in self.network_dict.keys()}

			while True:
				if self.abort_heartbeat:
					return
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
						try:
							node_timeouts[n_id] += 1
						except:
							node_timeouts[n_id] = 1
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
							heartbeat_msg._recv_host,heartbeat_msg._recv_port,state = self.network_dict[n_id]
							heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
							send_msg(s, heartbeat_msg)

				# re-starting timer
				time.sleep(self.heartbeat_delay)
		
		# for a non-leader node

		
		else:
			ldr_timeout_count = -1
			while True:
				if self.abort_heartbeat:
					return
				if self.ldr_alive:
					q = self.thread_msg_qs[threading.current_thread().ident]
					got_ldr_hbeat = False

					while not q.empty():
						hmsg = q.get()
						print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
						ldr_timeout_count =  0

						# reply to heartbeat
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							hbeat_id = hmsg._msg_id[0]
							if not(hbeat_id == self.node_id):
								hmsg_ip,hmsg_port,state = self.network_dict[hbeat_id]
							else:
								hmsg_ip,hmsg_port,state = (self.HOST,self.PORT,1)
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
						ldr_timeout_count= 0
						print("Leader failure detected")
						self.ldr_alive = False
						try:
							del self.network_dict[self.ldr_id]
						except:
							pass
						leader_elect_thread = threading.Thread(target=self.ldrelect_thread_fn,args=())
						leader_elect_thread.start()

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
							# also send the heartbeat to leader election thread
							if self.ldr_elect_tid is not None:
								try:
									self.thread_msg_qs[self.ldr_elect_tid].put(msg)
								except:
									pass

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

						elif Msg_type(msg._m_type) is Msg_type.ldr_proposal:
							# spawn a become_leader thread if it doesnt exist and pass future messages to it
							if self.become_ldr_tid is None:
								become_ldr_evnt = threading.Event()
								become_ldr_thread = threading.Thread(target = self.become_ldr_thread_fn,args=(become_ldr_evnt,))
								become_ldr_thread.start()
								become_ldr_tid = become_ldr_thread.ident
								self.thread_msg_qs[become_ldr_tid] = queue.Queue()

						elif Msg_type(msg._m_type) is Msg_type.new_ldr_id:
							# first check if this is a reply for earlier ldr_agreement sent from here:
							if msg.get_data('type')=='reply':
								# send to become_leader_thread and let it take :
								self.thread_msg_qs[become_ldr_tid].put(msg)
								become_ldr_evnt.set()

							# if it is a msg from some other node and seeks vote for itself
							else:
								self.ldr_id = msg.get_data('id')
								self.ldr_ip = msg.get_data('ip')
								self.ldr_port = msg.get_data('port')
								self.ldr_alive = True

								if ldr_agreement_fn(msg._msg_id[0]):
									new_msg = Message(Msg_type['new_ldr_id'],msg_id = (self.node_id,threading.current_thread().ident))
									new_recv = (self.network_dict[msg._msg_id[0]][0],self.network_dict[msg._msg_id[0]][1])
									new_msg._data={'type':'reply','ans':'ACK'}
									try:
										s.connect(new_recv)
									except:
										pass
									else:
										new_msg._source_host,new_msg._source_port = s.getsockname()
										new_msg._recv_host,new_msg._recv_port = new_recv
										send_msg(s, new_msg)


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