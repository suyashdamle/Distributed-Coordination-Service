#https://stackoverflow.com/questions/17667903/python-socket-receive-large-amount-of-data
import select
import socket
import sys
import queue
import threading
from threading import Lock
from _thread import *
import pickle
import enum
import time
import json

# importing custom utility functions
from coordination_utils import *
from add_node_utils import *
from write_utils import *
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
		self.sponsor_node_count = 0
		self.file_system_port = None
		self.inputs = []
		#self.config_table = {'127.0.0.1': 64531} 		# TODO mentioned above

		# Thread-ids of some critical processes kept as instance variables
		self.coordinator_tid = None
		self.ldr_elect_tid = None
		self.heartbeat_tid = None
		self.become_ldr_tid = None
		self.pause_heartbeat = False

		# Thread objects of some critical processes
		self.heartbeat_thread = None
		self.coordination_thread = None

		self.ldr_alive = True
		self.last_node_id = 1
		
		self.sponser_set = False  		# to ignore furthur messages if sponsor is set
		self.sponsor_host = None
		self.sponsor_port = None
		self.buffer_size = 10240
		self.file_system_name = None
		self.meta_data = {}
		self.add_node_timeout = 2
		# Thread-ids of some critical processes kept as instance variables
		self.add_node = False	#becomes true when add node protocol is completed. Till then no write operation
								#should take place
		# self.config_table = {"127.0.0.1": 64532}
		# present leader details
		self.ldr_id = None
		self.ldr_ip = None
		self.ldr_port = None
		self.ldr_heartbeat_delay=5		# max how much delay could be expected from the leader bet heartbeats
		self.AN_condition = threading.Condition()
		self.ldr_timeout_count = -1 	# for heartbeat thread at non - leader nodes

		if self.is_leader:
			self.file_system_name = "root"
			self.meta_data['./root'] = (0,'./root/',-1,[],False)
			self.node_id = 1
			self.ldr_id = 1
			self.ldr_ip = host
			self.ldr_port = port
			print("Leader up at ip :",self.HOST," port: ",self.PORT)

		self.write_tids = {}			# dict of {write_id : thread_id}
		self.global_write_id = 0		# each write will be assigned a write_id so that we can differentiate between multiple write
		self.write_conditions = {}		# dict of {write_id : condition object} //for wait-notify of threads
		self.wrreq_id = 0				# When any node receives a write req, it forwards it to leader while maintaining connection with client
										#so to differentiate with the client connected thread, this var will be used 
		self.wrreq_tids = {}			# dict of {write_req_id : thread_id}
		#TODO - update this value in add/delete node
		self.n_active_nodes = 2			# not needed now : will use network_dict length
		self.timeout_write_req = 60		# in seconds -- very large, as whole 2PC protocol to be run
		self.timeout_write = 10			# in seconds -- 
		self.timeout_2pc = 30			# in seconds -- should be large as file needs to be written
		self.wrreq_conditions = {}		# dict of {wrreq_id : condition object} //for wait-notify of threads
		self.max_tries = 3				# max number of times a message will be attempted to send
		
		# Global locks
		self.ldr_stat_lock = Lock()

		# Creating the heartbeat handling thread
		self.heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
		self.heartbeat_thread.start()
		self.heartbeat_tid = self.heartbeat_thread.ident
		
		self.coordination_thread =  threading.Thread(target = self.coordination_thread_fn, args=(self.heartbeat_tid,))
		self.coordination_thread.start()
		self.coordinator_tid = self.coordination_thread.ident

		if not self.is_leader:
			with open('standard_ip.json', 'r') as fp:
				self.config_table = json.load(fp)
			self.main_thread_tid = threading.current_thread().ident 	# find the tid of main_thread
			self.thread_msg_qs[self.main_thread_tid] = queue.Queue()	# this queue will have messages related to add_node
			self.add_node_protocol()									# get node added to the network			
			
		
	from ldr_elect_utils import ldrelect_thread_fn,ldr_agreement_fn, become_ldr_thread_fn
	from add_node_utils import add_node_protocol,send_AN_ldr_info,assign_new_id,send_file_system
	from write_utils import write_req_handler, routed_write_handler, non_leader_write_handler, two_phase_commit, clear_write_req_data, clear_write_data, send_msg_to_client, send_new_msg

	

	def thread_manager(self):
		"""
		ensures that finished threads are removed from lists of threads
		"""
		pass

	def heartbeat_thread_fn(self):
		'''
		Does all processes related to heartbeat receiving and sending
		'''
		self.pause_heartbeat = False
		self.thread_msg_qs[threading.current_thread().ident] = queue.Queue()
		heartbeat_msg = Message(Msg_type['heartbeat'],msg_id = (self.node_id, threading.current_thread().ident))

		# dict of type   [node_id : count of time-outs]
		node_timeouts = {n_id:-1 for n_id in self.network_dict.keys()} # initiate time-out counts
		
		
		while True:
			# for a leader node
			if self.is_leader:
			
				if self.pause_heartbeat:
					continue
				responded_nodes = []
				# Collect all messages from queue:
				q = self.thread_msg_qs[threading.current_thread().ident]

				while not q.empty():
					hmsg = q.get()
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
				to_del = []
				for n_id in self.network_dict.keys():
					if node_timeouts[n_id] >= self.timeout_thresh:
						print("NODE : ",n_id," found unresponsive")
						# TODO: what now? - initiate node deletion phase
						to_del.append(n_id)
				# delete in self and send to all
				for n_id in to_del:
					try:
						del self.network_dict[n_id]
					except:
						pass
					try:
						del node_timeouts[n_id]
					except:
						pass

				for n_to_delete in  to_del:
					del_msg = Message(Msg_type['delete_node'],msg_id = (self.node_id, threading.current_thread().ident))
					for n_id in self.network_dict:
						new_recv = (self.network_dict[n_id][0],self.network_dict[n_id][1])
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							try:
								s.connect(new_recv)
							except:
								pass
							else:
								del_msg._source_host,del_msg._source_port=s.getsockname()
								del_msg._recv_host,del_msg._recv_port = new_recv
								del_msg._msg_id = (self.node_id,threading.current_thread().ident)
								del_msg._data_dict = {'id':n_to_delete}
								send_msg(s, del_msg)





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
							heartbeat_msg._data_dict = {}
							send_msg(s, heartbeat_msg)

				# re-starting timer
				time.sleep(self.heartbeat_delay)
		
			# for a non-leader node
			else:
				if self.pause_heartbeat:
					continue
				got_ldr_hbeat = False
				q = self.thread_msg_qs[threading.current_thread().ident]
				while not q.empty():
					hmsg = q.get()
					if ((hmsg.get_data('type') is not None) and (hmsg.get_data('type')== 'reply')):
						continue
					self.ldr_timeout_count = 0

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
							heartbeat_msg._data_dict={'type':'reply'}
							send_msg(s, heartbeat_msg)
					

				if self.ldr_alive:
					if not got_ldr_hbeat:
						self.ldr_timeout_count += 1
					else:
						self.ldr_timeout_count = 0
					# check if leader has failed
					self.ldr_stat_lock.acquire()
					if self.ldr_timeout_count >= self.timeout_thresh:
						self.ldr_timeout_count = 0						
						print("Leader failure detected")
						self.ldr_alive = False
						try:
							del self.network_dict[self.ldr_id]
						except:
							pass
						leader_elect_thread = threading.Thread(target=self.ldrelect_thread_fn,args=())
						leader_elect_thread.start()
						self.ldr_elect_tid = leader_elect_thread.ident
					self.ldr_stat_lock.release()
				# re-rstarting timer
				time.sleep(self.ldr_heartbeat_delay)
	
	

	def coordination_thread_fn(self, heartbeat_tid):

		print("Listening on port :",self.PORT)
		self.thread_msg_qs[threading.get_ident()] = queue.Queue()
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # prevents "already in use" errors
		server.setblocking(0)
		server.bind((self.HOST, self.PORT))
		server.listen(5)
		self.inputs.append(server)
		outputs = []
		message_queues = {} # message queue dict
		while self.inputs:
			readable, writable, exceptional = select.select(self.inputs, outputs, self.inputs)
			for s in readable:
				if s is server:
					# for new connections
					connection, client_address = s.accept()
					print("DEBUG_MSG: got connection request from: ",client_address)
					connection.setblocking(0)

					self.inputs.append(connection)
					print("DEBUG_MSG: Received connection request from: ",client_address)

					# creating a message queue for each connection
					message_queues[connection] = queue.Queue() 
				else:
					# if some message has been received - be it in part
					msg = recv_msg(s)	#server
					if msg:
						print("DEBUG_MSG: data received: Msg_type:", Msg_type(msg._m_type))
						if msg._msg_id is not None:
							print("Message sender id: ",msg._msg_id[0])
						msg._source_host = client_address[0]
						msg._source_port = client_address[1]
						# find message type and send to the right thread
						if Msg_type(msg._m_type) is Msg_type.heartbeat:
							self.thread_msg_qs[self.heartbeat_tid].put(msg)
							# also send the heartbeat to leader election thread
							if self.ldr_elect_tid is not None:
								#try:
								self.thread_msg_qs[self.ldr_elect_tid].put(msg)
								#except:
								#	pass

						#Add try catch statements, as on returning early, dicts might get cleared resulting in illegal access
						elif Msg_type(msg._m_type) is Msg_type.write_req:		#can be received by any node (this message comes directly from client)
							#this message should have 'filedir', 'filename' and 'file' fields in it's _data_dict
							cond = threading.Condition()
							self.wrreq_id += 1
							curr_wrreq_id = self.wrreq_id
							#DONE - pass socket identifier as arg of following func call :
							write_thread = threading.Thread(target = self.write_req_handler, args=(msg, cond, curr_wrreq_id, s))
							write_thread.start()
							self.wrreq_conditions[curr_wrreq_id] = cond
							self.wrreq_tids[curr_wrreq_id] = write_thread.ident
							self.thread_msg_qs[write_thread.ident] = queue.Queue()
							continue											#we don't want this socket to close

						elif Msg_type(msg._m_type) is Msg_type.WR_ROUTE:		#only received by a leader
							cond = threading.Condition()
							write_thread = threading.Thread(target = self.routed_write_handler, args=(msg, cond))
							write_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.WR_COMMIT_REQ:	#received by non-leader node
							#DONE - add condition var in args, for wait,invoke
							cond = threading.Condition()
							non_leader_write_thread = threading.Thread(target = self.non_leader_write_handler, args=(msg, cond))
							non_leader_write_thread.start()
							write_id = msg._data_dict['write_id']
							self.write_conditions[write_id] = cond
							self.write_tids[write_id] = non_leader_write_thread.ident
							self.thread_msg_qs[non_leader_write_thread.ident] = queue.Queue()

						elif Msg_type(msg._m_type) is Msg_type.WR_AGREED:		#only received by a leader
							try:
								write_id = msg._data_dict['write_id']
								self.thread_msg_qs[self.write_tids[write_id]].put(msg)
								with self.write_conditions[write_id]:
									self.write_conditions[write_id].notify()
							except Exception as e:
							 	print("Exception : AGREED message\n",e)

						elif Msg_type(msg._m_type) is Msg_type.WR_ABORT:		#can be received by a leader or non-leader node
							try:
								write_id = msg._data_dict['write_id']
								self.thread_msg_qs[self.write_tids[write_id]].put(msg)
								with self.write_conditions[write_id]:
									self.write_conditions[write_id].notify()		#wake up the thread to accept ABORT message from queue
							except Exception as e:
							 	print("Exception : ABORT message\n",e)

						elif Msg_type(msg._m_type) is Msg_type.WR_COMMIT:		#received by non-leader node
							try:
								write_id = msg._data_dict['write_id']
								self.thread_msg_qs[self.write_tids[write_id]].put(msg)
								with self.write_conditions[write_id]:
									self.write_conditions[write_id].notify()		#wake up the thread to accept COMMIT message from queue
							except Exception as e:
							 	print("Exception : COMMIT message\n",e)

						elif Msg_type(msg._m_type) is Msg_type.WR_ACK:			#only received by a leader
							try:
								write_id = msg._data_dict['write_id']
								self.thread_msg_qs[self.write_tids[write_id]].put(msg)
								with self.write_conditions[write_id]:
									self.write_conditions[write_id].notify()
							except Exception as e:
							 	print("Exception : ACK message\n",e)

						elif Msg_type(msg._m_type) is Msg_type.WR_REPLY:		#received by node who is in contact with client for write opn
							try:
								write_req_id = msg._data_dict['write_req_id']
								self.thread_msg_qs[self.wrreq_tids[write_id]].put(msg)
								with self.wrreq_conditions[write_id]:
									self.wrreq_conditions[write_req_id].notify()
							except Exception as e:
							 	print("Exception : REPLY message\n",e)

						elif Msg_type(msg._m_type) is Msg_type.AN_ldr_info:

							if self.sponser_set is False:		#if not received any sponsor reply yet
								self.sponser_set = True
								self.thread_msg_qs[self.main_thread_tid].put(msg)
								with self.AN_condition:
									self.AN_condition.notifyAll()	#ask thread to wake up
							else:
								pass

						#new node to be added to table
						elif Msg_type(msg._m_type) is Msg_type.AN_add_to_network:
							self.network_dict[msg.get_data('key')] = msg.get_data('value')	#populate network table
							self.last_node_id = msg.get_data('key')		#keep the field updated in case leader fails
							print("***************Updated Network Dict***************")
							print(self.network_dict)

						elif Msg_type(msg._m_type) is Msg_type.AN_set_id:	#new id assigned by leader
							if msg._source_host == self.ldr_ip and msg.get_data('port') == self.ldr_port:
								self.thread_msg_qs[self.main_thread_tid].put(msg)
								with self.AN_condition:
									self.AN_condition.notifyAll()		#ask thread to wake up

						elif Msg_type(msg._m_type) is Msg_type.AN_FS_data:							
							self.file_system_port = s
							self.thread_msg_qs[self.main_thread_tid].put(msg)
							if self.file_system_name is None:
								with self.AN_condition:
										self.AN_condition.notifyAll()		#ask thread to wake up
							else:

								pass
							
							continue

						elif Msg_type(msg._m_type) is Msg_type.add_node:	#sponsor node on receiving 'add_node'
							add_node_thread = threading.Thread(target = self.send_AN_ldr_info, args=(msg._source_host,msg.get_data('port'), ))
							add_node_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.AN_assign_id:	#ask leader for new id
							AN_assign_id_thread = threading.Thread(target = self.assign_new_id, args=(msg._source_host,msg.get_data('port'), ))
							AN_assign_id_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.AN_FS_data_req:
							send_file_system_thread = threading.Thread(target = self.send_file_system, args=(msg._source_host,msg.get_data('port'),))
							send_file_system_thread.start()

						elif Msg_type(msg._m_type) is Msg_type.ldr_proposal:
							# spawn a become_leader thread if it doesnt exist and pass future messages to it
							if self.is_leader:
								continue
							if self.become_ldr_tid is None:
								become_ldr_evnt = threading.Event()
								become_ldr_thread = threading.Thread(target = self.become_ldr_thread_fn,args=(become_ldr_evnt,))
								become_ldr_thread.start()
								self.become_ldr_tid = become_ldr_thread.ident
								self.thread_msg_qs[self.become_ldr_tid] = queue.Queue()

						elif Msg_type(msg._m_type) is Msg_type.new_ldr_id:
							# first check if this is a reply for earlier ldr_agreement sent from here:
							if msg.get_data('type')=='reply':
								# send to become_leader_thread and let it take :
								self.thread_msg_qs[become_ldr_tid].put(msg)
								become_ldr_evnt.set()

							# if it is a msg from some other node and seeks vote for itself
							else:
								self.ldr_stat_lock.acquire()
								self.ldr_timeout_count = -1*self.timeout_thresh
								self.ldr_id = msg.get_data('id')
								self.ldr_ip = msg.get_data('ip')
								self.ldr_port = msg.get_data('port')
								print("DEBUG_MSG: new leader found: ",self.ldr_id)
								if self.ldr_agreement_fn(msg._msg_id[0]):
									new_msg = Message(Msg_type['new_ldr_id'],msg_id = (self.node_id,threading.current_thread().ident))
									new_recv = (self.network_dict[msg._msg_id[0]][0],self.network_dict[msg._msg_id[0]][1])
									new_msg._data_dict={'type':'reply','ans':'ACK'}
									try:
										s.connect(new_recv)
									except:
										pass
									else:
										new_msg._source_host,new_msg._source_port = s.getsockname()
										new_msg._recv_host,new_msg._recv_port = new_recv
										send_msg(s, new_msg)
								self.ldr_alive = True
								self.ldr_stat_lock.release()

						self.inputs.remove(s)
						s.close()

			for s in writable:
				# If something has to be sent - send it. Else, remove connection from output queue
				if not message_queues[s].empty():
					# if some item is present - send it
					next_msg = message_queues[s].get()
					send_msg(s,next_msg)
					#s.send(next_msg)
				else:
					# indicate that server has nothing to send
					outputs.remove(s)

			for s in exceptional:
				# remove this connection and all its existences
				self.inputs.remove(s)
				if s in outputs:
					outputs.remove(s)
				s.close()
				del message_queues[s]