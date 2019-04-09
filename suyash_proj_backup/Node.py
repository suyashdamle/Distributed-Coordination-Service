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
from Message import Message

class Node(object):
	"""
	The Node object runs on each node/server and does all tasks relating to 
	crash detection, serving clients on this server, etc.
	"""
	

	def __init__(self, config_fname=None,host=None,port=None):

		# instance members
		# TODO : read config file and populate lists and metadata
		self.HOST = '127.0.0.1'  		# Standard loopback interface address (localhost)
		self.PORT = 64532       		# Port to listen on
		self.thread_msg_qs = {}			# Each thread gets a msg queue, key = its thread-id;val= a Queue obj
		self.network_dict={}			# A dict of type 	[node_id : <host_ip, host_port, dead/alive status>]
		self.is_leader = False			# whether this node is the leader or not
		self.heartbeat_delay = 5		# in seconds
		self.timeout_thresh = 3			# number of timeouts, after which a node is declared dead
		self.node_id = -1				# the node-id of this instance - changed during joining protocol
		self.config_fname = config_fname

		# Thread-ids of some critical processes kept as instance variables
		self.coordinator_tid = None

		# present leader details
		self.ldr_ip = '127.0.0.1'
		self.ldr_port = 64532
		self.ldr_heartbeat_delay=5		# max how much delay could be expected from the leader bet heartbeats


		# Creating the heartbeat handling thread
		heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
		heartbeat_thread.start()
		heartbeat_tid = heartbeat_thread.ident
		
		# TODO : decide parameters of each thread function
		coordination_thread =  threading.Thread(target = self.coordination_thread_fn, args=(heartbeat_tid,))
		coordination_thread.start()
		self.coordinator_tid = coordination_thread.ident


	def thread_manager(self):
		"""
		ensures that finished threads are removed from lists of threads
		"""

	def leaderelect_thread_fn(self):
		"""
		"""


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
				if not q.empty():
					hmsg = q.get()
					print("DEBUG_MSG: got heartbeat_msg from: ",(hmsg._source_host,hmsg._source_port))
					ldr_timeout_count =  0

					# reply to heartbeat
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						try:
							s.connect((self.ldr_ip, self.ldr_port))
						except:
							pass
						else:
							heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
							heartbeat_msg._recv_host,heartbeat_msg._recv_port = (self.ldr_ip,self.ldr_port)
							heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
							send_msg(s, heartbeat_msg)
					
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