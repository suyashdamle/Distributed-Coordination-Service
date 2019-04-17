import time
from Message import Message
from coordination_utils import *
import threading
import signal
import queue
import socket
import select
import sys
from _thread import *
import pickle
import enum

def ldrelect_thread_fn(self):
	"""
	Tasked with the selection of the new leader
	"""
	# TODO: delete its entry from everywhere while exiting

	print("DEBUG_MSG: Leader Election started")
	self.thread_msg_qs[threading.get_ident()] = queue.Queue()
	heartbeat_msg = Message(Msg_type['heartbeat'])

	has_leader = False
	nodes = list(self.network_dict.keys())
	nodes.append(self.node_id)
	while not has_leader:
		nodes = sorted(nodes)
		for n_id in nodes:
			if n_id == self.node_id:
				msg = Message(Msg_type['ldr_proposal'])
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					try:
						s.connect((self.HOST,self.PORT))
					except:
						pass	# go to outer while loop and re-start process
					else:
						msg._source_host,msg._source_port=s.getsockname()
						msg._recv_host,msg._recv_port = (self.HOST,self.PORT)
						msg._msg_id = (self.node_id,threading.current_thread().ident)
						# assume that beyond this point, the found node stays alive...
						# ... or, this thread begins later again or in some other node
						has_leader = True
						send_msg(s, msg)
				# clear its existence before exiting
				self.ldr_elect_tid = None
				self.thread_msg_qs.pop(threading.get_ident(),None)
				return
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
		
		# now, the coordinator is responsible to pass the heartbeat messages into this thread

		# wait for timeout amount of time before deciding which all are alive
		# TODO: need to wait for multiple time-outs?
		time.sleep(self.heartbeat_delay)

		responded_nodes=set([self.node_id])
		q = self.thread_msg_qs[threading.get_ident()]
		while not q.empty():
			msg = q.get()
			responded_nodes.add(msg._msg_id[0])

		prospective_ldr = min(responded_nodes)

		msg = Message(Msg_type['ldr_proposal'])
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			try:
				s.connect((self.network_dict[prospective_ldr][0],self.network_dict[prospective_ldr][1]))
			except:
				pass	# go to outer while loop and re-start process
			else:
				msg._source_host,heartbeat_msg._source_port=s.getsockname()
				msg._recv_host,heartbeat_msg._recv_port = self.network_dict[n_id]
				msg._msg_id = (self.node_id,threading.current_thread().ident)
				# assume that beyond this point, the found node stays alive...
				# ... or, this thread begins later again or in some other node
				has_leader = True
				send_msg(s, msg)

	# clear its existence before exiting
	self.ldr_elect_tid = None
	self.thread_msg_qs.pop(threading.get_ident(),None)

def ldr_agreement_fn(self,prosp_ldr):
	# return whether the passed prospective leader could be agreed to be the...
	# ... next leader. 
	# if this node's number is lesser, just do NOT respond
	if self.node_id < prosp_ldr:
		return False
	else:
		return True

def become_ldr_thread_fn(self,evnt):
	print("Become Leader started")
	# send ldr_agreement msg to all and wait for returns
	ldr_elected = False

	msg = Message(Msg_type['new_ldr_id'],msg_id = (self.node_id,threading.current_thread().ident))
	msg._data = {'id':self.node_id,'ip':self.HOST,'port':self.PORT,'type':'proposal'}
	for n_id in self.network_dict.keys():
		new_recv = (self.network_dict[n_id][0],self.network_dict[n_id][1])
		try:
			s.connect(new_recv)
		except:
			pass
		else:
			new_msg._source_host,new_msg._source_port = s.getsockname()
			new_msg._recv_host,new_msg._recv_port = new_recv
			send_msg(s, new_msg)

	responded_nodes=set()

	timeout = threading.Timer(self.timeout_thresh*self.heartbeat_delay,become_ldr_killer,args=[self,threading.current_thread(),responded_nodes])
	while not ldr_elected:
		# the ACKs are treated as the first heartbeat message - used to get the ....
		# .. status of existing nodes
		# now wait for arrival of new messages	
		# delete nodes that have not responded	
		if len(responded_nodes) == len(self.network_dict):
			# all nodes have responded
			timeout.cancel()
			ldr_elected = True
			become_ldr_killer(self,threading.current_thread(),responded_nodes)
		evnt.wait()
		q = self.thread_msg_qs[self.node_id]		
		while not q.empty():
			msg = q.get()
			responded_nodes.add(mgs._msg_id[0])
			

def become_ldr_killer(self,thread_to_kill,responded_nodes):
	# remove from dict
	try:
		del self.thread_msg_qs[thread_to_kill.ident]
	except:
		pass
	# kill this thread	
	self.is_leader =  True
	# update the IP dict:
	new_dict={}
	for n_id in self.network_dict:
		if n_id in responded_nodes:
			new_dict[n_id] = self.network_dict[n_id]
	self.network_dict = new_dict
	# kill & restart heartbeat thread
	self.abort_heartbeat = True
	self.ldr_id = self.node_id
	self.ldr_port = self.PORT
	self.ldr_ip = self.HOST
	self.heartbeat_thread.join()
	del self.thread_msg_qs[self.heartbeat_tid]
	self.heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
	self.heartbeat_thread.start()
	self.heartbeat_tid = self.heartbeat_thread.ident
	self.thread_msg_qs[self.heartbeat_tid] = queue.Queue()
	heartbeat_tid = self.heartbeat_thread.ident
	self.ldr_alive = True
	self.become_ldr_tid = None
	print("Leader election complete: ",self.ldr_id," ",self.ldr_port)
	# return from this thread === kill it
	return
	#thread_to_kill.join()




