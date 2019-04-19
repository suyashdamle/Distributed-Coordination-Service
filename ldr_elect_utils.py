import time
from Message import Message
from coordination_utils import *
import threading
from threading import Lock
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

	print("DEBUG_MSG: Leader Election started ")
	self.thread_msg_qs[threading.get_ident()] = queue.Queue()
	heartbeat_msg = Message(Msg_type['heartbeat'])

	has_leader = False
	nodes = list(self.network_dict.keys())
	nodes.append(self.node_id)
	while not has_leader and not self.ldr_alive:
		nodes = sorted(nodes)
		print(nodes)
		# if this is itself the smallest id node
		if nodes[0] == self.node_id:
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

		for n_id in nodes:
			if n_id == self.node_id:
				continue
			print("DEBUG_MSG: sending heartbeat from ldr_elect to: ",n_id)
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				try:
					s.connect((self.network_dict[n_id][0],self.network_dict[n_id][1]))
				except:
					pass
				else:
					heartbeat_msg._source_host,heartbeat_msg._source_port=s.getsockname()
					heartbeat_msg._recv_host,heartbeat_msg._recv_port,status = self.network_dict[n_id]
					heartbeat_msg._msg_id = (self.node_id,threading.current_thread().ident)
					send_msg(s, heartbeat_msg)
		
		# now, the coordinator is responsible to pass the heartbeat messages into this thread

		# wait for timeout amount of time before deciding which all are alive
		# TODO: need to wait for multiple time-outs?
		time.sleep(self.heartbeat_delay*self.timeout_thresh)

		responded_nodes=set([self.node_id])
		q = self.thread_msg_qs[threading.current_thread().ident]
		while not q.empty():
			msg = q.get()
			responded_nodes.add(msg._msg_id[0])
		print("DEBUG_MSG: responded_nodes: ",responded_nodes)
		prospective_ldr = min(responded_nodes)

		msg = Message(Msg_type['ldr_proposal'])
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			if not(prospective_ldr == self.node_id): 
				new_recv = (self.network_dict[prospective_ldr][0],self.network_dict[prospective_ldr][1])
			else:
				new_recv = (self.HOST,self.PORT)
			try:
				s.connect(new_recv)
			except:
				pass	# go to outer while loop and re-start process
			else:
				msg._source_host,msg._source_port=s.getsockname()
				msg._recv_host,msg._recv_port = new_recv
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
	# send ldr_agreement msg to all and wait for returns
	ldr_elected = False

	msg = Message(Msg_type['new_ldr_id'],msg_id = (self.node_id,threading.current_thread().ident))
	msg._data_dict = {'id':self.node_id,'ip':self.HOST,'port':self.PORT,'type':'proposal'}
	for n_id in self.network_dict.keys():
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			print("Become Leader started: ",n_id)
			new_recv = (self.network_dict[n_id][0],self.network_dict[n_id][1])
			try:
				s.connect(new_recv)
			
			except:
				continue
			else:
				msg._source_host,msg._source_port = s.getsockname()
				msg._recv_host,msg._recv_port = new_recv
				print("DEBUG_MSG: sending new leader msg to :",n_id)
				send_msg(s, msg)
	try:
		del self.thread_msg_qs[thread_to_kill.ident]
	except:
		pass
	# kill this thread	
	self.is_leader =  True
	self.ldr_id = self.node_id
	self.ldr_port = self.PORT
	self.ldr_ip = self.HOST
	self.ldr_alive = True
	print("Leader election complete: ",self.ldr_id," ",self.ldr_port)
	
	##############################
	# wait for 1 complete heartbeat cycle - to update network table
	print("UPDATING NETWORK TABLE")
	time.sleep(10)#self.heartbeat_delay*self.timeout_thresh + 
	
	print("INITIATING CONSISTENCY CHECK:")
	new_msg = Message(Msg_type['send_metadata'],msg_id = (self.node_id,threading.current_thread().ident))
	# send to all alive nodes - try a max_tries number of times:
	for n_id in self.network_dict.keys():
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			new_recv = (self.network_dict[n_id][0],self.network_dict[n_id][1])
			new_msg._source_host,new_msg._source_port = s.getsockname()
			new_msg._recv_host,new_msg._recv_port = new_recv
			for i in range(self.max_tries):	
				try:
					s.connect(new_recv)
					send_msg(s, new_msg)			
				except:
					continue
				else:	
					break
	# wait on queue for all meta datas:
	q = self.thread_msg_qs[threading.current_thread().ident]		
	# received all metadatas in queue
	# TODO: handle the waiting differently?
	node_metadata_dict = {}			# all node-ids with metadata
	count_tries = 0
	while  len(node_metadata_dict) !=  len(self.network_dict) and count_tries<self.max_tries:
		time.sleep(self.heartbeat_delay*self.timeout_thresh)
		while not q.empty():
			msg = q.get()
			if not (msg._m_type is Msg_type.metadata_info):
				continue
			node_metadata_dict[msg._msg_id[0]] = msg._data_dict['meta-data']
		count_tries+=1
	
	# since this far exceeds heartbeat timeouts, all nodes that could respond have responded
	file_version_dict = {}		# stores file path with corresponding latest version number and id of node with latest data

	all_files_set = set()
	for n_id, metadata in node_metadata_dict.items():
		for entry,data in metadata.items():
			if data[0] == 0: 	# ignore directories
				continue
			else:
				all_files_set.add(entry)
	# collected all files
	for file in all_files_set:
		# data for a file has: [latest version no, node with latest version, is there inconsistency]		
		file_version_dict[file]=[-1, self.node_id,False]
		if file not in self.meta_data:
			file_version_dict[file][2]=True
			file_version_dict[file][0]=-1
		else:
			file_version_dict[file][0]=self.meta_data[file][2]

		for node,metadata in node_metadata_dict:
			file_metadata = metadata[file]
			if file_version_dict[file][0] != file_metadata[2]:
				file_version_dict[file][2] =  True
			if file_version_dict[file][0] < file_metadata[2]:
				file_version_dict[file][0] = file_metadata[2]
				file_version_dict[file][1] = node

	# We now have records of most recent file versions AND the nodes which have them

	# # Now, update itself to latest version of all files: - use read command
	# for file in all_files_set:
	# 	if file not in self.meta_data or file_version_dict[0] > self.meta_data[file][2]:
	# 		file_version_dict[file][2] = True
	# 		# copy (possibly large) file from another server
	# 		pass

	# Update all files that have inconsistency somewhere
	inconsistent_files = set()
	for file,entry in file_version_dict:
		if entry[2]:
			inconsistent_files.add(file)
	print("DEBUG_MSG: found %d number(s) of inconsistencies: " %(len(inconsistent_files)))

	for file,entry in file_version_dict:
		if entry[2]:
			# found inconsistency.... update everywhere
			new_recv = (self.network_dict[entry[1]][0],self.network_dict[entry[1]][1])
			msg = Message(Msg_type['cons_req'],msg_id = ())
			# filename, filedir, file,write_id
			msg._data_dict = {'filepath':file}
			new_recv = (self.HOST,self.PORT)
			msg._recv_host,msg._recv_port = new_recv
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				msg._source_host, msg._source_port = s.getsockname()	
				try:
					s.connect(new_recv)
				except:
					pass
				else:
					send_msg(s, new_msg)
					ack = recv_msg(s)
					# write req; status
					status = ack.get_data['status']
					print("DEBUG_MSG: Consistency Check: file: ",file,": status: ",status)


	# wipe off its existence
	try:
		del self.thread_msg_qs[thread_to_kill.ident]
	except:
		pass
	self.become_ldr_tid = None

	

	# responded_nodes=set()

	# timeout = threading.Timer(self.timeout_thresh*self.heartbeat_delay,become_ldr_killer,args=[self,threading.current_thread(),responded_nodes])
	# timeout.start()
	# while not ldr_elected:
	# 	# the ACKs are treated as the first heartbeat message - used to get the ....
	# 	# .. status of existing nodes
	# 	# now wait for arrival of new messages	
	# 	# delete nodes that have not responded	
	# 	if len(responded_nodes) == len(self.network_dict):
	# 		# all nodes have responded
	# 		timeout.cancel()
	# 		ldr_elected = True
	# 		become_ldr_killer(self,threading.current_thread(),responded_nodes)
	# 	evnt.wait()
	# 	q = self.thread_msg_qs[self,threading.current_thread()]		
	# 	while not q.empty():
	# 		msg = q.get()
	# 		responded_nodes.add(mgs._msg_id[0])
			

# def become_ldr_killer(self,thread_to_kill,responded_nodes):
# 	# remove from dict
# 	# update the IP dict:
# 	new_dict={}
# 	for n_id in self.network_dict:
# 		if n_id in responded_nodes:
# 			new_dict[n_id] = self.network_dict[n_id]
# 	self.network_dict = new_dict
# 	# kill & restart heartbeat thread
# 	# self.abort_heartbeat = True
# 	# print("heartbeat restarted")

# 	# self.heartbeat_thread.join()
# 	# del self.thread_msg_qs[self.heartbeat_tid]
# 	# self.heartbeat_thread = threading.Thread(target = self.heartbeat_thread_fn, args=())
# 	# self.heartbeat_thread.start()
# 	# self.heartbeat_tid = self.heartbeat_thread.ident
# 	# self.thread_msg_qs[self.heartbeat_tid] = queue.Queue()
# 	# heartbeat_tid = self.heartbeat_thread.ident
# 	# return from this thread === kill it
# 	return
# 	#thread_to_kill.join()



