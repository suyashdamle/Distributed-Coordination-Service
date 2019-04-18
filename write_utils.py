from Message import Message
import socket
from coordination_utils import *
import threading
import os
import queue

#should delete entries of queue in dict at exit point of each thread
#for this, create a func which cleans ALL dicts

def clear_write_req_data(self, write_req_id):
	tid = self.wrreq_tids[write_req_id]
	del self.thread_msg_qs[tid]
	del self.wrreq_tids[write_req_id]
	if write_req_id in self.wrreq_conditions:
		del self.wrreq_conditions[write_req_id]
	return

def clear_write_data(self, write_id):
	tid = self.write_tids[write_id]
	del self.thread_msg_qs[tid]
	del self.write_tids[write_id]
	if write_id in self.write_conditions:
		del self.write_conditions[write_id]
	return

def send_msg_to_client(self, client_ip, client_port, status, sock, write_req_id):
	reply_data = {}
	reply_data['status'] = status
	reply_msg = Message(Msg_type['write_reply'], recv_host = client_ip, recv_port = client_port, data_dict = reply_data)
	reply_msg._msg_id = (self.node_id, threading.current_thread().ident)
	send_msg(sock, reply_msg)
	#close the socket
	self.inputs.remove(sock)
	sock.close()
	#clean dicts
	self.clear_write_req_data(write_req_id)
	return

def send_new_msg(self, ip, port, msg):
	msg._msg_id = (self.node_id, threading.current_thread().ident)
	try:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.connect((ip, port))
			send_msg(s, msg)
		s.close()
		return True
	except Exception as e:
		print("Exception encountered while sending message to "+str(ip)+":"+str(port))
		print(e)
		return False

def write_req_handler(self, msg, cond, write_req_id, sock):
	#close sock before return
	print("DEBUG_MSG: New write request received, entered write_req_handler")
	data = msg._data_dict
	client_ip = msg._source_host
	client_port = msg._source_port
	data['write_req_id'] = write_req_id
	route_msg = Message(Msg_type['WR_ROUTE'], recv_host = self.ldr_ip, recv_port = self.ldr_port, data_dict = data)
	ret = self.send_new_msg(self.ldr_ip, self.ldr_port, route_msg)
	if not ret:
		print("DEBUG_MSG: Failed sending Route message to leader")
		self.send_msg_to_client(client_ip, client_port, -1, sock, write_req_id)
		return	

	#so now initial node(might be leader as well) is running this function and will inform client abt success failure from here itself
	#waiting for reply from leader
	with cond:
		timeout = cond.wait(timeout = self.timeout_write_req)
	#notify called on receiving a REPLY msg or timeout occurred
	q = self.thread_msg_qs[threading.current_thread().ident]
	if q.empty() or timeout is False:
		#there is some error, should report it
		self.send_msg_to_client(client_ip, client_port, -1, sock, write_req_id)
		return
	else:
		ldr_reply_msg = q.get()

	if Msg_type(ldr_reply_msg._m_type) is Msg_type.WR_REPLY:
		succ = ldr_reply_msg._data_dict['write_succ']
		if succ:
			self.send_msg_to_client(client_ip, client_port, +1, sock, write_req_id)
		else:
			self.send_msg_to_client(client_ip, client_port, -1, sock, write_req_id)
	else: 
		#some error
		self.send_msg_to_client(client_ip, client_port, -1, sock, write_req_id)
		pass

	return

def routed_write_handler(self, msg, cond):
	if not self.is_leader or not self.ldr_alive or not self.add_node or self.sponsor_node_count > 0:
		print("Entered stopping area in leader")
		if not self.is_leader:
			print("LOAD : No leader")
		if not self.ldr_alive:
			print("LOAD : Leader not alive")
		if not self.add_node:
			print("LOAD : Node not added yet")
		if not self.sponsor_node_count > 0:
			print("LOAD : Leader sponsoring new node rn")
		source_nid = msg._msg_id[0]
		if source_nid == self.node_id:
			reply_node_ip  = self.HOST
			reply_node_port = self.PORT
		else:
			reply_node_ip  = self.network_dict[source_nid][0]
			reply_node_port = self.network_dict[source_nid][1]
		# report some error, as routed write req must only be received by leader
		write_req_id = msg._data_dict['write_req_id']
		reply_data = {}
		reply_data['write_req_id'] = write_req_id
		reply_data['write_succ'] = False
		reply_msg = Message(Msg_type['WR_REPLY'], recv_host = reply_node_ip, recv_port = reply_node_port, data_dict = reply_data)
		ret = self.send_new_msg(reply_node_ip, reply_node_port, reply_msg)
		if not ret:
			pass
		#no need to clean anything here, as no queue/write_id is generated yet
	else :
		#start 2PC	
		self.two_phase_commit(msg, cond)
	return


#DONT FORGET TO SEND WRITE_ID IN EACH MESSAGE
#this is only called by leader
#this message should have 'filedir', 'filename' and 'file' fields in it's _data_dict
def two_phase_commit(self, msg, cond):
	#create a entry in write_ids dict
	self.global_write_id += 1
	curr_write_id = self.global_write_id
	self.write_tids[curr_write_id] = threading.current_thread().ident
	self.thread_msg_qs[threading.current_thread().ident] = queue.Queue()
	self.write_conditions[curr_write_id] = cond

	source_nid = msg._msg_id[0]
	if source_nid == self.node_id:
		reply_node_ip  = self.HOST
		reply_node_port = self.PORT
	else:
		reply_node_ip  = self.network_dict[source_nid][0]
		reply_node_port = self.network_dict[source_nid][1]
	write_req_id = msg._data_dict['write_req_id']
	reply_data = {}
	reply_data['write_req_id'] = write_req_id

	q = self.thread_msg_qs[threading.current_thread().ident]

	# n_active_nodes = len(self.network_dict)

	with cond:

		#store file's older version 
		write_succ = True
		msgdata = msg._data_dict
		filedir = msgdata['filedir']
		filename = msgdata['filename']
		filepath = filedir + '/' + filename
		exists = os.path.exists(filepath)
		older = None
		#store older version only if file existed previously
		if exists:
			try :
				with open(filepath, 'rb') as f:
					older = f.read()
					f.close()
			except IOError as e:
				printf("Exception : Error loading file, need to exit\n", filepath)
				write_succ = False
		else : 
			#no need to do anything
			pass		

		#write newer version (go through directory structure and write)
		if not os.path.exists(filedir):
			#need to create the directory first
			os.makedirs(filedir)

		try:
			with open(filepath, 'wb') as f:
				f.write(msgdata['file'])
				f.close()
			#newer version successfully written
		except IOError as e:
			printf("Exception : Error writing to file, will abort\n", filepath)
			write_succ = False
		
		if write_succ:
			#version no. to be changed in metadata only after all AGREED
			pass
		else :
			#send ABORT to reply_node, so that it can forward it to client
			reply_data['write_succ'] = write_succ
			reply_msg = Message(Msg_type['WR_REPLY'], recv_host = reply_node_ip, recv_port = reply_node_port, data_dict = reply_data)
			ret = self.send_new_msg(reply_node_ip, reply_node_port, reply_msg)
			if not ret:
				print("Failed to send ABORT message to node connected to client")
			self.clear_write_data(curr_write_id)
			return

		#send COMMIT_REQ message to all cohorts
		msgdata['write_id'] = curr_write_id
		COMMIT_REQ_msg = Message(Msg_type['WR_COMMIT_REQ'], data_dict=msgdata)
		for node_id, ip_port in self.network_dict.items():
			if ip_port[0] == self.ldr_ip  and ip_port[1] == self.ldr_port :
				continue
			tries = 0
			while tries < self.max_tries:
				ret = self.send_new_msg(ip_port[0], ip_port[1], COMMIT_REQ_msg)
				tries += 1
				if not ret:
					if tries == self.max_tries:
						print("Failed to send COMMIT_REQ message after multiple tries, node must have died")
					else:
						print("Try : "+str(tries)+" - Failed to send COMMIT_REQ message, retrying")
				else:
					break

		#wait for AGREED/ABORT from all (timeout big/dynamic) - TODO
		n_agreed = 0
		n_abort = 0
		fail = False

		#hoping n_active_nodes is dynamic (changes on addition/crash of nodes)
		while(n_agreed+n_abort < len(self.network_dict)) :
			timeout = cond.wait(timeout = self.timeout_2pc)
			if timeout is False:
				if n_agreed >= len(self.network_dict):
					write_succ = True
				else :
					fail = True
					write_succ = False
				break
			#came here, so notify is called, that means we can extract msg from q
			while not q.empty():
				new_msg = q.get()
				if Msg_type(new_msg._m_type) is Msg_type.WR_ABORT:
					n_abort += 1
					write_succ = False
				elif Msg_type(new_msg._m_type) is Msg_type.WR_AGREED:
					n_agreed += 1

		#if any ABORTS/*timeout*(even one time), undo the local changes and send fail reply msg to node 
		if n_abort > 0 or fail is True:
			#write back older version and discard new one
			if not exists :
				#file was not present and new file was created
				#so removing the newly created file
				os.remove(filepath)
			else :
				with open(filepath, 'wb') as f:
					f.write(older)
					f.close()
				del older
			reply_data['write_succ'] = False 
			reply_msg = Message(Msg_type['WR_REPLY'], recv_host = reply_node_ip, recv_port = reply_node_port, data_dict = reply_data)
			ret = self.send_new_msg(reply_node_ip, reply_node_port, reply_msg)
			if not ret:
				print("Failed to send Failure-REPLY message to node connected to client")

		#n_agreed must be len(self.network_dict) here
		if write_succ :
			#if all AGREED, leader commits the changes
			del older	#or older = None
			if filepath not in self.meta_data:
				self.meta_data[filepath] = [1, filepath, 0, [], False]
			self.meta_data[filepath][2] += 1
			msgdata['metadata'] = self.meta_data[filepath]
			#send COMMIT message to all
			COMMIT_msg = Message(Msg_type['WR_COMMIT'], data_dict=msgdata)
			for node_id, ip_port in self.network_dict.items():
				if ip_port[0] == self.ldr_ip and ip_port[1] == self.ldr_port :
					continue
				tries = 0
				while tries < self.max_tries:
					ret = self.send_new_msg(ip_port[0], ip_port[1], COMMIT_msg)
					tries += 1
					if not ret:
						if tries == self.max_tries:
							print("Failed to send COMMIT message after multiple tries, node must have died")
						else:
							print("Try : "+str(tries)+" - Failed to send COMMIT message, retrying")
					else:
						break
		else :
			#send ABORT message to all
			ABORT_msg = Message(Msg_type['WR_ABORT'], data_dict=msgdata)
			for node_id, ip_port in self.network_dict.items():
				if ip_port[0] == self.ldr_ip and ip_port[1] == self.ldr_port :
					continue
				ret = self.send_new_msg(ip_port[0], ip_port[1], ABORT_msg)
				if not ret:
					print("Failed to send ABORT message")
			self.clear_write_data(curr_write_id)
			return

		n_acks = 0
		#wait for ACK from all
		while n_acks < len(self.network_dict):
			timeout = cond.wait(timeout = self.timeout_write)
			if timeout is False:
				if n_acks >= len(self.network_dict):
					write_succ = True
				else :
					write_succ = False
				break
			while not q.empty():
				new_msg = q.get()
				if Msg_type(new_msg._m_type) is Msg_type.WR_ACK:
					n_acks += 1

		#send a "REPLY" msg to reply_node
		reply_data['write_succ'] = write_succ
		reply_msg = Message(Msg_type['WR_REPLY'], recv_host = reply_node_ip, recv_port = reply_node_port, data_dict = reply_data)
		tries = 0
		while tries < self.max_tries:
			ret = self.send_new_msg(reply_node_ip, reply_node_port, reply_msg)
			tries += 1
			if not ret:
				if tries == self.max_tries:
					print("Failed to send REPLY message after multiple tries, node must have died")
				else:
					print("Try : "+str(tries)+" - Failed to send REPLY message to node connected to client, retrying")
			else:
				break
		self.clear_write_data(curr_write_id)
		return


def non_leader_write_handler(self, msg, cond):
	#activated on receiving COMMIT_REQ message^
	#update global write id
	curr_write_id = msg._data_dict['write_id']
	self.global_write_id = max(self.global_write_id, curr_write_id)
	source_nid = msg._msg_id[0]
	if source_nid == self.node_id:
		reply_node_ip  = self.HOST
		reply_node_port = self.PORT
	else:
		reply_node_ip  = self.network_dict[source_nid][0]
		reply_node_port = self.network_dict[source_nid][1]
	
	if not self.ldr_alive or not self.add_node or self.sponsor_node_count > 0:
		print("Entered stopping area in node")
		if not self.ldr_alive:
			print("LOAD : Leader not alive")
		if not self.add_node:
			print("LOAD : Node not added yet")
		if not self.sponsor_node_count > 0:
			print("LOAD : Node sponsoring new node rn")
		reply_data = {}
		reply_data['write_id'] = curr_write_id
		reply_msg = Message(Msg_type['WR_ABORT'], data_dict=reply_data)
		ret = self.send_new_msg(reply_node_ip, reply_node_port, reply_msg)
		if not ret:
			pass
		self.clear_write_data(curr_write_id)
		return

	#store file's older version
	write_succ = True
	msgdata = msg._data_dict
	filedir = msgdata['filedir']
	filename = msgdata['filename']
	filepath = filedir + '/' + filename
	exists = os.path.exists(filepath)
	older = None
	#store older version only if file existed previously
	if exists:
		try :
			with open(filepath, 'rb') as f:
				older = f.read()
				f.close()
		except IOError as e:
			printf("Exception : Error loading file, need to exit\n", filepath)
			write_succ = False
	else : 
		#no need to do anything
		pass		

	#write newer version (go through directory structure and write)
	if not os.path.exists(filedir):
		#need to create the directory first
		os.makedirs(filedir)
	try:
		with open(filepath, 'wb') as f:
			f.write(msgdata['file'])
			f.close()
		#newer version successfully written
	except IOError as e:
		printf("Exception : Error writing to file, will abort\n", filepath)
		write_succ = False
	
	#send AGREED/ABORT Message to leader (from whom COMMIT_REQ was recvd)
	data_dict = {}
	data_dict['write_id'] = msg._data_dict['write_id']
	if write_succ:
		new_msg = Message(Msg_type['WR_AGREED'], data_dict=data_dict)
	else:
		new_msg = Message(Msg_type['WR_ABORT'], data_dict=data_dict)
	tries = 0
	while tries < self.max_tries:
		ret = self.send_new_msg(reply_node_ip, reply_node_port, new_msg)
		tries += 1
		if not ret:
			if tries == self.max_tries:
				print("Failed to send message(response to COMMIT_REQ) after multiple tries, node must have died")
			else:
				print("Try : "+str(tries)+" - Failed to send message(response to COMMIT_REQ) to leader, retrying")
		else:
			break

	if not write_succ :
		self.clear_write_data(curr_write_id)
		return

	#will also receive COMMIT message in its queue, need to wait till that time
	with cond:
		timeout = cond.wait(timeout = self.timeout_write)

	#will come here when notify is called from coordinator thread or timeout
	#now need to take out message out of queue
	q = self.thread_msg_qs[threading.current_thread().ident]
	if q.empty() or timeout is False:
		#there is some error, should report it
		self.clear_write_data(curr_write_id)
		return
	else:
		reply_msg = q.get()

	if Msg_type(reply_msg._m_type) is Msg_type.WR_ABORT:
		#write back older version and discard new one
		if not exists :
			#file was not present and new file was created
			#so removing the newly created file
			os.remove(filepath)
		else :
			with open(filepath, 'wb') as f:
				f.write(older)
				f.close()
			del older

	elif Msg_type(reply_msg._m_type) is Msg_type.WR_COMMIT:
		#then on receiving COMMIT msg, discard the older version
		del older	#or older = None
		#update version no and send ACK
		self.meta_data[filepath] = reply_msg._data_dict['metadata']
		while tries < self.max_tries:
			ret = self.send_new_msg(reply_node_ip, reply_node_port, Message(Msg_type['WR_ACK'], data_dict=data_dict))
			tries += 1
			if not ret:
				if tries == self.max_tries:
					print("Failed to send ACK after multiple tries, leader must have died")
				else:
					print("Try : "+str(tries)+" - Failed to send ACK to leader, retrying")
			else:
				break
	else :
		#load
		#should be some error
		pass

	#clear queue
	self.clear_write_data(curr_write_id)
	return
