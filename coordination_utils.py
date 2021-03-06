import enum
import socket
import pickle
import struct

class Msg_type(enum.Enum):
	heartbeat = 1
	read_req = 2
	write_req = 3
	add_node = 4			#Requesting standard IP's for leader info
	AN_ldr_info = 5 		#Sponsor node replying with leader information
	AN_assign_id = 6		#Requesting leader to assign new id
	AN_set_id = 7			#Leader replying with new id
	AN_FS_data_req = 8		#Node requesting file system data from sponsor node
	AN_FS_data = 9			#Sponsor node replying with file system data
	AN_add_to_network = 10	#Leader requesting all nodes to add a new node to the network
	new_ldr_id = 11			# sent after creation of new leader by it
	WR_COMMIT_REQ = 12
	WR_AGREED = 13
	WR_ABORT = 14
	WR_COMMIT = 15
	WR_ACK = 16
	WR_ROUTE = 17			#Route req from cohort to leader
	WR_REPLY = 18			#Final reply from leader to send to client (will contain bool for succ.)
	write_reply = 19		#Reply message from node to client telling write succeeded(1)/failed(-1) (status field)
	ldr_proposal = 20		# sent by any node to the prospective leader (least alive node-id)
	delete_node = 21		#Node/Leader requesting deletion
	init_delete = 22		#client commanding deletion
	read_request = 23		#request for read from user
	file_data = 24
	cons_req = 25
	send_metadata = 26
	metadata_info = 27
	# TODO: add more!!



def recv_msg(sock):
	"""
	returns the message object received on the connection
	"""
	msglen = recvall(sock, 4)
	if not msglen:
		return None
	msglen = struct.unpack('>I', msglen)[0]  # '>' denotes big-endian byte order
	byte_data = recvall(sock, msglen)
	return pickle.loads(byte_data)

def send_msg(sock, msg):
	"""
	sends message, appended by the length (in bytes) of the message
	Params:
		sock : the bound socket
		msg  : object to be sent 
	"""
	try:
		msg = pickle.dumps(msg)
		msg = struct.pack('>I', len(msg)) + msg
		sock.sendall(msg)
	except Exception as  e:
		print("Execption : ", e)

def recvall(sock, n):
	# Helper function to recv n bytes or return None if EOF is hit
	data = b''
	while len(data) < n:
		try:
			packet = sock.recv(n - len(data))
		except:
			continue
		if not packet:
			# only part of the data received so far
			return None
		data += packet
		
	return data