import enum
import socket
import pickle
import struct

class Msg_type(enum.Enum):
	heartbeat = 1
	read_req = 2
	write_req = 3
	leaderelect = 4
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
	msg = pickle.dumps(msg)
	msg = struct.pack('>I', len(msg)) + msg
	sock.sendall(msg)

def recvall(sock, n):
	# Helper function to recv n bytes or return None if EOF is hit
	data = b''
	while len(data) < n:
		packet = sock.recv(n - len(data))
		if not packet:
			# only part of the data received so far
			return None
		data += packet
	return data