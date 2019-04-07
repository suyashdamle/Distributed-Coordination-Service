import enum
import socket

class Msg_type(enum.Enum):
	heartbeat = 1
	read_req = 2
	write_req = 3
	# TODO: add more!!

def recv_msg(sock):
		msglen = recvall(sock, 4)
		if not msglen:
			return None
		msglen = struct.unpack('>I', raw_msglen)[0]
		return recvall(sock, msglen)

def send_msg(sock, msg):
	"""
	sends message, appended by the length (in bytes) of the message
	Params:
		sock : the bound socket
		msg  : byte-stream to be sent 
	"""
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