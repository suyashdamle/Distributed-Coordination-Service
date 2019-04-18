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

ip = "127.0.0.1"
port = 40000
data = None
with open("./temp2.txt", 'rb') as f:
	data = f.read()

data_dict = {}
data_dict['file'] = data
data_dict['filedir'] = './root/a/'
data_dict['filename'] = 'temp2.txt'
msg = Message(Msg_type['write_req'], recv_host = ip, recv_port = port, data_dict = data_dict)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.connect((ip, port))
	send_msg(s, msg)
	new_msg = recv_msg(s)
	status = new_msg._data_dict['status']
	if status == -1:
		print("File write failed")
	elif status == 1:
		print("File write successful")
	else:
		print("ERROR")
s.close()
