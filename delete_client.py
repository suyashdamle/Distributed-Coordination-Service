from coordination_utils import *
from add_node_utils import *
from delete_node_utils import *
from Message import Message

import time

ip = "127.0.0.1"
port = 64531

data_dict = {}


msg = Message(Msg_type['init_delete'], recv_host = ip, recv_port = port, data_dict = data_dict)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.connect((ip, port))
	send_msg(s, msg)

