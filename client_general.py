from coordination_utils import *
from add_node_utils import *
from write_utils import *
from delete_node_utils import *
from Message import Message
from read_utils import receive_file

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

function = sys.argv[1]

if function == 'write':
    ip = input('Enter node IP: ')
    port = int(input('Enter node PORT: '))
    file_path = input('Enter local file path: ')
    server_dir = input('Enter server directory path: ')
    filename = input('Enter file name: ')
    data = None

    with open(file_path, 'rb') as f:
	    data = f.read()

    data_dict = {}
    data_dict['file'] = data
    data_dict['filedir'] = server_dir
    data_dict['filename'] = filename
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



elif function == 'read':
    ip = input('Enter node IP: ')
    port = int(input('Enter node PORT: '))
    server_dir = input('Enter server directory path: ')
    filename = input('Enter file name: ')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(None)
        s.connect((ip, port))
        send_msg(s, Message(Msg_type['read_request'], recv_host = ip, recv_port = port, data_dict = {'filename': filename,\
                                                                                                    'filedir': server_dir}))
        receive_file('./',s)

    s.close()


elif function == 'delete_node':
    ip = input('Enter node IP: ')
    port = int(input('Enter node PORT: '))
    data_dict = {}
    msg = Message(Msg_type['init_delete'], recv_host = ip, recv_port = port, data_dict = data_dict)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, port))
        send_msg(s, msg)
    s.close()