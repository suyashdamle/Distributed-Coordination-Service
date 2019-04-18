from coordination_utils import *
import os
from Message import Message
import sys

def send_file(self,filename,filepath,sock):

	print("Preparing to send file...")
	full_file_name = filepath + '/' + filename

	f = open(full_file_name,"rb")
	file_size = os.path.getsize(full_file_name)

	send_msg(sock, Message(Msg_type['file_data'], data_dict = {'name': filename ,'size': file_size}))

	while True:
		chunk = f.read(self.buffer_size)
		if not chunk:
			break  # EOF
		send_msg(sock, Message(Msg_type['file_data'], data_dict = {'data': chunk}))

	print("File sent successfully!")

	f.close()
	self.inputs.remove(sock)
	sock.close()

def receive_file(dest_path,sock):
	
	message = recv_msg(sock)
	file_name = message.get_data('name')
	file_size = message.get_data('size')
	print("File name is ",file_name," size is ",file_size)

	file_pointer = open(dest_path+'/'+file_name,'wb')
	current_size = 0

	try:
		while True:
				message = recv_msg(sock)
				file_pointer.write(message.get_data('data'))
				current_size+= sys.getsizeof(message.get_data('data'))
				print("File size transferred ",current_size)
				if current_size >= file_size:
					break
	except:
		print("Connection broken. Exiting...")
		os._exit(0)
	file_pointer.close()

	sock.close()

	print("File received successfully !")