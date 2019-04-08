import socket

HOST = 'localhost'  # The server's hostname or IP address
PORT = 65432        # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	# connecting to server
    s.connect((HOST, PORT))
    print("Connected to server. Press ENTER to start sending requests")
    input()
    # sending data
    s.sendall(b'Hello, world')
    # waiting for response/ACK
    data = s.recv(1024)

print('Received', repr(data))
