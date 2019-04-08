import select
import socket
import sys
import queue

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # prevents "already in use" errors
server.setblocking(0)
server.bind((HOST, PORT))
server.listen(5)

# 3 lists
inputs = [server]
outputs = []
message_queues = {} # message queue dict

while inputs:
    # the lists get populated  with socket objects on select call
    readable, writable, exceptional = select.select(inputs, outputs, inputs)
    # select call could unblock if ANY ONE OF THE LISTS is non-empty. So, ...
    #... (1)there might be a connection request/data coming in...
    #... (2)or, something to be sent over...
    #... (3)or, an exception
    for s in readable:
        # s is a socket object
        if s is server:
            # for new connections
            connection, client_address = s.accept()
            connection.setblocking(0)
            inputs.append(connection)
            print("Received connection request from: ",client_address)
            # creating a message queue for each connection
            message_queues[connection] = queue.Queue()
        else:
            # if some data has been received
            data = s.recv(1024)
            if data:
                print("data received: ",data)
                # sending back ACK
                # NOTE: data is in bytes - it has to be sent over the network that way...
                #... & gets received as bytes as well (notice b" " - type message on client side)
                data = ("ACK - data received: "+str(data)).encode()
                message_queues[s].put(data)

                # add s as a connection waiting to send messages
                if s not in outputs:
                    outputs.append(s)
            # else:
            #     # TODO: why is this condition needed? - socket error?
            #     # if no data is readable
            #     if s in outputs:
            #         outputs.remove(s)
            #     inputs.remove(s)
            #     s.close()
            #     del message_queues[s]

    for s in writable:
        # If something has to be sent - send it. Else, remove connection from output queue
        if not message_queues[s].empty():
            # if some item is present - send it
            next_msg = message_queues[s].get()
            s.send(next_msg)
            # TODO : is if-else block needed?            
        else:
            # indicate that server has nothing to send
            outputs.remove(s)

    for s in exceptional:
        # remove this connection and all its existences
        inputs.remove(s)
        if s in outputs:
            outputs.remove(s)
        s.close()
        del message_queues[s]