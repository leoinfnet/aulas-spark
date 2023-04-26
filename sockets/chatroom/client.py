import socket 
import select 
import sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = 'localhost'
port = 50002
server.connect((host,port))
while True:
    sockets_list = [sys.stdin, server]
    read_sockets,write_socket, error_socket = select.select(sockets_list,[],[])
    for socket in read_sockets:
        if socket == server:
            message = socket.recv(2048)
            print(message)
        else:
            message = sys.stdin.readline()
            server.send(message.encode())
            sys.stdout.write("<VOCE>")
            sys.stdout.write(message)
            sys.stdout.flush()
server.close()


