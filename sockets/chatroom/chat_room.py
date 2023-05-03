import socket
from _thread import *
list_of_clients = []
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

host = 'localhost'
port = 50002
server.bind((host,port))
server.listen(100)

def client_thread(conn, addr):
    msg_boas_vindas = "Bem vindo a essa sala X!"
    conn.send(msg_boas_vindas.encode())
    while True:
        try:
            message = conn.recv(2048).decode()
            if message:
                message_to_send = "<" + addr[1] + ">" + message 
                print(message_to_send)
                broadcast(message_to_send.encode(), conn)
            else: 
                remove(conn)
        except:
            continue

def broadcast(message, conn):
    for client in list_of_clients:
        if client != conn:
            try:
                client.send(message)
            except:
                client.close()
                remove(conn)
def remove(connection):
    if connection in list_of_clients:
        list_of_clients.remove(connection)

print("Iniciando servidor de bate-papo")
print(f' Na porta {port}')
while True:
    conn , address = server.accept()
    list_of_clients.append(conn)
    print(address , " Connected")
    start_new_thread(client_thread,(conn,address))