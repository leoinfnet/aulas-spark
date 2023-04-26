import socket
import random
import time
host = '127.0.0.1'
port = 50001

client_socket = socket.socket()
client_socket.connect((host,port))
senha = "DDD-HGHG-132131"
client_socket.send(senha.encode())
resposta = client_socket.recv(1024).decode()
if resposta == 'Conectado':
    print("Conectado no host")
    print("="*50)
    for i in range(1,1000):
        temperatura = random.randint(1,100)
        client_socket.send(str(temperatura).encode())
        time.sleep(3)
else:
    client_socket.close()
client_socket.close()
