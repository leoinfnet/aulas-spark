import socket 
from _thread import *
dispositivos_permitidos = ["DDD-HGHG-132131"]
def on_new_client(clientsocket, addr):
    msg = clientsocket.recv(1024).decode()
    print(address, ' >> ', msg)
    if msg in dispositivos_permitidos:
        print(address , " ==> Conectado")
        clientsocket.send("Conectado".encode())
        while True:
            temperatura = clientsocket.recv(1024).decode()
            print(f' {address} -> {temperatura}C')
    clientsocket.close()

if __name__ == '__main__':
    hostname = 'localhost'
    port = 50001
    server_socket = socket.socket()
    server_socket.bind((hostname,port))
    print("="*50)
    print(f'Server ouvindo na porta {port}')
    print("="*50)
    server_socket.listen(20)
    while True:
        conn, address = server_socket.accept()
        start_new_thread(on_new_client,(conn,address))
    server_socket.close()
