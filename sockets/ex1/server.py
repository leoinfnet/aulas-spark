import socket
import random
import time 
def init_server():
    port = 50001
    host = 'localhost'
    server_socket = socket.socket()
    server_socket.bind((host,port))
    print(f'Server ouvindo na porta {port}' )
    server_socket.listen(15)
    conn, address = server_socket.accept()
    print(f'Conexão aceita de {address}')
    hello_message = f'Olá from server!!!'
    conn.send(hello_message.encode())
    while True:
        numero = random.randint(1,500)
        print(f'Enviando numero {numero}')
        if(numero > 490):
            break
        conn.send(f'{str(numero)}'.encode())
        time.sleep(3)
    conn.close()

if __name__ == '__main__':
   init_server()