import socket
def init_client():
    try:
        port = 50001
        host = 'localhost'
        client_socket = socket.socket()
        client_socket.connect((host,port))
        server_message = client_socket.recv(1024)
        print(server_message.decode())
        while True:
            data = client_socket.recv(1024)
            if(not data): 
                break
            print(data.decode())
            print("="*50)
        client_socket.close()
    except socket.error as e:
        print("Socket error %s" %str(e))
    except Exception as e:
        print("Outro error %s" %str(e))
    finally:
        client_socket.close()
if __name__ == '__main__':
    init_client()