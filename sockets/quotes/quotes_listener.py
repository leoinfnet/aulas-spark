from quoters import Quote
import socket 

def get_quote_from_twitter():
    return Quote.print_series_quote(True)

host = 'localhost'
port = 50001
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host,port))
s.listen(5)

try:
    while True:
        conn, addr = s.accept()
        for i in range(1000):
            quote = get_quote_from_twitter()
            print(quote)
            conn.send(bytes("{}\n".format(quote), "utf-8"))
finally:
    s.close()

