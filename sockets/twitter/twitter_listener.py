import tweepy
bearer = 'XXXXXXXXXXXXXXXXXXXXXXXXXX' 
import socket

port = 50001
host = "localhost"
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host,port))
print("Servidor iniciado")
server_socket.listen(100)
conn , addr = server_socket.accept()


class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        tweet_text = tweet.text
        print(tweet_text)
        print("="*50)
        conn.send(bytes("{}\n".format(tweet_text), "utf-8"))
printer = GetTweets(bearer)
printer.add_rules(tweepy.StreamRule("flamengo"))
printer.filter()
printer.sample()

