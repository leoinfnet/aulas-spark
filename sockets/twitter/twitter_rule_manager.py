import tweepy
bearer = 'XXXXXXXXXXXXXXXXXXXXXXXXX' 

class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        tweet_text = tweet.text
        print(tweet_text)
        print("="*50)

printer = GetTweets(bearer)
print(printer.get_rules())
#printer.add_rules(tweepy.StreamRule("flamengo"))
printer.delete_rules(['1651614411843330048','1651379019844075521'])
print(printer.get_rules())

#id='1651614411843330048'
#id='1651379019844075521'
printer.filter()
#printer.sample()

