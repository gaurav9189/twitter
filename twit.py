import json
from kafka import SimpleProducer, KafkaClient
import tweepy
#import configparser

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.

class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("172.16.1.54:6667")
        self.producer = SimpleProducer(client, async = True,
                          batch_send_every_n = 1000,
                          batch_send_every_t = 10)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        msg =  status.text.encode('utf-8')
        try:
            self.producer.send_messages(b'company', msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

if __name__ == '__main__':

    # Read the credententials from 'twitter-app-credentials.txt' file
   # config = configparser.ConfigParser()
   # config.read('twitter-app-credentials.txt')
   # consumer_key = config['DEFAULT']['consumerKey']
   # consumer_secret = config['DEFAULT']['consumerSecret']
   # access_key = config['DEFAULT']['accessToken']
   # access_secret = config['DEFAULT']['accessTokenSecret']

    access_token = "997956757271076864-KeYwvt94uKhU3AQeYc8qtaZgfcSNszM"
    access_token_secret =  "iRpJRyl4P2EvtZX0syLGF4jWCGJPZe2IFstWDS3F1Xxj0"
    consumer_key =  "7A0ND0ulEnNmkRe9eDUNRhYyq"
    consumer_secret =  "4xXx1PsikpoKd1ylqv6a8YkNVyOKFvEf4nxh8p4dNWEclucMw7"

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))

    #Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['google', 'facebook', 'amazon', 'microsoft', 'trump', 'putin', 'modi'], languages = ['en'])
    stream.filter(track = ['denmark', 'australia', 'germany', 'china', 'pakistan', 'india', 'usa', 'america'], languages = ['en'])
    #stream.filter(track = ['shruti', 'gaurav'], languages = ['en'])
    #stream.filter(track = ['gaurav', 'shruti', 'Gaurav', 'Shruti'])
    #stream.filter(locations=[-180,-90,180,90], languages = ['en'])
    #stream.filter(languages = ['en'])

