################################################################################
#
#
###############################################################################
import threading
import itertools

import re
from csv import *
import tweepy
from setuptools.namespaces import flatten

import FilterFunctions
from Functions import usersList, addtoList, popfromList
from FilterFunctions import *
import random
import time
from time import sleep
import datetime

config_file1 = open('config1.txt')
configs1 = config_file1.readlines()

config_file2 = open('config2.txt')
configs2 = config_file2.readlines()

config_file3 = open('config3.txt')
configs3 = config_file3.readlines()


CONSUMER_KEY = configs1[0].rstrip()
CONSUMER_SECRET = configs1[1].rstrip()

ACCESS_KEY = configs1[2].rstrip()
ACCESS_SECRET = configs1[3].rstrip()


seed_user = input("Please type seed username (ex. @JohnnyTheBad): ")
header_list = ['created_at','id','text']

with open('metadata.csv', 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    metadata_reader = list(flatten(reader(read_obj)))
    print(metadata_reader)

    # Iterate over each row in the csv using reader object



mutex = threading.Lock()  # lock for the threads


def Collector(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET, seed_user, user_from_list):
    global new_user
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth)
    local_usersList = []
    ThirdLayerUsers = []
    tweets_from_user = api.user_timeline(screen_name=user_from_list, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                         tweet_mode='extended')
    # here create CSV file and put metadata in it #
    with open(""+user_from_list+"_1.csv", 'a', encoding="utf-8") as f_object:
        writer_object = writer(f_object)
        First = metadata_reader[0]
        Secon = metadata_reader[1]
        Third = metadata_reader[2]
        Fourt = metadata_reader[3]
        Fifth = metadata_reader[4]
        Sixth = metadata_reader[5]
        Seven = metadata_reader[6]
        Eight = metadata_reader[7]
        for tweets in tweets_from_user:
            FirstObj = getattr(tweets,""+First+"")
            SeconObj = getattr(tweets,""+Secon+"")
            ThirdObj = getattr(tweets,""+Third+"")
            ThirdObj = str(ThirdObj)
            text = bytes(ThirdObj, 'utf-8').decode('utf-8', 'ignore')
            text = filterstring(text)
            FourtObj = getattr(tweets,""+Fourt+"")
            FifthObj = getattr(tweets,""+Fifth+"")
            SixthObj = getattr(tweets,""+Sixth+"")
            SevenObj = getattr(tweets,""+Seven+"")
            EightObj = getattr(tweets,""+Eight+"")
            writer_object.writerow([""+str(FirstObj)+"", ""+str(SeconObj)+"", ""+str(text)+"", ""+str(FourtObj)+"",""+str(FifthObj)+"",""+str(SixthObj)+"",""+str(SevenObj)+"",""+str(EightObj)+""])
        f_object.close()



    for tweets in tweets_from_user:
        text = tweets.full_text
        print("Text: ",text) # test
        usernames = list(filter(lambda word: word[0]=='@', text.split()))
        print("Usernames: ",usernames) # test
        for user in usernames:
            local_usersList.append(clearUsername(user))
        print(local_usersList)
    ###### Now from Local User List get one by one the users and fetch tweets from their timeline
    for user in local_usersList:
        try:
            users_tweet = api.user_timeline(screen_name=user, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                             tweet_mode='extended')

            with open("" + user + "_1_U_3rd.csv", 'a', encoding="utf-8") as f_object:
                writer_object = writer(f_object)
                for tweets in users_tweet:
                    FirstObj = getattr(tweets, "" + First + "")
                    SeconObj = getattr(tweets, "" + Secon + "")
                    ThirdObj = getattr(tweets, "" + Third + "")
                    ThirdObj = str(ThirdObj)
                    text = bytes(ThirdObj, 'utf-8').decode('utf-8', 'ignore')
                    text = filterstring(text)
                    FourtObj = getattr(tweets, "" + Fourt + "")
                    FifthObj = getattr(tweets, "" + Fifth + "")
                    SixthObj = getattr(tweets, "" + Sixth + "")
                    SevenObj = getattr(tweets, "" + Seven + "")
                    EightObj = getattr(tweets, "" + Eight + "")
                    writer_object.writerow(
                        ["" + str(FirstObj) + "", "" + str(SeconObj) + "", "" + str(text) + "", "" + str(FourtObj) + "",
                        "" + str(FifthObj) + "", "" + str(SixthObj) + "", "" + str(SevenObj) + "",
                        "" + str(EightObj) + ""])

            for tweets in users_tweet:
                text = tweets.full_text
                print("Text: ", text)  # test
                usernames = list(filter(lambda word: word[0] == '@', text.split()))
                for user in usernames:
                    ThirdLayerUsers.append(clearUsername(user))
                print("ThirdLayerList: ",ThirdLayerUsers)

        except Exception as e:
            print("Username Does Not exists")
            print(e)













def CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,seed_user):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth)
    fetched_tweets = tweepy.Cursor(api.search, q=seed_user, count=100, include_entities=True, result_type="recent").items()

    for tweets in fetched_tweets:
        user = tweets.user
        if (user.statuses_count >= 600):     # TODO add checker if it is in list OR not
            addtoList(user.screen_name)
            #print(user.id)
    #user = api.get_user(usersList[1])
    print(user) # test
    Collector(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, seed_user, user.screen_name)




CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,seed_user)