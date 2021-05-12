################################################################################
#
#
###############################################################################
import threading
import itertools
import os
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

MAIN_BUFFER = []

selection = input("|| For Hashtag Mode type: 1  || For Username Mode type: 2 || ->")

header_list = ['created_at','id','text']

with open('metadata.csv', 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    metadata_reader = list(flatten(reader(read_obj)))
    print(metadata_reader)

    # Iterate over each row in the csv using reader object



mutex = threading.Lock()  # lock for the threads
# CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET, seed_user,

def Collector(api, user_from_list):
    flag_of_following = '0'
    global new_user

    local_usersList = []
    ThirdLayerUsers = []
    tweets_from_user = api.user_timeline(screen_name=user_from_list, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                         tweet_mode='extended')
    # here create CSV file and put metadata in it #
    #if (api.exists_friendship(userObj.id, tweets_from_user.user.id)):
     #   flag_of_following = '1'

    # here check friendship status
    #seed_user_followers =[]
    #for follower in api.followers(seed_user):
     #   seed_user_followers.append(follower.screen_name)
    #if (user_from_list in seed_user_followers):
     #   flag_of_following = '1'
    #else:
     #   flag_of_following = '2'


    with open("output/"+user_from_list+"_"+flag_of_following+"_U.csv", 'a', encoding="utf-8") as f_object:
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
            if tweets.lang == 'en' :
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
        if os.stat("output/"+user_from_list+"_"+flag_of_following+"_U.csv").st_size == 0:
            os.remove("output/"+user_from_list+"_"+flag_of_following+"_U.csv")



    for tweets in tweets_from_user:
        text = tweets.full_text
      #  print("Text: ",text) # test
        usernames = list(filter(lambda word: word[0]=='@', text.split()))
      #  print("Usernames: ",usernames) # test
        for user in usernames:
            local_usersList.append(clearUsername(user))
     #   print(local_usersList)
    ###### Now from Local User List get one by one the users and fetch tweets from their timeline
    for user in local_usersList:
        follow_flag = '0'
        try:
            users_tweet = api.user_timeline(screen_name=user, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                             tweet_mode='extended')
            #here we are checking the friendship at 3rd layer
            #user_from_list_followers = []
            #for follower in api.followers(user_from_list):
             #   user_from_list_followers.append(follower.screen_name)
            #if ((user in user_from_list_followers) and(user in seed_user_followers) ):
             #   follow_flag = '1'
            #else:
             #   follow_flag = '2'

            with open("output/" + user +"_"+follow_flag+ "_U_3rd.csv", 'a', encoding="utf-8") as f_object:
                writer_object = writer(f_object)
                for tweets in users_tweet:
                    if tweets.lang == 'en':
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
                f_object.close()
                if os.stat("output/" + user +"_"+follow_flag+ "_U_3rd.csv").st_size == 0:
                    os.remove("output/" + user +"_"+follow_flag+ "_U_3rd.csv")

            for tweets in users_tweet:
                text = tweets.full_text
              #  print("Text: ", text)  # test
                usernames = list(filter(lambda word: word[0] == '@', text.split()))
                for user in usernames:
                    ThirdLayerUsers.append(clearUsername(user))
              #  print("ThirdLayerList: ",ThirdLayerUsers)
                usersList.extend(ThirdLayerUsers)

        except Exception as e:
            print("Username Does Not exists")
            print(e)













def CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,seed_user, num):
    print("This is Thread: ", num)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
  #  print('searching tweets about the seed_user...')
    fetched_tweets = tweepy.Cursor(api.search, q=seed_user, count=50, result_type="recent").items(100)
   # print('search completed')
    #print(fetched_tweets)

    for tweets in fetched_tweets:
       # print('filtering users...')
        user = tweets.user
        if (user.statuses_count >= 600 and (user.screen_name in MAIN_BUFFER) == False ):
            #print('matching user found adding to the list...')
            addtoList(user.screen_name)
            MAIN_BUFFER.append(user.screen_name)

            #print(user.id)
    while usersList:


        mutex.acquire()
        username = usersList.pop(0)
        mutex.release()
        #print(user) # test
        Collector(api, username)


############ HASHTAG VERSION ######################################################################################
###################################################################################################################
#
###################################################################################################################
#
#
###################################################################################################################


def Hash_Collector(api, user_from_list, userObj):  # here flag_of_following represents using the same hashtag
    flag_of_following = '0'
    global new_user

    local_usersList = []
    ThirdLayerUsers = []
    tweets_from_user = api.user_timeline(screen_name=user_from_list, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                         tweet_mode='extended')
    # here create CSV file and put metadata in it #
    #if (api.exists_friendship(userObj.id, tweets_from_user.user.id)):
     #   flag_of_following = '1'

    # here check friendship status
    #seed_user_followers =[]
    #for follower in api.followers(seed_user):
     #   seed_user_followers.append(follower.screen_name)
    #if (user_from_list in seed_user_followers):
     #   flag_of_following = '1'
    #else:
     #   flag_of_following = '2'
    tweet_text_user = []
    for tweets in tweets_from_user:
        tweet_text_user.append(tweets.full_text)
        if hashtag in tweets.full_text:
            flag_of_following = '1'
            break


    with open("output/"+user_from_list+"_"+flag_of_following+"_U.csv", 'a', encoding="utf-8") as f_object:
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
            if tweets.lang == 'en' :
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
        if os.stat("output/"+user_from_list+"_"+flag_of_following+"_U.csv").st_size == 0:
            os.remove("output/"+user_from_list+"_"+flag_of_following+"_U.csv")



    for tweets in tweets_from_user:
        text = tweets.full_text
        #print("Text: ",text) # test
        usernames = list(filter(lambda word: word[0]=='@', text.split()))
        #print("Usernames: ",usernames) # test
        for user in usernames:
            local_usersList.append(clearUsername(user))
        print(local_usersList)
    ###### Now from Local User List get one by one the users and fetch tweets from their timeline
    for user in local_usersList:
        follow_flag = '0'
        try:
            users_tweet = api.user_timeline(screen_name=user, count=999, exclude_replies=True, include_rts=False,
                                         # scans maximum 100 tweets from an account
                                             tweet_mode='extended')
            #here we are checking the friendship at 3rd layer
            #user_from_list_followers = []
            #for follower in api.followers(user_from_list):
             #   user_from_list_followers.append(follower.screen_name)
            #if ((user in user_from_list_followers) and(user in seed_user_followers) ):
             #   follow_flag = '1'
            #else:
             #   follow_flag = '2'
            for tweets in users_tweet:
                    #tweet_text_user.append(tweets.full_text)
                    if (hashtag in tweets.full_text):
                        follow_flag = '1'
                        break


            with open("output/" + user +"_"+follow_flag+ "_U_3rd.csv", 'a', encoding="utf-8") as f_object:
                writer_object = writer(f_object)
                for tweets in users_tweet:
                    if tweets.lang == 'en':
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
                f_object.close()
                if os.stat("output/" + user +"_"+follow_flag+ "_U_3rd.csv").st_size == 0:
                    os.remove("output/" + user +"_"+follow_flag+ "_U_3rd.csv")

            for tweets in users_tweet:
                text = tweets.full_text
                #print("Text: ", text)  # test
                usernames = list(filter(lambda word: word[0] == '@', text.split()))
                for user in usernames:
                    ThirdLayerUsers.append(clearUsername(user))
                #print("ThirdLayerList: ",ThirdLayerUsers)
                usersList.extend(ThirdLayerUsers)

        except Exception as e:
            print("Username Does Not exists")
            print(e)













def Hash_CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,seed_user,num):
    print("This is Thread: ", num)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    #print('searching tweets about the seed_user...')
    fetched_tweets = tweepy.Cursor(api.search, q=seed_user, count=50, result_type="recent").items(100)
    #print('search completed')
    print(fetched_tweets)

    for tweets in fetched_tweets:
        #print('filtering users...')
        user = tweets.user
        if (user.statuses_count >= 600 and (user.screen_name in MAIN_BUFFER) == False ):
            #print('matching user found adding to the list...')
            addtoList(user.screen_name)
            MAIN_BUFFER.append(user.screen_name)

            #print(user.id)
    while usersList:
        #user = api.get_user(usersList[1])
        #print(user) # test
        mutex.acquire()
        username = usersList.pop(0)
        mutex.release()

        Hash_Collector(api, username)


if selection == '1':

    hashtag = input("Please type Hashtag (ex. #HelloWorld): ")
    threading.Thread(target=Hash_CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,hashtag,1)).start()
    threading.Thread(
        target=Hash_CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, hashtag,2)).start()
    threading.Thread(
        target=Hash_CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, hashtag,3)).start()
    #Hash_CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET,hashtag)
else:

    seed_user = input("Please type seed username (ex. @JohnnyTheBad): ")
    threading.Thread(
        target=CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, seed_user,1)).start()
    threading.Thread(
        target=CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, seed_user,2)).start()
    threading.Thread(
        target=CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, seed_user,3)).start()
    #CrawlerAction(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, seed_user)
