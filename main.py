import sys
import os
import logging
import math
import datetime
from typing import Literal
from ast import literal_eval
import pandas as pd


def count_hashtags(row):
    one_tweet = row['tweet']
    return one_tweet.count("#")


def count_atSign(row):
    one_tweet = row['tweet']
    return one_tweet.count("@")


def clean_sp(T):
    T = T.split()
    T_new = [x for x in T if not x.startswith(
        "@") if not x.startswith("#") if not x.startswith("RT")]
    return ' '.join(T_new)


def main():
    # logging path and file name
    run_time = datetime.datetime.utcnow().strftime("%Y%m%d")
    logfile = 'log/log_main_error_{}.log'.format(run_time)
    logging.basicConfig(filename=logfile, filemode='a',
                        format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    # General config for CSV metadata
    tweets_csv_metadata = [
        {
            "file_name": "GamerGate_20210822",
            "hashtag": "GamerGate"
        },
        {
            "file_name": "GamerGate_20210831",
            "hashtag": "GamerGate"
        },
        {
            "file_name": "GamerGate_20210902",
            "hashtag": "GamerGate"
        },
        {
            "file_name": "GamerGate_20210911",
            "hashtag": "GamerGate"
        },
        {
            "file_name": "GamerGate_20210922",
            "hashtag": "GamerGate"
        },
        {
            "file_name": "Gamergaters_20210823",
            "hashtag": "Gamergaters"
        },
        {
            "file_name": "Gamergaters_20210903",
            "hashtag": "Gamergaters"
        },
        {
            "file_name": "Gamergaters_20210912",
            "hashtag": "Gamergaters"
        },
        {
            "file_name": "Gamergaters_20210923",
            "hashtag": "Gamergaters"
        },
        {
            "file_name": "gamergirl_20210823",
            "hashtag": "gamergirl"
        },
        {
            "file_name": "gamergirl_20210904",
            "hashtag": "gamergirl"
        },
        {
            "file_name": "gamergirl_20210912",
            "hashtag": "gamergirl"
        },
        {
            "file_name": "gamergirl_20210923",
            "hashtag": "gamergirl"
        },
        {
            "file_name": "notyourshield_20210823",
            "hashtag": "notyourshield"
        },
        {
            "file_name": "notyourshield_20210903",
            "hashtag": "notyourshield"
        },
        {
            "file_name": "notyourshield_20210904",
            "hashtag": "notyourshield"
        },
        {
            "file_name": "notyourshield_20210912",
            "hashtag": "notyourshield"
        },
        {
            "file_name": "notyourshield_20210923",
            "hashtag": "notyourshield"
        }
    ]

    # Base dataframe for all tweets
    tweet_metadata = {
        'file_id': [],
        'tweet': [],
        'tweet_id': [],
        'tweet_link': [],
        'user': [],
        'followers_count': [],
        'friends_count': [],
        'listed_count': [],
        'created_at': [],
        'favourites_count': [],
        'date': [],
        'place': [],
        'coordinateS': [],
        'lang': [],
        'source': [],
        'likes': [],
        'retweets': [],
        'favorite_count_tweets': [],
        'in_reply_to_status_id_str': [],
        'has_link': [],
        'number_of_hashtags': [],
        'hashtag_used': []

    }
    df_all_tweets = pd.DataFrame(tweet_metadata)
    logging.info(df_all_tweets.dtypes)
    # Solves problem that stores tweet id as float
    df_all_tweets['tweet_id'] = df_all_tweets['tweet_id'].astype('int')
    df_all_tweets['in_reply_to_status_id_str'] = df_all_tweets['in_reply_to_status_id_str'].astype(
        'int')
    logging.info(df_all_tweets.dtypes)
    # General for to iterate all CSVs files
    for tweet_csv_file in tweets_csv_metadata:
        # Open CSV into dataframe
        df = pd.read_csv('tweets/'+tweet_csv_file['file_name']+'.csv', converters={
                         "tweet": lambda x: literal_eval(x).decode("utf-8")})
        # Add new column on dataframe to check if has or not a link
        df['has_link'] = df['tweet'].str.contains("https://t.co")
        # Add new column on dataframe that specify hashtag used
        df['hashtag_used'] = tweet_csv_file['hashtag']

        df["number_of_hashtags"] = df.apply(count_hashtags, axis=1)
        logging.debug(df)

        df["clean_text"] = df['tweet'].str.replace(
            r"(b')|https\S+|[^\w+\s*',@*]|_", "")
        logging.debug(df)

        df["number_of_atSign"] = df.apply(count_atSign, axis=1)
        logging.debug(df)

        df["atSign_used"] = df['tweet'].str.contains('@', regex=False)
        logging.debug(df)

        df["clean_text2"] = df['tweet'].str.replace(
            r"(b')|https\S+|[^\w+\s*',@#*]", "").apply(clean_sp)
        logging.debug(df)

        df["at_least_one_word_is_uppercase"] = df['clean_text2'].str.contains(
            r"\b[A-Z]+(?:\s+[A-Z]+)*\b", regex=True)
        logging.debug(df)

        df["all_tweet_uppercase"] = df['clean_text2'].str.isupper()
        logging.debug(df)
        df["in_reply_to_status_id_str"].fillna(0, inplace=True)
        # Remove tweets if they have the same id
        no_duplicates = df.drop_duplicates(subset=['tweet_id'])
        # Remove tweets if they have the same text
        no_duplicates = no_duplicates.drop_duplicates(subset=['tweet'])
        logging.debug(no_duplicates)
        # Combine all tweets in one dataframe
        df_all_tweets = pd.concat(
            [df_all_tweets, no_duplicates], axis=0, ignore_index=True)
    logging.debug(df_all_tweets)
    # Add link to tweet
    df_all_tweets['tweet_link'] = df_all_tweets.agg(
        'https://twitter.com/anyuser/status/{0[tweet_id]}'.format, axis=1)
    # Remove tweets if they have the same id
    final_no_duplicates = df_all_tweets.drop_duplicates(subset=['tweet_id'])
    logging.info(final_no_duplicates)
    unique_users = final_no_duplicates.drop_duplicates(subset=['user'])
    index = unique_users.index
    number_of_rows = len(index)
    logging.info('Numero de perfiles - {}'.format(number_of_rows))

    df_all_en_tweets = final_no_duplicates.loc[final_no_duplicates['lang'] == "en"]

    # Save all EN tweets on csv
    df_all_en_tweets.to_csv('cleanText-en.csv')
    index = df_all_en_tweets.index
    number_of_rows = len(index)
    logging.info('Numero de tweets en ingles - {}'.format(number_of_rows))
    logging.info(df_all_en_tweets.dtypes)

    df_all_es_tweets = final_no_duplicates.loc[final_no_duplicates['lang'] == "es"]
    index = df_all_es_tweets.index
    number_of_rows = len(index)
    logging.info('Numero de tweets en espanol - {}'.format(number_of_rows))
    uniqueValues = df_all_en_tweets['source'].unique()
    countUniqueValues = df_all_en_tweets['source'].nunique()
    print('Unique elements in column "source" ')
    print(uniqueValues)
    print(countUniqueValues)


if __name__ == '__main__':
    main()
