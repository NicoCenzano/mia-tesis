import sys
import os
import logging
import math
import datetime
import pandas as pd


def count_hashtags(row):
    one_tweet = row['tweet']
    return one_tweet.count("#")


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

    # General for to iterate all CSVs files
    for tweet_csv_file in tweets_csv_metadata:
        # Open CSV into dataframe
        df = pd.read_csv('tweets/'+tweet_csv_file['file_name']+'.csv')
        # Add new column on dataframe to check if has or not a link
        df['has_link'] = df['tweet'].str.contains("https://t.co")

        df["number_of_hashtags"] = df.apply(count_hashtags, axis=1)
        logging.debug(df)
        # Remove tweets if they have the same id
        no_duplicates = df.drop_duplicates(subset=['tweet_id'])
        # Remove tweets if they have the same text
        no_duplicates = no_duplicates.drop_duplicates(subset=['tweet'])
        logging.debug(no_duplicates)
        # Combine all tweets in one dataframe
        df_all_tweets = pd.concat(
            [df_all_tweets, no_duplicates], axis=0, ignore_index=True)
    logging.debug(df_all_tweets)
    # Remove tweets if they have the same id
    final_no_duplicates = df_all_tweets.drop_duplicates(subset=['tweet_id'])
    logging.info(final_no_duplicates)
    unique_users = final_no_duplicates.drop_duplicates(subset=['user'])
    index = unique_users.index
    number_of_rows = len(index)
    logging.info('Número de perfiles - {}'.format(number_of_rows))

    df_all_en_tweets = final_no_duplicates.loc[final_no_duplicates['lang'] == "en"]
    index = df_all_en_tweets.index
    number_of_rows = len(index)
    logging.info('Número de tweets en inglés - {}'.format(number_of_rows))

    df_all_es_tweets = final_no_duplicates.loc[final_no_duplicates['lang'] == "es"]
    index = df_all_es_tweets.index
    number_of_rows = len(index)
    logging.info('Número de tweets en español - {}'.format(number_of_rows))


if __name__ == '__main__':
    main()
