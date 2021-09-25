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
    # Open CSV into dataframe
    df = pd.read_csv('tweets/gamergirl_20210823.csv')
    # Add new column on dataframe to check if has or not a link
    df['has_link'] = df['tweet'].str.contains("https://t.co")

    df["number_of_hashtags"] = df.apply(count_hashtags, axis=1)
    logging.info(df)
    # Remove tweets if they have the same id
    no_duplicates = df.drop_duplicates(subset=['tweet_id'])
    logging.info(no_duplicates)


if __name__ == '__main__':
    main()
