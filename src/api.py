import plotly
import json
import datetime
import pandas as pd
import plotly.express as px
from flask import Flask, render_template
from src.db_handler import retrieve_all_data
from src.preprocessing import hourly_pipe, daily_pipe
from src.data_manager import extract_tweets_hourly, extract_btc_hourly, extract_btc_daily
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, template_folder='templates')

query_clean_tweets = """
SELECT  TWEET_CREATED,
        CONVERSATION_ID,
        TWEET_ID,
        AUTHOR_ID,
        TWEET_TEXT,
        RETWEET_COUNT,
        REPLY_COUNT,
        LIKE_COUNT,
        QUOTE_COUNT,
        ACCOUNT_CREATED,
        ACCOUNT_ID,
        ACCOUNT_NAME,
        VERIFIED,
        FOLLOWER_COUNT,
        FOLLOWING_COUNT,
        TWEET_COUNT,
        LISTED_COUNT,
        WORD_UNIQUENESS,
        SENTIMENT
FROM preprocessed_tweets
"""
query_btc_hourly = """
SELECT time_stamp, high, low, open_, close_, volume_to, volume_from FROM btc_hourly
"""
query_btc_daily = """
SELECT date_, open_, high, low, close_, adj_close, volume FROM btc_daily
"""


def get_tweets(query):
    return pd.DataFrame(retrieve_all_data(query=query),
                        columns=['tweet_created', 'conversation_id', 'tweet_id', 'author_id',
                                 'text', 'retweet_count', 'reply_count', 'like_count', 'quote_count',
                                 'account_created', 'account_id', 'name', 'verified', 'follower_count',
                                 'following_count', 'tweet_count', 'listed_count', 'word_uniqueness_%',
                                 'sentiment'])


def get_hourly_btc(query):
    return pd.DataFrame(retrieve_all_data(query),
                        columns=['timestamp', 'high', 'low', 'open', 'close', 'volume_to', 'volume_from'])


def get_daily_btc(query):
    return pd.DataFrame(retrieve_all_data(query),
                        columns=['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])


def plot_time_series(title, data, y=None):
    y = y if y else ['close_norm', 'avg_sentiment']
    fig = px.line(data, x=data.index, y=y, title=title)
    return fig


def plot_heat_map(title, data, columns=None):
    columns = columns if columns else ['close', 'open', 'high', 'low', 'avg_sentiment']
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr, title=title)
    return fig


def plot_tweet_count(title, data):
    fig = px.bar(data, x=data.index, y='tweet_count', title=title)
    return fig


@app.route('/hourly/')
def hourly():
    tweets_df = get_tweets(query_clean_tweets)
    btc_hourly = get_hourly_btc(query_btc_hourly)
    hourly_df = hourly_pipe(tweets_df, btc_hourly)

    # Plots
    hourly_df = hourly_df.loc[hourly_df.index >= datetime.datetime(2022, 6, 5)]
    ts_fig = plot_time_series('Hourly Sentiment vs BTC Close', hourly_df)
    title2 = 'Daily Correlation'
    hm_fig = plot_heat_map(title2, hourly_df)
    title3 = 'Tweet Count Hourly'
    count_fig = plot_tweet_count(title3, hourly_df)

    timeseries = json.dumps(ts_fig, cls=plotly.utils.PlotlyJSONEncoder)
    heatmap = json.dumps(hm_fig, cls=plotly.utils.PlotlyJSONEncoder)
    count = json.dumps(count_fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('/hourly.html', timeseries=timeseries, heatmap=heatmap, count=count)


@app.route('/daily/')
def daily():
    tweets_df = get_tweets(query_clean_tweets)
    btc_daily = get_daily_btc(query_btc_daily)
    tweet_btc_daily = daily_pipe(tweets_df, btc_daily)
    # tweet_btc_daily = tweet_btc_daily.loc[tweet_btc_daily.index >= datetime.datetime(2022, 6, 5)]

    # Figures
    title = 'Timeseries, Daily'
    ts_fig = plot_time_series(title, tweet_btc_daily)
    title_2 = 'Daily Correlation'
    hm_fig = plot_heat_map(title_2, tweet_btc_daily)
    title_3 = 'Tweet Count Daily'
    count_fig = plot_tweet_count(title_3, tweet_btc_daily)

    timeseries = json.dumps(ts_fig, cls=plotly.utils.PlotlyJSONEncoder)
    heatmap = json.dumps(hm_fig, cls=plotly.utils.PlotlyJSONEncoder)
    count = json.dumps(count_fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('/daily.html', timeseries=timeseries, heatmap=heatmap, count=count)


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(extract_tweets_hourly(), 'interval', hours=1)
    scheduler.add_job(extract_btc_hourly(), 'interval', hours=1)
    scheduler.add_job(extract_btc_daily(), 'interval', hours=24)
    scheduler.start()
    app.run(debug=False, host='0.0.0.0', port=5678)
