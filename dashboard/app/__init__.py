# # Import flask and template operators
# from flask import Flask, render_template
#
# app = Flask(__name__)
#
# # Configurations
# app.config.from_object('config')
#
import plotly
import json
import datetime
import pandas as pd
import plotly.express as px
from flask import Flask, render_template
from src.archive.data_managing.preprocessing import hourly_pipe, daily_pipe
from data_managing.db_handler import retrieve_data

app = Flask(__name__, template_folder='templates')

query_clean_tweets = """
SELECT tweet_created, cleaned_text, vader_compound, text_blob_polarity, text_blob_subjectivity
FROM preprocessed_tweets_info
LEFT OUTER JOIN raw_tweets_info rti on preprocessed_tweets_info.tweet_id = rti.tweet_id
"""
query_btc_hourly = """
SELECT input_datetime, high_price, low_price, open_price, close_price, volumeto, volumefrom FROM btc_hourly_info
"""
query_btc_daily = """
SELECT input_date, open_price, high_price, low_price, close_price, adj_close_price, volume FROM btc_daily_info
"""


def get_tweets(query):
    return pd.DataFrame(retrieve_data(query=query),
                        columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity', 'tb_subjectivity'])


def get_hourly_btc(query):
    return pd.DataFrame(retrieve_data(query),
                        columns=['input_datetime', 'high_price', 'low_price', 'open_price', 'close_price', 'volumeto',
                                 'volumefrom'])


# def get_daily_btc(query):
#     return pd.DataFrame(retrieve_all_data(query),
#                         columns=['input_date', 'open_price', 'high_price', 'low_price', 'close_price',
#                         'adj_close_price', 'volume'])


def plot_time_series(title, data, y=None):
    y = y if y else ['close_norm', 'avg_vader_compound_norm', 'avg_tb_subjectivity_norm', 'avg_tb_polarity_norm']
    fig = px.line(data, x=data.index, y=y, title=title)
    # fig.show()
    return fig


def plot_heat_map(title, data, columns=None):
    columns = columns if columns else ['close_price', 'open_price', 'high_price', 'low_price',
                                       'avg_vader_compound', 'avg_tb_subjectivity', 'avg_tb_polarity']
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr, title=title)
    # fig.show()
    return fig


def plot_tweet_count(title, data):
    fig = px.bar(data, x=data.index, y='tweet_count', title=title)
    # fig.show()
    return fig


@app.route('/hourly/')
def hourly():
    tweets_df = get_tweets(query_clean_tweets)
    btc_hourly = get_hourly_btc(query_btc_hourly)
    hourly_df = hourly_pipe(tweets_df, btc_hourly)
    # Plots
    # hourly_df = hourly_df.loc[hourly_df.index >= datetime.datetime(2022, 6, 5)]
    ts_fig = plot_time_series('Hourly Sentiment vs BTC Close', hourly_df)
    title2 = 'Daily Correlation'
    hm_fig = plot_heat_map(title2, hourly_df)
    title3 = 'Tweet Count Hourly'
    count_fig = plot_tweet_count(title3, hourly_df)

    timeseries = json.dumps(ts_fig, cls=plotly.utils.PlotlyJSONEncoder)
    heatmap = json.dumps(hm_fig, cls=plotly.utils.PlotlyJSONEncoder)
    count = json.dumps(count_fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('/hourly.html', timeseries=timeseries, heatmap=heatmap, count=count)


# @app.route('/daily/')
# def daily():
#     tweets_df = get_tweets(query_clean_tweets)
#     btc_daily = get_daily_btc(query_btc_daily)
#     tweet_btc_daily = daily_pipe(tweets_df, btc_daily)
#     # tweet_btc_daily = tweet_btc_daily.loc[tweet_btc_daily.index >= datetime.datetime(2022, 6, 5)]
#
#     # Figures
#     title = 'Timeseries, Daily'
#     ts_fig = plot_time_series(title, tweet_btc_daily)
#     title_2 = 'Daily Correlation'
#     hm_fig = plot_heat_map(title_2, tweet_btc_daily)
#     title_3 = 'Tweet Count Daily'
#     count_fig = plot_tweet_count(title_3, tweet_btc_daily)
#
#     timeseries = json.dumps(ts_fig, cls=plotly.utils.PlotlyJSONEncoder)
#     heatmap = json.dumps(hm_fig, cls=plotly.utils.PlotlyJSONEncoder)
#     count = json.dumps(count_fig, cls=plotly.utils.PlotlyJSONEncoder)
#     return render_template('/daily.html', timeseries=timeseries, heatmap=heatmap, count=count)


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5678)
