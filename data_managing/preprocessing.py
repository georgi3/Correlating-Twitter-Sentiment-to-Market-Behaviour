import re
import nltk
import emoji
import string
import contractions
import pandas as pd
from sklearn.preprocessing import FunctionTransformer
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.pipeline import Pipeline

TEXT = 'text'
SLANG = ['REKT', 'WAGMI', 'NGMI', 'HODL', 'BEAR', 'BEARISH', 'BULL', 'BULLISH', 'SHITCOIN', 'LFG', 'BLUECHIP', 'GG']
slang_patterns = re.compile(r'|'.join(SLANG), flags=re.IGNORECASE)
GOOD_CHARS = re.escape(string.printable + ''.join(emoji.EMOJI_DATA.keys()))
# nltk.download('vader_lexicon')


# TEXT PREPROCESSING FUNCTIONS
def removing_common_patterns(dataframe):
    """Removes common patterns from text"""
    column = TEXT

    def patterns(row):
        row = str(row)
        row = row.replace('RT ', '').replace('JUST IN ', ' ').replace('JUST IN: ', ' ').replace('BREAKING: ', ' '). \
            replace('BREAKING ', ' ').replace('NEW: ', ' ').replace('NEW ', ' ').replace('ICYMI: ', ' '). \
            replace('ICYMI ', ' ').replace('TOMORROW: ', ' ').replace('TOMORROW ', ' ').replace('COMING UP: ', ' '). \
            replace('COMING UP ', ' ').replace('LIVE: ', ' ').replace('LIVE ', ' ').replace('#', ' ').replace('’', "'")

        row = re.sub(r'http\S+', ' ', row)                      # removing links
        row = re.sub(r"@\S+", ' ', row)                         # removing mentions
        row = re.sub(r'|'.join(string.whitespace), ' ', row)    # removing whitespaces chars
        row = re.sub(fr'[^{GOOD_CHARS}]', ' ', row)             # removing non ascii and non emoji chars
        row = re.sub(r'&[A-Za-z\d#]+;', ' ', row)               # removing html character reference
        row = ' '.join(emoji.get_emoji_regexp().split(row))     # create spaces between emojis
        row = re.sub(r'\s+', ' ', row)                          # removing extra spaces
        row = re.sub(r'^\s', '', row)                           # removing leading spaces
        return row

    dataframe[column] = dataframe[column].apply(patterns)
    return dataframe


def drop_spam_filter_1(dataframe):
    """This function works a first spam filter."""
    df = dataframe.copy(deep=True)
    column = TEXT
    df = df[~df[column].isin(['', ' '])]                                                        # dropping empty strings
    df = df[~df[column].isin(df[column].value_counts().loc[lambda count: count > 2].index)]     # dropping duplicates >2
    # duplicates that occur > 2
    pattern_1 = re.compile(r'(^| )gm($| |!+|\.)', flags=re.IGNORECASE)
    pattern_2 = re.compile(r'(^| )Thank me later($| |!+|\.)', flags=re.IGNORECASE)
    pattern_3 = re.compile(r' FREE')
    pattern_4 = re.compile(r'milkywaydefi|taxi|shib|santafloki|luffy|check out|play to earn|Bluebit',
                           flags=re.IGNORECASE)
    pattern_5 = re.compile(r'DM for more premium', flags=re.IGNORECASE)
    pattern_6 = re.compile(r'Contact me via the link below', flags=re.IGNORECASE)
    pattern_7 = re.compile(r'I won’t be replying everyone on comment section', flags=re.IGNORECASE)
    pattern_8 = re.compile(r'The new trend 2022', flags=re.IGNORECASE)
    pattern_9 = re.compile(r'Opportunity for early investors', flags=re.IGNORECASE)
    pattern_10 = re.compile(r'hit me up on telegram', flags=re.IGNORECASE)

    # dropping tweets with common spam messages
    df = df[~((df[column].str.match(pattern_1)) & (df[column].str.len() < 30))]     # dropping first pattern
    df = df[~(df[column].str.match(pattern_2))]                                     # dropping second pattern
    df = df[~(df[column].str.contains(pattern_3, regex=True))]                      # dropping third pattern
    df = df[~(df[column].str.contains(pattern_4, regex=True))]
    df = df[~(df[column].str.contains(pattern_5, regex=True))]
    df = df[~(df[column].str.contains(pattern_6, regex=True))]
    df = df[~(df[column].str.contains(pattern_7, regex=True))]
    df = df[~(df[column].str.contains(pattern_8, regex=True))]
    df = df[~(df[column].str.contains(pattern_9, regex=True))]
    df = df[~(df[column].str.contains(pattern_10, regex=True))]

    short_df = df.loc[df[column].str.len() <= 20]                                   # tweets where char count <= 20

    non_adj_adv = \
        short_df[~short_df[column].apply(           # saving indices that we want to drop of non adjectives/adverbs
            lambda row: str(nltk.pos_tag(nltk.word_tokenize(row)))).str.contains(r"JJ|RB", regex=True)].index.tolist()

    df.drop(non_adj_adv, axis=0, inplace=True)      # dropping tweets without adjectives/adverbs and char count <= 20
    return df


def prepend_to_short_tweets(dataframe):
    """Functions finds short tweets (<20 chars), if it's a reply to a tweet, it prepends original tweet to the reply.
    Otherwise, it drops the tweet, as well as those tweets which parent tweet was not found, and those tweets that
    are still <20 chars after prepending. """
    column = TEXT
    temp_df = dataframe.loc[dataframe[column].str.len() <= 20, [column, 'conversation_id']]  # temporary df with short
    temp_df['index'] = temp_df.index  # creating column with their original index, to be used later

    df = pd.merge(dataframe[['tweet_id', column]], temp_df, how='right', left_on='tweet_id',
                  right_on='conversation_id')                           # merging

    df['prepended'] = df[f'{column}_x'] + '. ' + df[f'{column}_y']      # concatenating original tweets to comments
    to_drop = df.loc[(df[f'{column}_y'] == df[f'{column}_x']) | (df[f'{column}_x'].isna()) | (
            df['prepended'].str.len() < 20), 'index'].values.tolist()   # filtering garbage
    dataframe.loc[df['index'], column] = df[
        'prepended'].values.tolist()                                    # assigning concatenated tweets back to main df
    dataframe.drop(to_drop, axis=0, inplace=True)                       # dropping garbage
    return dataframe


def demojize(dataframe):
    """Demojizes emojis"""
    column = TEXT

    def demojize_row(row):
        row = str(row)
        row = emoji.demojize(row)
        return row

    dataframe[column] = dataframe[column].apply(demojize_row)
    return dataframe


def drop_spam_filter_2(dataframe):
    """This function works a first spam filter."""
    df = dataframe.copy(deep=True)
    column = TEXT
    df = df.loc[
        ~(df[column].str.len() > 280)]       # drop tweets with char length > 280, because most of them tend to be spams
    df = df.loc[~((df[column].str.len() > 28) & (df[column].str.split().map(
        lambda words: len(words)) <= 3))]    # (> 28 chars) & (<= 3 words) per tweet ==> drop
    df = df.loc[~(df[column].str.split().map(
        lambda words: sum(len(word) for word in words) / len(words)) > 14)]     # average len(word) > 14 ==> drop
    return df


def clean_contractions(row):
    """Cleans up contractions"""
    row = contractions.fix(row, slang=False)
    row = re.sub(r" he s ", " he's ", row, flags=re.IGNORECASE)
    row = re.sub(r" she s ", " she's ", row, flags=re.IGNORECASE)
    row = re.sub(r" couldn t ", " couldn't ", row, flags=re.IGNORECASE)
    row = re.sub(r" ll ", ' will ', row, flags=re.IGNORECASE)  # removes misspelled contractions
    row = re.sub(r" re ", ' are ', row, flags=re.IGNORECASE)
    row = re.sub(r" m ", ' am ', row, flags=re.IGNORECASE)
    return row


def clean_slang(row):
    """Cleans up common crypto slang"""
    # SLANG # https://twitter.com/galus_titanium/status/1483370382845435907
    # SLANG # https://www.nasdaq.com/articles/decoding-crypto%3A-top-25-crypto-terms-you-need-to-know-2021-09-13
    # important
    # row = re.sub(r"(^| ):rocket:($| )", ' I want positive movement ', row)              # rocket emoji       # BIASED
    # row = re.sub(r"(^| )moon($| )", ' I want the positive movement ', row, flags=re.IGNORECASE)              # BIASED
    row = re.sub(r"(^| )WAGMI($| )", ' we are going to make it ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )NGMI($| )", ' never going to make it ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )FOMO($| )", ' fear of missing out ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Rekt($| |!+|\.|t+)", ' wrecked ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )FUD($| |!+|\.)", ' fear, uncertainty, doubt ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )HODL($| |!+|\.|ing|er)", ' I am losing money, hold on for dear life ',
                 row, flags=re.IGNORECASE)                                                                     # BIASED
    row = re.sub(r"(^| )bear($| |!+|\.)", ' negative movement ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )bearish($| |!+|\.)", ' negative movement ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )bull($| |!+|\.)", ' positive movement ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )bullish($| |!+|\.)", ' positive movement ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Bullrun($| |!+|\.)", ' positive movement ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )shitcoin($| |!+|\.)", ' bad investment ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )shitcoins($| |!+|\.)", ' bad investments ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )LFG($| |!+|\.)", ' lets go, good investment  ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Bluechip($| )", ' high value ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Rugged($| |!+|\.)", ' scammed ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Rug Pull($| |!+|\.)", ' fake projects ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )GG($| |!+|\.)", ' smart investment ', row, flags=re.IGNORECASE)
    # other
    row = re.sub(r"(^| )u($| )", ' you ', row, flags=re.IGNORECASE)
    row = re.sub(' n ', ' and ', row)
    row = re.sub(' w ', ' with ', row)
    row = re.sub(r"(^| )smh($| |!+|h+)", ' shaking my head ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )tbh( |h+)", ' to be honest ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )imo($| )", ' in my opinion ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )imho($| )", ' in my honest opinion ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Lambo($| |!+)", ' get rich by trading crypto ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )DYOR($| )", ' do your own research ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )WL($| )", ' whitelist ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Frens($| |s+)", ' cryptocurrency friends ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Anon($| )", ' cryptocurrency anonymous friends ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Whale($| |s+)", ' big companies ', row, flags=re.IGNORECASE)  # Someone who owns a lot of crypto
    row = re.sub(r"(^| )DCA($| )", ' dollar-cost averaging  ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Paper Hands($| )", ' short term holders ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Diamond Hands($| )", ' long term holders ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )DeFi($| )", ' Decentralized Finance ', row, flags=re.IGNORECASE)
    row = re.sub(r"(^| )Working at McDonald's($| |!+|\.)", ' I am broke ', row, flags=re.IGNORECASE)            # meme
    row = re.sub(r"(^| )Can Devs do something($| |!+|\.)", ' bad investment ', row, flags=re.IGNORECASE)
    return row


def clean_money(row):
    """Cleans up money relates context"""
    row = row.replace('Bitcoin Bitcoin', ' Bitcoin ')           # prevent from "#BTC #Bitcoin"
    row = re.sub(r'^\. ', '', row)                              # get rid of the dots in the beginning
    row = re.sub('(?<=\d) (?=\d)', '', row)                     # attaching neighbouring numbers
    row = re.sub('(?<=\d),(?=\d)', '', row)                     # attaching neighbouring numbers
    row = re.sub(r'\$', ' dollar ', row)
    row = re.sub(r'€', ' euro ', row)
    row = re.sub(r'(^| )bitcoin($| )', ' Bitcoin ', row)
    row = re.sub(r'(^| )BTC($| )', ' Bitcoin ', row, flags=re.IGNORECASE)
    row = re.sub(r'(^| )crypto currency($| )', ' cryptocurrency ', row, flags=re.IGNORECASE)
    row = re.sub(r'(^| )crypto currencies($| |\.)', ' cryptocurrency ', row, flags=re.IGNORECASE)
    row = re.sub(r'(^| )crypto($| )', ' cryptocurrency ', row, flags=re.IGNORECASE)
    row = re.sub(r'(^| )eth($| )', ' Ethereum ', row, flags=re.IGNORECASE)
    row = re.sub(r'(^| )ethereum($| )', ' Ethereum ', row)
    return row


def clean_other(row):
    """Cleans up other text."""
    # USA
    row = re.sub(r'(^| )US($| )', ' USA ', row)
    row = re.sub(r'(^| )U\.S\.A($| )', ' USA ', row)
    row = re.sub(r'(^| )U\.S\.($| )', ' USA ', row)
    row = re.sub(r'(^| )u\.s\.($| )', ' USA ', row)
    row = re.sub(r'(^| )U\.S($| )', ' USA ', row)
    row = re.sub(r'(^| )u\.s($| )', ' USA ', row)
    row = re.sub(r'(^| )US($| )', ' USA ', row)

    # Other
    row = re.sub(r'^\. ', '', row)                      # get rid of the dots in the beginning
    row = re.sub(r' \. ', '. ', row)                    # map dot to their words
    return row


def clean_text(dataframe):
    """Cleans up different slang/lingo, etc."""
    column = TEXT

    def clean(row):
        row = str(row)
        row = clean_other(row)
        row = clean_contractions(row)
        row = clean_slang(row)
        row = clean_money(row)
        row = re.sub(r'^\. ', '', row)                   # get rid of the dots in the beginning
        row = re.sub(r'\s+', ' ', row)                   # get rid of extra spaces
        return row

    dataframe[column] = dataframe[column].apply(clean)
    return dataframe


def drop_spam_filter_3(dataframe):
    """Doubles checks for spams after all text transformations"""
    df = dataframe.copy(deep=True)
    column = TEXT
    df = df[~df[column].isin(df[column].value_counts().loc[lambda count: count > 2].index)]              # identical > 2
    df['txt-5'] = df[column].str[:-5]
    df = df.loc[~(df['txt-5'].isin(df['txt-5'].value_counts().
                                   loc[lambda count: count > 2].index))]                  # identical up to last 5 chars

    df['word_uniqueness_%'] = df.text.str.split().map(lambda row: round(len(set(row)) / len(row) * 100))
    df = df.loc[df['word_uniqueness_%'] > 53]                               # keep tweets if only % of unique words > 53
    return df


# OTHER
def to_datetime(dataframe: pd.DataFrame, columns: list):
    """Converts passed columns to datetime objects"""
    for column in columns:
        dataframe[column] = pd.to_datetime(dataframe[column])
    return dataframe


def vader_sentiment(dataframe):
    column = TEXT
    vd = SentimentIntensityAnalyzer()

    def transform(row):
        return dict(vd.polarity_scores(row)).get('compound')
    dataframe['vader_compound'] = dataframe[column].apply(transform)
    return dataframe


def text_blob_analysis(dataframe):
    column = TEXT

    def polarity(row):
        return TextBlob(row).polarity

    def subjectivity(row):
        return TextBlob(row).subjectivity
    dataframe['text_blob_sentiment'] = dataframe[column].apply(polarity)
    dataframe['text_blob_subjectivity'] = dataframe[column].apply(subjectivity)
    return dataframe


def columns_to_keep(dataframe):
    columns = ['tweet_id', 'text', 'vader_compound', 'text_blob_sentiment', 'text_blob_subjectivity']
    return dataframe[columns]


text_pipe = Pipeline(steps=[
    ('remove_common_patters', FunctionTransformer(func=removing_common_patterns)),
    ('spam_filter_1', FunctionTransformer(func=drop_spam_filter_1)),
    ('prepend_to_short_tweets', FunctionTransformer(func=prepend_to_short_tweets)),
    ('demojize', FunctionTransformer(func=demojize)),
    ('spam_filter_2', FunctionTransformer(func=drop_spam_filter_2)),
    ('clean_text', FunctionTransformer(func=clean_text)),
    ('spam_filter_3', FunctionTransformer(func=drop_spam_filter_3)),
    ('vader_sentiment', FunctionTransformer(func=vader_sentiment)),
    ('text_blob_analysis', FunctionTransformer(func=text_blob_analysis)),
    ('drop_useless_cols', FunctionTransformer(func=columns_to_keep)),
])
