import re
import nltk
import emoji
import string
import contractions
import pandas as pd
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline

SLANG = ['REKT', 'WAGMI', 'NGMI', 'HODL', 'BEAR', 'BEARISH', 'BULL', 'BULLISH', 'SHITCOIN', 'LFG', 'BLUECHIP', 'GG']
slang_patterns = re.compile(r'|'.join(SLANG), flags=re.IGNORECASE)
GOOD_CHARS = re.escape(string.printable + ''.join(emoji.EMOJI_DATA.keys()))


def removing_common_patterns(dataframe):
    """Removes common patterns from text"""
    column = 'text'

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
        row = row.replace(r'&[A-Za-z0-9#]+;', ' ')              # removing html character reference
        row = ' '.join(emoji.get_emoji_regexp().split(row))     # create spaces between emojis
        row = re.sub(r'\s+', ' ', row)                          # removing extra spaces
        row = re.sub(r'^\s', '', row)                           # removing leading spaces
        return row

    dataframe[column] = dataframe[column].apply(patterns)
    return dataframe


def drop_spam_filter_1(dataframe):
    """This function works a first spam filter."""
    df = dataframe.copy(deep=True)
    column = 'text'
    df = df[~df[column].isin(['', ' '])]                                                        # dropping empty strings
    df = df[~df[column].isin(df[column].value_counts().loc[lambda count: count > 2].index)]     # dropping text
    # duplicates that occur > 2
    pattern_1 = re.compile(r'(^| )gm($| |!+|\.)', flags=re.IGNORECASE)
    pattern_2 = re.compile(r'(^| )Thank me later($| |!+|\.)', flags=re.IGNORECASE)
    pattern_3 = re.compile(r'(^| )FREE($| |!+|\.)')
    df = df[~((df[column].str.match(pattern_1)) & (df[column].str.len() < 30))]                 # dropping tweets
    # with common spam messages
    df = df[~(df[column].str.match(pattern_2))]     # dropping second pattern
    df = df[~(df[column].str.match(pattern_3))]     # dropping third pattern
    short_df = df.loc[df[column].str.len() <= 20]   # tweets where char count <= 20

    non_adj_adv = \
        short_df[~short_df[column].apply(           # saving indices that we want to drop of non adjectives/adverbs
            lambda row: str(nltk.pos_tag(nltk.word_tokenize(row)))).str.contains(r"JJ|RB", regex=True)].index.tolist()

    df.drop(non_adj_adv, axis=0, inplace=True)      # dropping tweets without adjectives/adverbs and char count <= 20
    return df


def prepend_to_short_tweets(dataframe):
    """Functions finds short tweets (<20 chars), if it's a reply to a tweet, it prepends original tweet to the reply.
    Otherwise, it drops the tweet, as well as those tweets which parent tweet was not found, and those tweets that
    are still <20 chars after prepending. """

    temp_df = dataframe.loc[dataframe['text'].str.len() <= 20, ['text', 'conversation_id']]  # temporary df with short
    temp_df['index'] = temp_df.index  # creating column with their original index, to be used later

    df = pd.merge(dataframe[['tweet_id', 'text']], temp_df, how='right', left_on='tweet_id',
                  right_on='conversation_id')                           # merging

    df['prepended'] = df['text_x'] + '. ' + df['text_y']                # concatenating original tweets to comments
    to_drop = df.loc[(df['text_y'] == df['text_x']) | (df['text_x'].isna()) | (
            df['prepended'].str.len() < 20), 'index'].values.tolist()   # filtering garbage
    dataframe.loc[df['index'], 'text'] = df[
        'prepended'].values.tolist()                                    # assigning concatenated tweets back to main df
    dataframe.drop(to_drop, axis=0, inplace=True)                       # dropping garbage
    return dataframe


def demojize(dataframe):
    """Demojizes emojis"""
    column = 'text'

    def demojize_row(row):
        row = str(row)
        row = emoji.demojize(row)
        return row

    dataframe[column] = dataframe[column].apply(demojize_row)
    return dataframe


def drop_spam_filter_2(dataframe):
    """This function works a first spam filter."""
    df = dataframe.copy(deep=True)
    column = 'text'
    df = df.loc[
        ~(df[column].str.len() > 280)]       # drop tweets with char length > 280, because most of them tend to be spams
    df = df.loc[~((df[column].str.len() > 28) & (df[column].str.split().map(
        lambda words: len(words)) <= 3))]    # (> 28 chars) & (<= 3 words) per tweet ==> drop
    df = df.loc[~(df[column].str.split().map(
        lambda words: sum(len(word) for word in words) / len(words)) >= 15)]  # average len(word) >= 15 ==> drop
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
    column = 'text'

    def clean(row):
        row = str(row)
        row = clean_slang(row)
        row = clean_money(row)
        row = clean_other(row)
        row = clean_contractions(row)
        row = re.sub(r'^\. ', '', row)                   # get rid of the dots in the beginning
        row = re.sub(r'\s+', ' ', row)                   # get rid of extra spaces
        return row

    dataframe[column] = dataframe[column].apply(clean)
    return dataframe


text_pipe = Pipeline(steps=[
    ('remove_common_patters', FunctionTransformer(func=removing_common_patterns)),
    ('spam_filter_1', FunctionTransformer(func=drop_spam_filter_1)),
    ('prepend_to_short_tweets', FunctionTransformer(func=prepend_to_short_tweets)),
    ('demojize', FunctionTransformer(func=demojize)),
    ('spam_filter_2', FunctionTransformer(func=drop_spam_filter_2)),
    ('clean_text', FunctionTransformer(func=clean_text)),
])
