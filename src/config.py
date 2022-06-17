import os
from dotenv import load_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')

load_dotenv(dotenv_path)

GC_API_KEY = os.environ['GC_API_KEY']
GC_API_SECRET_KEY = os.environ['GC_SECRET_API']
GC_BEARER_TOKEN = os.environ['GC_BEARER_TOKEN']
GC_ACCESS_TOKEN = os.environ['GC_ACCESS_TOKEN']
GC_ACCESS_TOKEN_SECRET = os.environ['GC_ACCESS_TOKEN_SECRET']

API_KEY = os.environ['API_KEY_PR']
API_SECRET_KEY = os.environ['SECRET_API_PR']
BEARER_TOKEN = os.environ['BEARER_TOKEN_PR']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN_PR']
ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET_PR']

# DATABASE
DATABASE = os.environ['DATABASE']
USER = os.environ['DB_USER']
PASSWORD = os.environ['PASSWORD']
HOST = os.environ['HOST']
PORT = os.environ['PORT']

TARGET_ACCOUNTS = ['BitcoinMagazine', 'CoinDesk', 'Bitcoin', 'BT']
