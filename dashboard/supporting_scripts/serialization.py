import pickle
import os


def serialize(**kwargs):
    """
    Helper function, mainly works with task scheduler to serialize heavy database every hour, because that is how
    often database gets updated. Is also used to serialize other objects to improve speed performance.
    :param kwargs: name of the object and object itself
    """
    if not os.path.isdir('./serialized'):
        os.mkdir('./serialization')
    for label, df in kwargs.items():
        with open(f'serialized/{label}.pickle', 'wb') as f:
            pickle.dump(df, f)


def read_serialized(label: str):
    """
    Reads serialized objects
    :param label: str, name of the serialized object
    :return: pd.DataFrame
    """
    try:
        with open(f'serialized/{label}.pickle', 'rb') as f:
            dataframe = pickle.load(f)
    except FileNotFoundError as err:
        raise FileNotFoundError(err)
    return dataframe



