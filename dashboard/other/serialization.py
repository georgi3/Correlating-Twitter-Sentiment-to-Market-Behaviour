import pickle


def serialize(**kwargs):
    for label, df in kwargs.items():
        with open(f'serialized/{label}.pickle', 'wb') as f:
            pickle.dump(df, f)


def read_serialized(label):
    with open(f'serialized/{label}.pickle', 'rb') as f:
        dataframe = pickle.load(f)
    return dataframe
