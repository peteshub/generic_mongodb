import pandas as pd
import json


def bulk_insert_from_result(result, to_collection,func, batch_size=100000):
    while True:
        lst = []

        for i, doc in enumerate(result):
            lst.append(func(doc))

            if (not result.alive) or (i > batch_size):
                print(i)
                break

        to_collection.bulk_write(lst, ordered=False)

        if not result.alive:
            break

    return to_collection


def load_and_create_key(collection, path, meta, dateparse):
    cols = meta['SHORT'].tolist()
    types = meta['TYPE'].tolist()

    meta.sort_values(by='LABEL', inplace=True)

    label_columns = meta.loc[not meta['LABEL'].isnull(), 'SHORT'].tolist()

    date_cols = [col for col, tp in zip(cols, types) if tp == 'DATE']

    types = [tp if tp != 'DATE' else 'str' for tp in types]
    dtypes = {col: tp.lower() if tp != 'DATE' else 'str' for col, tp in zip(cols, types)}

    load_collection(collection=collection,
                                               path=path,
                                               usecols=cols,
                                               dtypes=dtypes,
                                               date_parser=dateparse,
                                               parse_dates=date_cols,
                                               label_columns=label_columns)

    key_cols = meta.loc[not meta['ISKEY'].isnull(), 'SHORT'].tolist()

    create_key(collection, key_cols)

    return collection


def load_collection(collection, path, usecols, dtypes, date_parser, parse_dates, chunksize=10000, sep=";", label_columns=[]):

    cols = pd.read_csv(path,sep=sep, header=None,nrows=1).as_matrix().tolist()[0]
    usecols = list(set(cols).intersection(set(usecols)))
    parse_dates = list(set(cols).intersection(set(parse_dates)))

    chunks = pd.read_csv(path, usecols=usecols, chunksize=chunksize, dtype=dtypes, sep=sep, date_parser=date_parser,
                         parse_dates=parse_dates)

    for chunk in chunks:
        chunk['source'] = path
        chunk['label'] = chunk.apply(lambda x: " ".join([ str(x[l]) for l in label_columns]), axis = 1)
        collection.insert_many(json.loads(chunk.to_json(orient='records')))

    return collection


def create_relationship(from_collection, to_collection, keys, name):

    create_key(to_collection, keys)

    for item in from_collection.find():
        to_collection.update_many({key:item[key] for key in keys},
                                  {"$set":
                                       {name:
                                            {"_id":item['_id'],
                                             "label" : item['label']
                                             }
                                        }
                                   }
                                  )

    return to_collection


def create_key(collection, keys):
    kex_indexes = [(key, 1) for key in keys]
    collection.create_index(kex_indexes, unique=True)

    return collection


def create_indexes(collection, indexes):
    for idx in indexes:
        collection.create_index([(idx,1)])

    return collection
