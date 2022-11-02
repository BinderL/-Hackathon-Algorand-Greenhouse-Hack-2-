from time import time

from sklearn.neighbors import NearestNeighbors
import numpy as np

import json
from ENGINE.Service import build_db, db_readAll, PATH_DB, sqlite3

header = ['Pants', 'Overcoat', 'Shirt', 'Hip Item', 'Head', 'Background Accent', 'Back Hand', 'Face', 'Footwear', 'Back Item', 'Scenery', 'Body', 'Right Arm', 'Left Arm', 'Necklace', 'Hat', 'Shirts', 'Tattoo', 'Pet', 'Facial Hair', 'Front Hand', 'Background']

def testing_Y():
    return ('Sea', None, 'Green Blouse', None, 'Mohawk', None, 'Bladed', None, 'Sandals', None, 'Ocean', 'Coconut', None, None, None, None, None, None, None, None, None, None, 44, 39, 55, 40)

def init_db(_path_db = PATH_DB):
    DB = sqlite3.connect(_path_db)
    return DB.cursor()

def classify(_name, _data):
    _time_begin = time()
    cur = init_db("DB/pirate.db")
    _X = db_readAll(cur, _name)
    _Y = _data
    _bag = build_bag()
    _Y_harmo = indexWithBag([_Y], _bag)
    _X = clean_stuff(_X)
    _X_harmo = indexWithBag(_X, _bag)
    _dist, _ind = get_nearestNeighbors(_Y_harmo, _X_harmo)
    _time_end = - _time_begin + time()
    print("time: " + str(_time_end))
    return _X[_ind[0][0]], _dist[0][0], _X[_ind[0][1]], _dist[0][1]

def get_nearestNeighbors(_Y, _X):
    nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree').fit(_X)
    distances, indices = nbrs.kneighbors(_Y)
    return distances, indices

def clean_stuff(X):
    list(map(lambda x: _clean_stuff(iter(x[0:26]), X), X))
    return X

def _clean_stuff(_it, X):
    try:
        _value = _it.__next__()
        if _value == 0:
            X[X.index(_value)] = None
        _clean_stuff(_it, X)
    except:
        return



bag = [[None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None]]

def build_bag():
    cur = init_db("DB/pirate.db")
    _data = db_readAll(cur, "available")
    extractBagOfWords(bag, _data)
    return bag

def extractBagOfWords(bag, data):
    list(map(lambda x: parse_data(x[0:22], bag) ,data))

def parse_data(_data, bag):
    def apply(x, _id, bag):
        if bag[_id].count(x)==0:
            bag[_id].append(x)
    list(map(lambda x:apply(x, _data.index(x), bag), _data))

def indexWithBag(_data, _bag):
    _ncol_words = _bag.__len__()
    _data_indexed = list(map(lambda x: list(map(lambda y: _bag[x.index(y)].index(y) * 100 / _bag[x.index(y)].__len__() if x.index(y) < _ncol_words else y * _ncol_words / 4, x[0:26])) ,_data))
    return _data_indexed

