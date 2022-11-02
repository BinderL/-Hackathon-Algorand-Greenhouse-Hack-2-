from time import time
import pytest
import os
import random

from ENGINE.Service import get_dataAssetAvailable, get_dataAssetSold
from ENGINE.Service import build_db, PATH_DB
from ENGINE.Similarity import classify
from ENGINE.Service import db_read
from ENGINE.Service import log_json, header_add

@pytest.mark.db
def test_getData():
    _begin = time()
    print("\nFetching pirate AlgoSeas Pirate #21103")
    _assetId = 917606678
    data = get_dataAssetAvailable(_assetId)
    _end = time() - _begin
    print(data)
    data = get_dataAssetSold(_assetId)
    print("Pirate #21103 has been sold " + str(data["soldPrice"]) + " algo at round" + str(data["soldBlock"]))
    print(data)
    print('time: ', _end, 's')

@pytest.mark.db
def test_buildDbWith100Sample():
    _begin = time()
    print("\nBuilding db for 100 samples")
    _repo, _file = PATH_DB.split("/")
    if not os.listdir(_repo).count(_file) == 1:
        build_db(True)
    print("DB has been built here: ", PATH_DB)
    _end = time() - _begin
    print('time: ', _end, 's')

@pytest.mark.similarity
def test_nnbOnDataset():
    _begin = time()
    print("finding similarity with k nearest neighbors of 2")
    dataToClass = db_read("available", 1)[0]
    data = classify("available", dataToClass)
    print("first neighbors: ")
    log_json(header_add("available", data[0]))
    print("with similarity measure of: " + str(data[1]), "\nsecond neighbors: ")
    log_json(header_add("available", data[2]))
    print("with similarity measure of" + str(data[3]))
    _end = time() - _begin
    print('time: ', _end, 's')

@pytest.mark.similarity
def test_similarity():
    _begin = time()
    _rand = int(random.random()*100 + 1)
    dataToClass = db_read("available", _rand)[0]
    data = classify("sold", dataToClass)
    print("this NFT"),
    log_json(header_add("available", dataToClass))
    print(" is similar to")
    log_json(header_add("sold", data[2]))
    print("which has been sold: ", data[2][27]/100000, " algo")

@pytest.mark.similarity
def test_weightedStrategies():
    print("weight strategies 1: plunder")
    _rand = int(random.random()*100+1)
    dataToClass = db_read("available", _rand)[0]
    data = classify("available", dataToClass, [1 for i in range(0, dataToClass.__len__()-6)]+[1,1,1,10])
    _helper(dataToClass, data)
    print("weight strategies 1: beauty as I am")
    _rand = int(random.random()*100+1)
    dataToClass = db_read("available", _rand)[0]
    data = classify("available", dataToClass, [10 for i in range(0, dataToClass.__len__()-6)]+[1,1,1,1])
    _helper(dataToClass, data)




def _helper(_dataToClass, _data):
    print("this NFT"),
    log_json(header_add("available", _dataToClass))
    print(" is similar to")
    log_json(header_add("sold", _data[2]))

