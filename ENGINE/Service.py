from time import time
import sqlite3
import json
import base64
from algosdk.v2client import indexer

header_features = """'Pants', 'Overcoat', 'Shirt', 'Hip Item', 'Head', 'Background Accent', 'Back Hand', 'Face', 'Footwear', 'Back Item', 'Scenery', 'Body', 'Right Arm', 'Left Arm', 'Necklace', 'Hat', 'Shirts', 'Tattoo', 'Pet', 'Facial Hair', 'Front Hand', 'Background','combat', 'constitution', 'luck', 'plunder'"""
header_comon = """'assetId', 'lastBlockUpdated'"""
header_sold = """'soldPrice', 'soldBlock'"""
header_available = header_features + "," + header_comon
header_sold = header_features + "," + header_sold + "," + header_comon

header = {"available": header_available, "sold": header_sold}

def build_db(once=False):
    _begin = time()
    names = list(header.keys())
    db_createTable(names[0], header[names[0]])
    db_createTable(names[1], header[names[1]])
    _its = build_data(names, [], True, once)
    opt = list(map(lambda z: list(map(lambda x, y: build_table(y, x), z, names)), _its))
    db_saveChanges()
    print("build time: ", - _begin + time())
    return opt

def build_table(_name, data):
    list(map(lambda x: _build_table(x, _name), data))
    return True

def _build_table(_it, _name):
    try:
        _value = _it
        if(_value):
            db_write(_name, _value)
    except:
        db_saveChanges()
        return True



PIRATE_CREATOR = "NEX7N2U36UHBTRHJA4T3WEOOGYMPXL56ICNO4CCJAJBYFJSA7PZJU33H7U"
ALGOSEAS_MARKET = "7SSZHWXNWMY3AV6HV5PTF3R2BPH67BMQPCCXWYQWUPHUKE6E4PA5QUOQSU"

def build_data(_names, data_its=[], nextToken=True, once=False):
    def build(_name, _assets):
        return map(lambda x: get_dataAssetAvailable(x["index"] if _isPirate(x["index"]) else False), _assets) if _name == list(header.keys())[0] else map(lambda x: get_dataAssetSold(x["index"]), _assets)

    if not nextToken:
        return data_its
    try:
        assets, curent_block, nextToken = fetch_assets(PIRATE_CREATOR,nextToken=nextToken)
    except:
        return build_data(_names, data_its, nextToken, once)
    data_its.append(map(lambda x: build(x, assets), _names))
    if once:
        return data_its
    return build_data(_names, data_its, nextToken, once)

DOUBLOON_ASS = 689171215
BASICSKULL_ASS = 778197923

def _isPirate(_id):
    return True if [DOUBLOON_ASS, BASICSKULL_ASS].count(_id) == 0 else False

def _findTable(_name):
    try:
        return True if db_showTable()[0].count(_name)>0 else False
    except:
        return False





def get_assetIdFromTxs(tab_txs):
    _asset = map(lambda x:x["index"] if _isPirate(x["index"]) else False, tab_txs)
    return _asset

def get_dataAssetAvailable(_assetId):
    if not _assetId:
        return False
    _tx_assetFeatures, _curent_block = fetch_txsFromAsset(_assetId, _tx_type="acfg")[0:2]
    if _tx_assetFeatures:
        asset = json.loads(parse_metadataFromTx(_tx_assetFeatures[_tx_assetFeatures.__len__() - 1])).pop("properties")
    else:
        asset = {}
    asset["assetId"] = _assetId
    asset["lastBlockUpdated"] = _curent_block
    return asset

def get_dataAssetSold(assetId):
    _tx_assetSold, curent_block = fetch_txsFromAsset(assetId, _tx_type="axfer")[0:2]
    if _tx_assetSold:
        asset = get_dataFromTxSold(assetId, iter(_tx_assetSold), [])
    else:
        asset = {}
    asset["assetId"] = assetId
    asset["lastBlockUpdated"] = curent_block
    return asset

def get_dataFromTxSold(_assetId, _it, data=[]):
    try:
        tx = _it.__next__()
        if isSoldTx(tx):
            _block = parse_blockFromTx(tx)
            _tx_assetFeatures = fetch_txsFromAsset(_assetId, max_round=_block, _tx_type="acfg")[0]
            _old = json.loads(parse_metadataFromTx(_tx_assetFeatures[_tx_assetFeatures.__len__() - 1])).pop("properties")
            _old["soldPrice"] = parse_price(_block)
            _old["soldBlock"] = _block
            data = _old
        return get_dataFromTxSold(_assetId, _it, data)
    except:
        return {} if data.__len__() == 0 else data

def isSoldTx(_tx):
    try:
        return True if _tx["application-transaction"]["application-id"] == MARKET_APP and _tx["inner-txns"].__len__()==8 else False
    except:
        return False

MARKET_APP = 718331363
BID_APP = 692596876




def parse_blockFromTx(_tx):
    return _tx["confirmed-round"]

def parse_hashFromTx(_tx):
    return _tx["id"]

def parse_price(_block):
    _txs = fetch_txsFromBlock(_block)[0]
    return _txs[_txs.__len__() - 1]['payment-transaction']["amount"]

def parse_metadataFromTx(_tx):
    try:
        return base64.b64decode(_tx["note"])
    except:
        print(_tx)
        return '{"properties": {}}'




PATH_DB = "DB/pirate.db"
DB = sqlite3.connect(PATH_DB)
cur = DB.cursor()

def db_createTable(_name, _header):
    _line = "CREATE TABLE " + _name + "(" + _header + ")"
    cur.execute(_line)

def db_write(_name, data):
    op = "INSERT INTO " + _name + " " + _format(data)
    cur.execute(op)

def db_readAll(_cur, _name):
    res = _cur.execute("SELECT * FROM "+ _name)
    return res.fetchall()

def db_read(_name, _id):
    res = cur.execute("SELECT * FROM "+ _name + " WHERE rowid=" + str(_id))
    return res.fetchall()

def db_flushTable(name):
    try:
        cur.execute("DROP TABLE "+ name)
    except RuntimeError as e:
        print(e)

def db_len(_name):
    res = cur.execute("SELECT COUNT() FROM " + _name)
    return res.fetchall()

def db_showTable():
    res = cur.execute("SELECT name FROM sqlite_master")
    return res.fetchall()

def db_showCol(_name):
    res = cur.execute("SELECT * FROM "+_name)
    return res.fetchall()

def db_addHeader(_name, _data):
    _head = db_showHeader(_name)
    return _add_header(iter(_head), _data, {})

def _add_header(_it, _res ,data={}):
    try:
        _value = _it.__next__()
        if _res[_value[0]] != None:
            data[_value[1]] = _res[_value[0]]
        return _add_header(_it, _res, data)
    except:
        return data

def db_showHeader(_name):
    res = cur.execute("PRAGMA table_info(" + _name + ")")
    return res.fetchall()

def db_saveChanges():
    DB.commit()

def _format(_data):
    _header = str(list(_data.keys()))
    _data = str(list(_data.values()))
    return "(" + str(_header[1:_header.__len__()-1]) +")" + " VALUES (" + _data[1:_data.__len__()-1] + ")"








INDEXER_URL ="https://mainnet-algorand.api.purestake.io/idx2"

_PATH = "/etc/purestake.key"
def get_key ():
    with open(_PATH,"r") as key_file:
        _key = key_file.read()
        return _key[0: _key.__len__() - 1]

HEADERS = {
    "X-API-Key": get_key(),
}

INDEXER_CLIENT = indexer.IndexerClient("", INDEXER_URL, HEADERS)

def fetch_assets(_address, nextToken=True):
    if nextToken == True:
        call = INDEXER_CLIENT.lookup_account_asset_by_creator(_address)
        return _serialized_call(call)
    call = INDEXER_CLIENT.lookup_account_asset_by_creator(_address, next_page=nextToken)
    return _serialized_call(call)

def fetch_txsFromBlock(_block, _address=ALGOSEAS_MARKET, _tx_type="pay"):
    _call = INDEXER_CLIENT.search_transactions_by_address(address=_address , block=_block, txn_type=_tx_type)
    return _serialized_call(_call)

def fetch_txsFromAsset(_assetId, _block=0, min_round=0, max_round=24429353, _tx_type=""):
    try:
        call = INDEXER_CLIENT.search_asset_transactions(asset_id=_assetId, min_round=min_round, max_round=max_round, block=_block, txn_type=_tx_type)
        return _serialized_call(call)
    except:
        return fetch_txsFromAsset(_assetId, _block, min_round, max_round, _tx_type)







def _serialized_call(call):
    _index = list(call.keys())
    if call.__len__() == 2:
        call["next-token"] = False
        _index.append("next-token")
    call = [call[_index[0]], call[_index[1]], call[_index[2]]] if _index.count("transactions") == 0 else [call[_index[2]], call[_index[1]], call[_index[0]]]
    return call








def log_json(data):
    print(json.dumps(data, indent=2, sort_keys=True))







def visualise_tx(tx, keeper):
    _it = iter(tx)
    return _parse_tx(_it, keeper, [])

def _parse_tx(_it, _toKeep, txs=[]):
    try:
        _tx = _it.__next__()
        _it_tuple = iter(_toKeep)
        if _attested_tx(_it_tuple, _tx, []):
            txs.append(_tx)
        return _parse_tx(_it, _toKeep, txs)
    except:
        return txs

def _attested_tx(_it, _tx, _result=[]):
    try:
        _keeper = _it.__next__()
        try:
            _result.append(True) if _tx[_keeper[0]] == _keeper[1] else _result.append(False)
        except:
            _result.append(lookFor_key(_keeper, _tx))
        return _attested_tx(_it, _tx, _result)
    except:
        return True if _result.count(True) == _result.__len__() else False

def lookFor_key(key, _tx):
    try:
        return True if _tx[key] else True
    except:
        return False


def header_add(_name, data):
    if _name == "available":
        _header = db_showHeader("available")
        return {_header[i][1]:data[i] for i in range(0, data.__len__())}
    if _name == "sold":
        _header = db_showHeader("sold")
        return {_header[i][1]:data[i] for i in range(0, data.__len__())}
