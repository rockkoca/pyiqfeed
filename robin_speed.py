from robinhood.Robinhood import *
from robinhood.credentials import *
from datetime import datetime as dt
from my_functions import *
import requests
import random
from numba import jit

print(trader.get_account())

update_mongo = UpdateMongo()
db = update_mongo.get_db()

amd = db.instruments.find_one({'symbol': 'AMD'})

cache = {
    'bot': {
        'AMD': {
            'auto.lv2_quick_sell': True
        }
    }
}


def random_lv2():
    choices = [10.11, 10, 11, 10, 11, 10.08, 10.12, 10, 12, 10, 12, 10.05, 10, 10.01, 10.02, 10.06, 10, 9, 8, 8.5]
    return {
        'bidinfovalid': True,
        'bid': random.choice(choices),
        'bid_size': random.randrange(100, 3000),
        'ask': random.choice(choices),
        'ask_size': random.randrange(100, 3000),
        'askinfovalid': True
    }


def lv2_speed():
    lv2 = {
        'bids': {},
        'asks': {}
    }  # symbol', 'MMID', 'bid', 'ask', 'bid_size', 'ask_size', 'bidinfovalid', 'askinfovalid'
    for val in vals:

        if val['bidinfovalid']:
            bid = val['bid']
            lv2['bids'][bid] = lv2['bids'].get(bid, 0) + val['bid_size']

        if val['askinfovalid']:
            ask = val['ask']
            lv2['asks'][ask] = lv2['asks'].get(ask, 0) + val['ask_size']

    lv2['symbol'] = 'AMD'

    lv2['bids_order'] = sorted(list(lv2['bids'].keys()), reverse=True)
    lv2['asks_order'] = sorted(list(lv2['asks'].keys()))
    #
    # lv2['bids_total'] = sum(lv2['bids'].values())
    # lv2['asks_total'] = sum(lv2['asks'].values())
    # print(lv2)
    result = {
        'symbol': 'AMD',
        'bids': lv2['bids_order'],
        'bids_price': [lv2['bids'][price] for price in lv2['bids_order']],
        'asks': lv2['asks_order'],
        'asks_price': [lv2['asks'][price] for price in lv2['asks_order']],
        'bids_total': sum(lv2['bids'].values()),
        'asks_total': sum(lv2['asks'].values())
    }


# vals = [random_lv2() for i in range(200)]

# trader = Robinhood()
# trader.login(username=Credential.get_username(), password=Credential.get_password())

update_mongo = UpdateMongo()
db = update_mongo.get_db()

print(type(db.nonzero_positions.find_one({'_id': "vciE7HofYmyCTAFZX"})['quantity']))


# print(trad)
def search_mongo(ty: str):
    if ty == 'ins':
        return db.instruments.find()
    if ty == 'orders':
        return db.orders.find({'cancel': {'$ne': None}})
    if ty == 'pos':
        return db.nonzero_positions.find()
        # return db.orders.find({'symbol': 'AMD', 'cancel': {'$ne': None}})


def sync_mongo():
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        ins = executor.submit(search_mongo, ty='ins')
        orders = executor.submit(search_mongo, ty='orders')
        positions = executor.submit(search_mongo, ty='pos')
        try:
            ins = ins.result()
            orders = orders.result()
            positions = positions.result()
        except Exception as exc:
            print(f'exceptions when syncing mongo {exc}')
        else:
            for instrument in ins:
                update_mongo.mongo_cache['ins'][instrument['symbol']] = instrument
                update_mongo.mongo_cache['ins_to_symbol'][instrument['url']] = instrument
            for order in orders:
                update_mongo.mongo_cache['orders'][order['instrument']] = order
            for pos in positions:
                update_mongo.mongo_cache['pos'][pos['instrument']] = pos


start = dt.datetime.now()

# trader.get_quote('AMD')
# requests.get('https://api.robinhood.com/quotes/?symbols=GPRO,DRYS,AMD')
# l = [x for x in range(200)]
# x = [l for i in range(200)]
# for i, l in enumerate(x):
#     x[i] = sorted(l)
# order = trader.limit_buy(amd, 1, 1)
#
# print(order)

# print(trader.url('https://api.robinhood.com/accounts/5SA59772/positions/940fc3f5-1db5-4fed-b452-f3a2e4562b5f/'))
# test = db.instruments.find_one({'auto.lv2_quick_sell': True})
# orders = db.orders.find({'symbol': 'AMD', 'cancel': {'$ne': None}})
# for j in range(1000):
#     sync_mongo()



end = dt.datetime.now()
# print(result)
# print(trader.url(order['position']))

# time.sleep(5)
# print(trader.cancel_order(order))
# print(trader.cancel_order(order))


# print(trader.url_post("https://api.robinhood.com/orders/22e0ba99-b346-4591-b36d-55d4d617cb57/cancel/"))

used = end - start
us = used.microseconds
print(f'time used {us / 1000} ms or {us / 1000 / 1000} secs')
print(f'time used {us / 1000 / 1000} ms or {us / 1000 / 1000 / 1000} secs')
# for order in search_mongo('orders'):
#     print(order)
# print(update_mongo.mongo_cache['orders'])
print(update_mongo.get_instrument('AMD'))
print(update_mongo.mongo_cache)
