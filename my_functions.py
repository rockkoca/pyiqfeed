#! /usr/bin/env python3
# coding=utf-8
"""
Some examples of how to use the library.

Run first with no options to see the usage message.
Then try with different options to see that different functionality. Not all
library functionality is used in this file. Look at conn.py and listeners.py
for more details.
"""
import sys
import argparse
import pyiqfeed as iq
import requests
import datetime
import numpy as np
import math
from typing import Sequence
import time
from passwords import dtn_product_id, dtn_login, dtn_password
from pyiqfeed import *
from pymongo import MongoClient
import threading
import datetime as dt
import subprocess
import concurrent.futures
import numpy as np
from talib.abstract import *
from talib import MA_Type
from scipy import stats as st

import robinhood.Robinhood as RB
from robinhood.Robinhood import *
from robinhood.credentials import *

verbose = 1
look_back_bars = 720

trader = RB.Robinhood()
trader.login(username=Credential.get_username(), password=Credential.get_password())


def set_timeout(sec: float, func: object, *args, **kwargs) -> threading.Timer:
    t = threading.Timer(sec, func, *args, **kwargs)
    t.start()
    return t


def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()

    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


launch_service_lock = False


def relaunch_service():
    for i in range(10):
        subprocess.call('killall winedevice.exe', shell=True)
        subprocess.call('killall iqconnect.exe', shell=True)

    launch_service()


def check_connection():
    global launch_service_lock
    try:
        # if key == 'bar:TOPS' and future.running():
        #     print(key, future.running())
        quote_conn = iq.QuoteConn(name="test connection")
        quote_conn.connect()
        quote_conn.disconnect()
        # print('connection is healthy')
    except Exception as e:
        # if str(e).startswith('[Errno'):
        if not launch_service_lock:
            launch_service_lock = True
            relaunch_service()
        time.sleep(15)
        launch_service_lock = False


def is_server() -> bool:
    return sys.platform != 'darwin'


class Math(object):
    @staticmethod
    def to_2_decimal_floor(f: float) -> str:
        return f'{math.floor(f*100)/100:.2f}'


class UpdateMongo(object):
    stop = 0
    static_cache = {
        'date': {}
    }

    cache = {
        'bars': {},
        'update_mongo_time': {},
        'trend': {},
        'date': {},
        'lv1': {},
        'lv2': {},
        'lv2_result': {},

    }

    mongo_cache = {
        'ins': {},
        'ins_to_symbol': {},
        'orders': {},
        'pos': {}

    }

    def __init__(self):

        if sys.platform == 'darwin':
            self.client = MongoClient("mongodb://localhost:3001")
            self.db = self.client.meteor

        else:
            self.client = MongoClient("mongodb://localhost:27017")
            self.db = self.client.stock

    def get_client(self):
        return self.client

    def get_db(self):
        return self.db

    @staticmethod
    def default_bar_data(symbol, name):
        return {
            'bars': np.array([]),
            'symbol': symbol,
            'name': name,
            'last_cross_b': '',
            'cross_b_and_close_above': '',
        }

    def get_symbols(self) -> dict:
        symbols = {}
        col = self.db.instruments
        # resultss = col.find()
        for result in col.find():
            symbols[result['symbol']] = result
        return symbols

    def ins_to_symbol(self, ins: str) -> str:
        return self.get_instrument().get('symbol', '')

    def get_instrument(self, symbol: str) -> dict:
        if symbol in self.mongo_cache['ins']:
            return self.mongo_cache['ins'][symbol]
        else:
            self.mongo_cache['ins'][symbol] = self.db.instruments.find_one({'symbol': symbol})
            if not self.mongo_cache['ins'][symbol]:
                return {}
            self.mongo_cache['ins'][self.mongo_cache['ins'][symbol]['url']] = self.db.instruments.find_one(
                {'symbol': symbol})
            return self.mongo_cache['ins'][symbol]

    @staticmethod
    def _process_regional_quote(data: np.array) -> dict:
        if len(data) == 0:
            return {}
        else:
            fields = data[0]
            rgn_quote = dict()
            rgn_quote["symbol"] = UpdateMongo.process_binary_symbol(fields[0])
            rgn_quote["bid_price"] = str(fields[1])
            rgn_quote["bid_size"] = int(fields[2])
            # rgn_quote["bidTime"] = fields[5]
            rgn_quote["ask_price"] = str(fields[4])
            rgn_quote["ask_size"] = int(fields[5])
            # rgn_quote["askTime"] = fields[8]
            # rgn_quote["Fraction Display Code"] = fields[9]
            # rgn_quote["Decimal Precision"] = fields[10]
            # rgn_quote["Market Center"] = fields[11]
            return rgn_quote

    def update_regional_quote(self, data: np.array) -> None:
        col = self.db.quotes
        dic = self._process_regional_quote(data)
        if dic:
            # print(dic)
            keys = list(dic.keys())
            new_dic = {}
            old = col.find_one({'symbol': dic['symbol']})
            # print(old)
            if not old:
                old = {}

            if new_dic['tick'] == old.get('tick', 0):
                return

            for key in keys:
                if dic[key] != 'nan' and dic[key]:
                    new_dic[key] = dic[key]
                else:
                    if key in old:
                        new_dic[key] = old[key]

            result = col.update_one(
                {'symbol': new_dic['symbol']},
                {
                    "$set": new_dic,
                },
                True
            )
            # print(result)

    @staticmethod
    def _process_fundamentals(data: np.array) -> dict:
        data = data[0].tolist()
        fundamental = {}
        for i, info in enumerate(data):
            ty = type(info)
            if ty == bytes:
                info = UpdateMongo.process_binary_symbol(info)
            if ty == datetime.datetime or ty == datetime.date:
                info = str(info)
            fundamental[QuoteConn.fundamental_keys[i]] = info
        return fundamental

    def update_fundamentals(self, data: np.array) -> None:
        col = self.db.fundamentals
        dic = self._process_fundamentals(data)
        col.update_one(
            {'symbol': dic['symbol']},
            {
                "$set": dic,
            },
            True
        )

    @staticmethod
    def process_quote(data: np.array) -> dict:
        if len(data) == 0:
            return {}
        else:
            fields = data[0]
            rgn_quote = dict()
            # print(fields)
            # if str(fields[43]) in UpdateMongo.static_cache['date']:
            #     dt_date = UpdateMongo.static_cache['date']
            # else:
            #     dt_date = iq.field_readers.read_ccyymmdd(str(fields[43]).replace('-', ''))
            #     UpdateMongo.static_cache['date'] = dt_date

            rgn_quote["symbol"] = UpdateMongo.process_binary_symbol(fields[0])
            rgn_quote["ask_price"] = str(fields[1])
            rgn_quote["ask_size"] = int(fields[4])
            rgn_quote["bid_price"] = str(fields[8])
            rgn_quote["bid_size"] = int(fields[11])
            rgn_quote['close'] = str(fields[15])
            rgn_quote['delay'] = str(fields[20])
            rgn_quote['last'] = str(fields[22])
            rgn_quote['last_date'] = str(fields[23])
            rgn_quote['last_market'] = str(fields[24])
            rgn_quote['last_size'] = int(fields[25])
            rgn_quote['last_time'] = UpdateMongo.tick_time_js_timestamp(str(fields[43]), int(fields[26]))
            rgn_quote['change'] = str(fields[27])
            rgn_quote['open'] = str(abs(fields[27] - fields[22]))
            rgn_quote['change_from_open'] = str(fields[28])
            rgn_quote['high'] = str(fields[31])
            rgn_quote['low'] = str(fields[37])
            rgn_quote['tick_vol'] = int(fields[35])
            rgn_quote['volume'] = int(fields[65])
            rgn_quote['tick'] = int(fields[64])

            # dt_datetime = iq.field_readers.date_us_to_datetime(dt_date, int(fields[36]))
            #
            # rgn_quote['tick_time'] = str(int(np.floor(dt_datetime.timestamp()))) + '000'
            rgn_quote['tick_time'] = UpdateMongo.tick_time_js_timestamp(str(fields[43]), int(fields[36]))
            # rgn_quote["bidTime"] = fields[5]

            # rgn_quote["askTime"] = fields[8]
            # rgn_quote["Fraction Display Code"] = fields[9]
            # rgn_quote["Decimal Precision"] = fields[10]
            # rgn_quote["Market Center"] = fields[11]
            # print(
            #     "symbol:{}, ask{}, size:{}, bid:{} size:{} close:{}, last: {},high:{}, ?: {} tick_vol:{}, vol:{}, tick: {}".format(
            #         fields[0],rx
            #         fields[1],
            #         fields[4],
            #         fields[8],
            #         fields[11],
            #         fields[15],
            #         fields[22],
            #         fields[31],
            #         fields[32],
            #         fields[35],
            #         fields[65],
            #         fields[64]
            #     ))
            return rgn_quote

    @staticmethod
    def tick_time(yymmdd: str, us: int) -> datetime.datetime:
        if yymmdd in UpdateMongo.static_cache['date']:
            dt_date = UpdateMongo.static_cache['date'][yymmdd]
        else:
            dt_date = iq.field_readers.read_ccyymmdd(yymmdd.replace('-', ''))
            UpdateMongo.static_cache['date'][yymmdd] = dt_date
        dt = iq.field_readers.date_us_to_datetime(dt_date, int(us))
        return dt

    @staticmethod
    def tick_time_js_timestamp(yymmdd: str, us: int) -> str:
        return UpdateMongo.tick_time_timestamp(yymmdd, us) + '000'

    @staticmethod
    def tick_time_timestamp(yymmdd: str, us: int) -> str:
        return str(int(np.floor(UpdateMongo.tick_time(yymmdd, us).timestamp())))

    def update_index(self, data: np.array, name: str) -> None:
        col = self.db.index
        dic = self.process_quote(data)
        # print(dic)
        symbol = IndexWatcher.symbol_map.get(dic['symbol'])
        dic['symbol'] = symbol
        # print(symbol)
        update_meteor = True

        if dic:
            # print(dic)
            keys = list(dic.keys())
            new_dic = {}
            old = col.find_one({'symbol': symbol})
            # print(old)
            if not old:
                old = {}

            for key in keys:
                if dic[key] != 'nan' and dic[key]:
                    new_dic[key] = dic[key]
                else:
                    if key in old:
                        new_dic[key] = old[key]

            if new_dic['tick'] == old.get('tick', 0):
                return

            if update_meteor:
                result = col.update_one(
                    {'symbol': new_dic['symbol']},
                    {
                        "$set": new_dic,
                    },
                    True
                )
                # print(dir(result))
                # print(result.matched_count, result.row_result)

    def update_quote(self, data: np.array, name: str) -> None:
        col = self.db.quotes
        dic = self.process_quote(data)
        # print(dic)
        symbol = dic['symbol']
        # print(symbol)
        update_meteor = name.startswith('auto_unwatch')
        if dic:
            # print(dic)
            keys = list(dic.keys())
            new_dic = {}
            old = col.find_one({'symbol': symbol})
            # print(old)
            if not old:
                old = {}

            for key in keys:
                if dic[key] != 'nan' and dic[key]:
                    new_dic[key] = dic[key]
                else:
                    if key in old:
                        new_dic[key] = old[key]
            if 'last_size' not in new_dic:
                print(old, dic)

            if new_dic['tick'] == old.get('tick', 0):
                return
            else:
                self.cache['lv1'][symbol] = new_dic

            if update_meteor:
                result = col.update_one(
                    {'symbol': new_dic['symbol']},
                    {
                        "$set": new_dic,
                    },
                    True
                )
                # print(dir(result))
                # print(result.matched_count, result.row_result)

    @staticmethod
    def _process_lv2(data: dict) -> dict:
        # print(data)
        # data = data[0].tolist()
        date = str(data['bid_date'].tolist())
        bid_time = min(85400000000, int(data['bid_time']))
        ask_time = min(85400000000, int(data['ask_time']))
        data['bid_time'] = UpdateMongo.tick_time_js_timestamp(date, bid_time)
        data['ask_time'] = UpdateMongo.tick_time_js_timestamp(date, ask_time)
        data['bid_date'] = date
        data['ask_date'] = date
        # print(data)

        # for i, info in enumerate(data):
        #     ty = type(info)
        #     if ty == bytes:
        #         info = UpdateMongo.process_binary_symbol(info)
        #     if ty == datetime.datetime or ty == datetime.date:
        #         info = str(info)
        #     fundamental[QuoteConn.fundamental_keys[i]] = info
        return data

    def update_lv2(self, data: dict, name: str) -> None:
        if verbose:
            start = dt.datetime.now()
        col = self.db.lv2
        # dic = self._process_lv2(data)
        dic = data
        # print(dic)
        symbol = dic['symbol']
        # print(symbol)
        update_meteor = name.startswith('auto_unwatch')

        if dic:
            # print(dic)
            keys = list(dic.keys())
            bid = dic['bidinfovalid']
            ask = dic['askinfovalid']
            # old = col.find_one({'symbol': symbol})
            old = self.cache['lv2'].get(symbol, {})
            # print(old)

            mmid = dic['MMID']
            old_mmid_data = {}
            if not old:
                new_dic = {
                    mmid: dic
                }
            else:
                new_dic = old
                new_dic[mmid] = dic
                # if mmid not in new_dic:
                #     new_dic[mmid] = dic
                # else:
                #     new_dic[mmid] = dic
                # old_dic = new_dic.get(mmid)
                # old_mmid_data = old_dic.copy()
                # for key in keys:
                #     # if (ask and key.startswith('ask')) or (bid and key.startswith('bid')):
                #     new_dic[mmid][key] = dic[key]
                # else:
                #     new_dic[mmid][key] = old_dic.get(key, dic[key])
            #
            # for k, v in new_dic.items():
            #     print(type(v))

            self.cache['lv2'][symbol] = new_dic

            if update_meteor:
                # TODO create a best data structure for the web
                # result = self.cache['lv2_result'].get(symbol, {})
                # bids = result.get('bids', False)
                # asks = result.get('asks', False)
                # if result and old_mmid_data and ((bid and bids and dic['bid'] == bids[0] == old_mmid_data['bid'])
                #                                  or (ask and asks and dic['ask'] == asks[0] == old_mmid_data['bid'])):
                #
                #     if bid and dic['bid_size'] != old_mmid_data['bid_size']:
                #         result['bids_price'][0] += \
                #             dic['bid_size'] - old_mmid_data['bid_size']
                #     else:
                #         if bid and dic['ask_size'] != old_mmid_data['ask_size']:
                #             result['asks_price'][result['asks'].index(dic['ask'])] += \
                #                 dic['ask_size'] - old_mmid_data['ask_size']
                # else:

                lv2 = {
                    'bids': {},
                    'asks': {}
                }
                # symbol', 'MMID', 'bid', 'ask', 'bid_size', 'ask_size', 'bidinfovalid', 'askinfovalid'
                print(len(new_dic.keys()))
                for val in new_dic.values():

                    if val['bidinfovalid']:
                        bid = val['bid']
                        lv2['bids'][bid] = lv2['bids'].get(bid, 0) + val['bid_size']

                    if val['askinfovalid']:
                        ask = val['ask']
                        lv2['asks'][ask] = lv2['asks'].get(ask, 0) + val['ask_size']

                lv2['symbol'] = symbol

                lv2['bids_order'] = sorted(list(lv2['bids'].keys()), reverse=True)
                lv2['asks_order'] = sorted(list(lv2['asks'].keys()))
                #
                # lv2['bids_total'] = sum(lv2['bids'].values())
                # lv2['asks_total'] = sum(lv2['asks'].values())
                # print(lv2)
                result = {
                    'symbol': symbol,
                    'bids': lv2['bids_order'],
                    'bids_price': [lv2['bids'][price] for price in lv2['bids_order']],
                    'asks': lv2['asks_order'],
                    'asks_price': [lv2['asks'][price] for price in lv2['asks_order']],
                    'bids_total': sum(lv2['bids'].values()),
                    'asks_total': sum(lv2['asks'].values())
                }

                # self.cache['lv2_result'][symbol] = result
                # print(result)

                # TODO trigger quick sell, cancel order
                # threading.Thread
                if verbose:
                    end = dt.datetime.now()
                    used = end - start
                    us = used.microseconds
                    print(f'time used before mongo {us / 1000} ms or {us / 1000 / 1000} secs')

                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    task = ''
                    start_task = dt.datetime.now()
                    bid_size = result['bids_price']
                    ask_size = result['asks_price']

                    if len(bid_size) > 0 and len(ask_size) > 0 and bid_size[0] / ask_size[0] < .7:
                        # task = executor.submit(self.call_server_api, path='v2-quick-sell', data={
                        #     'symbol': symbol
                        # })
                        pass

                    result = col.update_one(
                        {'symbol': symbol},
                        {
                            "$set": result,
                        },
                        True
                    )

                    try:
                        data = task.result()
                    except Exception as exc:
                        print('%r generated an exception: %s' % ('v2-quick-sell' + symbol, exc))
                    else:
                        print('%r submitted, %s, used %s ms' % ('v2-quick-sell' + symbol, data,
                                                                (dt.datetime.now() - start_task).microseconds / 1000))
                # print(dir(result))
                # print(result.matched_count, result.row_result)
                pass
            if verbose:
                end = dt.datetime.now()
                used = end - start
                us = used.microseconds
                print(f'time used after mongo update {us / 1000} ms or {us / 1000 / 1000} secs')

    def lv2_quick_sell(self, symbol: str):
        now = dt.datetime.now()
        key = f'lv2_quick_sell-{symbol}'
        if key in self.cache and (now - self.cache[key]).microseconds / 1000 < 1000:
            self.cache[key] = now
            return
        self.cache[key] = now

        db = self.get_db()
        # first find the instrument of this symbol
        ins = self.get_instrument(symbol)

        if not ins:  # this is not going to happen!!!!!, just in case
            ins = trader.instrument(symbol.upper())
            db.instruments.insert({'symbol': symbol}, ins, True)
        # get all the pending orders of this stock
        orders = db.orders.find({'instrument': ins['url'], 'cancel': {'$ne': None}})
        pos = db.nonzero_positions.find_one({'instrument': ins['url']})
        if not orders and not (
                            float(pos['shares_held_for_buys']) > 0 or float(pos['quantity']) > 0 or float(
                    pos['shares_held_for_sells']) > 0):
            return
        if verbose:
            print(f'time used before with statement: {self.pt_time_used(now)}')
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:

            # 首先不管三七二十一, 立即取消所有订单!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            # TODO 如果有 pending buy, 立即发出一个市价单?? 如果已经成交, 这个赔钱概率99.99999%
            # TODO if has pending buying order. cancel right away, and place a market sell at same time
            # TODO 因为订单可能已经被执行, 所以同时发出一个市价单去清仓
            # TODO 但是, 也有可能没有执行或者没有完全执行. 这是需要更新 position 并且再次下一个市价单

            # TODO 如果有 pending limit selling order, 立即取消订单, 并在 8 ms 后发出一个市价单清仓, 不要等返回消息
            # TODO 因为需要至少20ms才能知道结果. 这时也需要更新 position, 然后没有销售掉的情况下再次发送市价单清仓
            # TODO This is the most important one!!!

            canceled_orders = {}
            selling_orders = {}
            order_types = {}
            for order in orders:
                canceled_orders[executor.submit(self.cancel_order, order=order)] = order
                order_types[order['type']] = [order] if order['type'] not in order_types else order_types[
                                                                                                  order['type']] + [
                                                                                                  order]

                order_types[order['side']] = [order] if order['side'] not in order_types else order_types[
                                                                                                  order['side']] + [
                                                                                                  order]
                if verbose:
                    print(f'canceling {order}')
            if verbose:
                print(f'time used before for_buy order: {self.pt_time_used(now)}')

            if 'buy' in order_types:
                selling_orders['for_buy'] = executor.submit(self.place_market_sell_order, ins=ins,
                                                            qty=int(float(pos['shares_held_for_buys'])))
                if verbose:
                    print(f"market selling for_buy {pos['shares_held_for_buys']}")

            # if 'sell' in order_types:
            #     time.sleep(.007)  # 暂停7 ms, 等待订单成功取消, 否则发出订单也会被拒绝
            #     selling_orders['for_sell'] = executor.submit(self.place_market_sell_order, ins=ins, qty=pos['quantity'],
            #                                                  avg_price=pos['average_buy_price'])
            # if 'sell' not in order_types:
            #     time.sleep(.01)  # 暂定10ms, 然后更新 position
            # else:
            #     time.sleep(.003)  # 暂定10ms, 然后更新 position
            if verbose:
                print(f'time used before 等待所有取消订单: {self.pt_time_used(now)}')
            # TODO 等待所有取消订单成功后更新 position
            for future in concurrent.futures.as_completed(canceled_orders):
                try:
                    data = future.result()
                except Exception as e:
                    print(e)
            if verbose:
                print(f'time used after 等待所有取消订单: {self.pt_time_used(now)}')

            # 先发出一个市价单清仓在更新 position, 因为更新 position 还需要大概20ms
            if 'sell' in order_types or float(pos['quantity']) > 0:
                selling_orders['for_sell'] = executor.submit(self.place_market_sell_order, ins=ins,
                                                             qty=int(float(pos['quantity'])),
                                                             avg_price=float(pos['average_buy_price']))
                print(f"market selling for_sell {pos['quantity']}")
            if verbose:
                print(f'time used after market selling for_sell: {self.pt_time_used(now)}')

            try:
                pos = self.get_position(pos)
            except Exception as e:
                print(e)
            else:
                pos['quantity'] = int(float(pos['quantity']))
                if pos['quantity'] > 0:
                    # 如果还有 position, 清理掉, 因为上一步的市价单的 qty 可能不正确
                    result = self.place_market_sell_order(ins=ins, qty=pos['quantity'],
                                                          avg_price=float(pos['average_buy_price']))
                    self.db.orders.insert_one(result)
                    if verbose:
                        print(f"market selling final {pos['quantity']} {result}")
            if verbose:
                print(f'time used after market selling final: {self.pt_time_used(now)}')

            try:
                for_sell = selling_orders['for_sell'].result()
                for_buy = selling_orders['for_buy'].result()
            except Exception as e:
                print(e)
            else:
                self.db.orders.insert_one(for_sell)
                self.db.orders.insert_one(for_buy)
                # if verbose:
                #     print(for_buy, type(for_buy))
                #     print(for_sell, type(for_sell))
            if verbose:
                print(f'time used after lv2_quick_sell is done: {self.pt_time_used(now)}')

    @staticmethod
    def place_market_sell_order(ins: dict, qty: int, avg_price: float) -> dict:
        try:
            order = trader.place_order(instrument=ins, quantity=qty, price=Math.to_2_decimal_floor(avg_price * .97),
                                       transaction=Transaction.SELL)
        except Exception as e:
            print(e)
            return {'detail': e}
        else:
            return order

    @staticmethod
    def pt_time_used(start: dt.datetime, pt='') -> None:
        us = (dt.datetime.now() - start).microseconds
        if pt == '':
            return f' {us / 1000} ms or {us / 1000 / 1000} secs'
        else:
            print(f'{pt}: time used {us / 1000} ms or {us / 1000 / 1000} secs')

    @staticmethod
    def cancel_order(order: dict):
        try:
            return trader.url_post(order['cancel'])
        except Exception as e:
            print(e)
            return {'detail': e}

    @staticmethod
    def get_position(pos: dict):
        try:
            return trader.url_json(pos['url'])
        except Exception as e:
            print(e)
            return {'detail': e}

    @staticmethod
    def call_server_api(path: str, data={}):
        url = 'http://localhost:3000/f991eb733c339f4a5f9087354e9c683cd0b9969f/'
        if path == 'lv2-quick-sell':
            return requests.get(f'{url}/{path}/{data["symbol"]}').content

    @staticmethod
    def process_binary_symbol(s) -> str:
        # TODO make sure this works for binary str
        return s.decode('ascii') if type(s) != str else s

    @staticmethod
    def process_bars(data: np.array) -> tuple:
        if len(data) == 0:
            return '', np.array([])
        else:
            fields = data[0]
            bar = dict()
            # bar["symbol"] = fields[0].decode('ascii')
            # bar['date'] = str(int(self.tick_time(fields[1], fields[2]).timestamp())) + '000'
            # bar['open'] = fields[3]
            # bar['high'] = fields[4]
            # bar['low'] = fields[5]
            # bar['close'] = fields[6]
            # bar['volume'] = str(fields[8])
            # bar['vol'] = fields[7]
            assert fields[4] >= fields[5]
            return UpdateMongo.process_binary_symbol(fields[0]), \
                   np.array(
                       [
                           UpdateMongo.tick_time_js_timestamp(str(fields[1]), int(fields[2])),
                           # str(int(UpdateMongo.tick_time(fields[1], fields[2]).timestamp())) + '000',
                           float(fields[3]),  # open
                           float(fields[4]),  # high
                           float(fields[5]),  # low
                           float(fields[6]),  # close
                           int(fields[8]),  # volume
                       ]
                       , dtype='f8'
                   )

    def update_bars(self, data: np.array, name: str, history=False, live=False) -> None:
        col_ins = self.db.instruments
        col = self.db.bars
        symbol, ndarray = self.process_bars(data)
        if len(ndarray) == 0:
            print('empty bar')
            return
        # print('ndarray: ', ndarray)
        # symbol = dic['symbol']
        info = name.split('-')
        # assert symbol == info[0]

        instrument = col_ins.find_one({'symbol': symbol})
        update_meteor = name.startswith('auto_unwatch')
        # print(name, update_meteor)

        default_old = self.default_bar_data(symbol, name)

        # if not history:
        old = self.cache['bars'].get(symbol, default_old)
        # if len(old['bars']) == 0:
        #     old = col.find_one({'symbol': symbol})
        # else:
        #     old = self.cache['bars'].get(name, default_old)

        if len(old['bars']) > 0 and old['bars'][-1][0] == ndarray[0]:
            old['bars'][-1] = ndarray

        else:

            # new bars
            if len(old['bars']) == 0 or int(old['bars'][0][-1]) > int(ndarray[0]):
                old['bars'] = ndarray.reshape(ndarray.shape[0], -1)

            elif old['bars'][0][-1] == ndarray[0]:  # update latest bar
                for i, row in enumerate(old['bars']):
                    row[-1] = ndarray[i]
            else:  # append new bar
                old['bars'] = np.append(old['bars'], ndarray.reshape(ndarray.shape[0], -1), axis=1)

        if history:  # the history bars may come without order
            sorted(old['bars'], key=lambda item: item[0])

        old['bars'] = old['bars'][-look_back_bars:]

        # TODO create a thread here and create a process inside the thread
        # calculate the SAR and BB, to figure out if the bar crosses the buttom
        # and then close above the buttom and SAR is going up
        # if all conditions meet, put this stock into meteor
        # if not update_meteor:
        #     threading.Timer(.001, self.calculate_trend, [symbol]).start()

        # print(old['bars'])
        self.cache['bars'][symbol] = old

        # if not auto watch, calculate the indicators
        if not update_meteor and live:
            threading.Timer(.001, self.calculate_trend, [symbol, name]).start()

        if not history and (update_meteor or live):
            # old['bars'] = old['bars'].tolist()
            self.update_history_bars_after_done(symbol, name)


            # clear the cache
            # self.cache['bars'][name] = default_old
            # else:

            # for line in self.cache['bars'][symbol]['bars']:
            #     # print(line)
            # print(len(self.cache['bars'][symbol]['bars']))

            # used to update the mongo when history bars has done, but
            # no live bars are coming (in after hours)
        if dt.datetime.today().weekday() > 4 or 16 <= dt.datetime.now().hour or dt.datetime.now().hour < 5:
            # set_timeout(1, update_history_bars_after_done)
            threading.Timer(1, self.update_history_bars_after_done, [symbol, name]).start()

    # used to update the mongo when history bars has done, but
    # no live bars are coming (in after hours)
    def update_history_bars_after_done(self, symbol: str, name: str):
        col = self.db.bars
        temp = self.cache['bars'].get(symbol, self.default_bar_data(symbol, name))
        if verbose:
            print(temp)
        update_mongo_time = self.cache['update_mongo_time'].get(symbol, '0')

        # date + vol, vol is changing even if date is not changing
        current_data_time = temp['bars'][0][-1] + temp['bars'][-1][-1] if len(temp['bars']) else '0'

        if len(temp['bars']) > 0 and update_mongo_time != current_data_time:
            # temp['bars'] = temp['bars'].tolist()

            col.update_one(
                {'symbol': symbol},
                {
                    "$set": {
                        'bars': temp['bars'].tolist()
                    },
                },
                True
            )
            self.cache['update_mongo_time'][symbol] = current_data_time
            # self.cache['bars'][name] = default_old
            # threading.Timer(.001, self.calculate_trend, [symbol, name]).start()

    def calculate_trend(self, symbol, name):

        data = self.cache['bars'][symbol]['bars']
        inputs = {
            'time': data[0],
            "open": data[1],
            'high': data[2],
            'low': data[3],
            'close': data[4],
            'volume': data[5]
        }
        # print(inputs)
        # bb = BBANDS(inputs, matype=MA_Type.T3)
        # sar = SAR(inputs)

        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.bb_calculator, inputs): 'bb',
                executor.submit(self.sar_calculator, inputs): 'sar'
            }
            indicators = {}
            for future in concurrent.futures.as_completed(futures):
                # print(str(future.result())[:100])
                indicators[futures[future]] = future.result()
        rebound, result = self.rebound(indicators, inputs, symbol)
        if rebound > 2.9:
            # print(result)
            self.insert_possible_rebound_stock(symbol, name, rebound)
            for k, v in result.items():
                print('\t', k, v)

                # up_trend, result = self.up_trend(indicators, inputs, symbol)
                # if up_trend > 5:
                #     # print(result)
                #     self.insert_possible_rebound_stock(symbol, name, up_trend, 6.5, 4)
                #     for k, v in result.items():
                #         print('\t', k, v)

    def insert_possible_rebound_stock(self, symbol: str, name: str, rebound: float, good=4.5, normal=3.5):
        rank = 100000
        ins = self.db.instruments
        logs = self.db.logs
        data = self.cache['bars'][symbol]['bars']

        auto = {
            'stop_profit': 0.01,
            'stop_lose': 0.01,
            'sl': False,
            'sp': False,
            'size': 100,
            'inc': '0.01',
            'lock': {
                'lp': False,
                'buffer': '.02',
                'high': None

            }
        }
        stock = ins.find_one({{'symbol': symbol}})
        if not stock:
            stock = trader.instrument(symbol)

        def log(info):
            logs.insert({
                'type': 'log',
                'info': info + " {}".format(
                    datetime.datetime.fromtimestamp(float(str(int(data[0][-1]))[:-3])).isoformat()
                ),
                'date': datetime.datetime.utcnow()
            })

        if rebound > good:  # the best so far
            rank = -100000
        elif rebound > normal:
            rank = -10000

        if not stock or not stock['tradeable']:
            print('none tradable stock {}'.format(symbol))
            return
        elif ins.find({'symbol': symbol}).count() != 0:  # check if the stock already in the watching list
            info = '{}, rebounding {}, already watching '.format(symbol, rebound)
            print(info)
            log(info)
            self.update_history_bars_after_done(symbol, name)

        else:
            info = '{}, rebounding {}'.format(symbol, rebound)
            print(info)
            log(info)
            stock['auto'] = auto
            stock['rank'] = rank
            ins.insert(stock, True)
            self.update_history_bars_after_done(symbol, name)
        UpdateMongo.stop = 15

    @staticmethod
    def bb_calculator(sample):
        bb = BBANDS(sample, 20, 2, 2)
        return bb

    @staticmethod
    def sar_calculator(sample):
        sar = SAREXT(sample)
        return np.abs(sar)

    @staticmethod
    def up_trend(indicators: dict, inputs: dict, symbol: str) -> (float, dict):
        bb_h, bb_m, bb_l = indicators['bb']
        sar = indicators['sar']
        open = inputs['open']
        close = inputs['close']
        low = inputs['low']
        look_back = 3
        min_slope = .005
        bb_h_back = bb_h[-look_back:]
        close_back = close[-look_back:]
        open_back = open[-look_back:]
        bb_m_back = bb_m[-look_back:]
        last_bb_h_slope = st.linregress(np.arange(look_back), bb_h_back)
        last_close_slope = st.linregress(np.arange(look_back), close_back)
        above_mid_line = np.all((close_back - bb_m_back) > 0)  # all close above mid line
        above_high_line = np.all((close_back - bb_h_back) > 0)  # all close above mid line
        all_green_bar = np.all((close_back - open_back) > 0)  # all bars are green
        green_sar = np.all((sar[-look_back:] - close_back) < 0)
        up_results = {
            "last_bb_h_slope": last_bb_h_slope[0] > min_slope,
            "last_close_slope": last_close_slope[0] > min_slope,
            "above_mid_line": above_mid_line,
            "above_high_line": above_high_line,
            "all_green_bar": all_green_bar,
            'green_sar': green_sar,
        }
        result = sum(up_results.values())

        # TODO open above upper line is not a buy!!!!!!!!!!!!!!!!!!!!
        # TODO last one is red is not a buy!!!!!!!!!!!!!!!!!!!!
        # TODO the new last two slope < old slope is not a buy

        if not green_sar or not all_green_bar or last_bb_h_slope[0] < min_slope or last_close_slope[0] < min_slope:
            result = -1
        elif above_high_line and all_green_bar:
            result += 4
        elif above_high_line:
            result += 3
        elif above_mid_line and all_green_bar:
            result += 2
        elif above_mid_line:
            result += 1

        up_results['symbol'] = symbol
        up_results['last_bb_h_slope'] = last_bb_h_slope
        up_results['last_close_slope'] = last_close_slope
        up_results['time'] = datetime.datetime.fromtimestamp(float(str(int(inputs['time'][-1]))[:-3])).isoformat()

        return result, up_results

    @staticmethod
    def rebound(indicators: dict, inputs: dict, symbol: str) -> (float, dict):
        look_back = 8
        bb_h, bb_m, bb_l = indicators['bb']
        bb_l_lb = bb_l[-look_back:]
        sar = indicators['sar']
        sar_lb = sar[-look_back:]
        open = inputs['open']
        open_lb = open[-look_back:]
        close = inputs['close']
        close_lb = close[-look_back:]
        low = inputs['low']
        low_lb = low[-look_back:]
        close_above_bb_l = bb_l[-1] < close[-1]
        up_sar = sar[-1] < close[-1]
        down_sar_pre = sar[-2] > close[-2]
        cross_bb_l = np.any(bb_l_lb >= low_lb)  # if any of pre four candles lower than the bb_l
        green_bar = inputs['close'][-1] > inputs['open'][-1]

        result = {
            'symbol': symbol,
            'open': open_lb,
            'close': close_lb,
            'low': low_lb,
            'bb_low': bb_l_lb,
            'sar': sar_lb,
            "close_above_bb_l": close_above_bb_l,
            "cross_bb_l": cross_bb_l,
            "down_sar_pre": down_sar_pre,
            'up_sar': up_sar,
            "green_bar": green_bar,
            'time': datetime.datetime.fromtimestamp(float(str(int(inputs['time'][-1]))[:-3])).isoformat()
        }
        # sar rebound + cross bb b then close above
        # this is the best

        # TODO if several red before current bar, probably not a buy
        # TODO 已经在中线上方并且前方为红色 bar, not a buy
        # TODO 持续在中线上方爬升, another buy
        if green_bar:

            if close_above_bb_l and up_sar and cross_bb_l and down_sar_pre:
                return 5, result
            elif close_above_bb_l and up_sar and cross_bb_l:
                return 4, result
            elif close_above_bb_l and cross_bb_l:
                return 2, result
        return -1, result


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="5.2.6.0",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(30)

    # If you are running headless comment out the line above and uncomment
    # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
    # server since IQFeed.exe runs under wine and always wants to create a GUI
    # window.
    # svc.launch(headless=True)


class MyQuote(iq.QuoteConn):
    def __init__(self, name: str = "QuoteConn", host: str = iq.FeedConn.host,
                 port: int = iq.FeedConn.quote_port):
        super().__init__(name, host, port)

    def read_message(self):
        super()._read_messages()
        print(super()._next_message())


class MyLV2(iq.Lv2Conn):
    def __init__(self, name: str = "LV2Conn", host: str = iq.FeedConn.host,
                 port: int = iq.FeedConn.depth_port):
        super().__init__(name, host, port)

    def read_message(self):
        super()._read_messages()
        print(super()._next_message())


class MyQuoteListener(iq.SilentQuoteListener):
    def __init__(self, name: str):
        super().__init__(name)
        self.update_mongo = UpdateMongo()
        self.summary_tick_id = {}
        self.watches = set()

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if verbose:
            print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        if verbose:
            print("%s: News Item Received" % self._name)
            print(news_item)

    def process_regional_rgn_quote(self, quote: np.array) -> None:
        if verbose:
            print("%s: Regional Quote:" % self._name)
            print(quote)
        self.update_mongo.update_regional_quote(quote)

    def process_summary(self, summary: np.array) -> None:
        # if is_server():
        #     if len(summary) > 0 and len(summary[0]) > 64 and summary[0][64] != self.summary_tick_id:
        self.update_mongo.update_quote(summary, self._name)
        #         self.summary_tick_id = summary[0][64]

        if verbose:
            # print("%s: Data Summary\r" % self._name)
            # print('\r', summary)
            # for i, data in enumerate(summary[0]):
            #     print(i, data)

            summary = summary[0]
            if summary[64] != self.summary_tick_id:
                print(
                    "symbol:{}, ask{}, size:{}, bid:{} size:{} close:{}, last: "
                    "{},high:{}, ?: {} tick_vol:{}, vol:{}, tick: {}".format(
                        summary[0],
                        summary[1],
                        summary[4],
                        summary[8],
                        summary[11],
                        summary[15],
                        summary[22],
                        summary[31],
                        summary[32],
                        summary[35],
                        summary[65],
                        summary[64]
                    ))
                self.summary_tick_id = summary[64]

            pass

    def process_update(self, update: np.array) -> None:
        self.update_mongo.update_quote(update, self._name)

        if verbose:
            print("%s: Data Update" % self._name)
            print(update)

    def process_fundamentals(self, fund: np.array) -> None:
        self.update_mongo.update_fundamentals(fund)
        if verbose:
            print("%s: Fundamentals Received:" % self._name)
            print(fund[0].tolist(), type(fund[0].tolist()[0]))
            fund = fund[0].tolist()
            # for i, info in enumerate(fund):
            #     if type(info) == bytes:
            #         info = UpdateMongo.process_binary_symbol(info)
            #     print(QuoteConn.fundamental_keys[i], info)

            pass

    def process_auth_key(self, key: str) -> None:
        if verbose:
            print("%s: Authorization Key Received: %s" % (self._name, key))

    def process_keyok(self) -> None:
        if verbose:
            print("%s: Authorization Key OK" % self._name)

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        if verbose:
            print("%s: Customer Information:" % self._name)
            print(cust_info)

    def process_watched_symbols(self, symbols: Sequence[str]):
        # print(symbols)
        self.watches.clear()
        self.watches = set(symbols)
        if verbose:
            print("%s: List of subscribed symbols:" % self._name)
            print(symbols)

    def process_log_levels(self, levels: Sequence[str]) -> None:
        if verbose:
            print("%s: Active Log levels:" % self._name)
            print(levels)

    def process_symbol_limit_reached(self, sym: str) -> None:
        if verbose:
            print("%s: Symbol Limit Reached with subscription to %s" %
                  (self._name, sym))

    def process_ip_addresses_used(self, ip: str) -> None:
        if verbose:
            print("%s: IP Addresses Used: %s" % (self._name, ip))


class IndexWatcher(MyQuoteListener):
    symbol_map = {
        'SPX.XO': '^GSPC',
        'VIX.XO': '^VIX',
    }

    def __init__(self, name: str):
        super().__init__(name)
        self.update_mongo = UpdateMongo()
        self.summary_tick_id = {}
        self.watches = set()

    def process_summary(self, summary: np.array) -> None:
        # if is_server():
        #     if len(summary) > 0 and len(summary[0]) > 64 and summary[0][64] != self.summary_tick_id:
        self.update_mongo.update_index(summary, self._name)
        #         self.summary_tick_id = summary[0][64]

        if verbose:
            # print("%s: Data Summary\r" % self._name)
            # print('\r', summary)
            # for i, data in enumerate(summary[0]):
            #     print(i, data)

            summary = summary[0]
            if summary[64] != self.summary_tick_id:
                print(
                    "symbol:{}, ask{}, size:{}, bid:{} size:{} close:{}, last: "
                    "{},high:{}, ?: {} tick_vol:{}, vol:{}, tick: {}".format(
                        summary[0],
                        summary[1],
                        summary[4],
                        summary[8],
                        summary[11],
                        summary[15],
                        summary[22],
                        summary[31],
                        summary[32],
                        summary[35],
                        summary[65],
                        summary[64]
                    ))
                self.summary_tick_id = summary[64]

            pass

    def process_update(self, update: np.array) -> None:
        self.update_mongo.update_index(update, self._name)

        if verbose:
            print("%s: Data Update" % self._name)
            print(update)


history_cache = {}


class LV2Listener(MyQuoteListener):
    def process_summary(self, summary: np.array) -> None:
        summary = summary[0]
        self.update_mongo.update_lv2(summary, self._name)

        # if is_server():
        #     if len(summary) > 0 and len(summary[0]) > 64 and summary[0][64] != self.summary_tick_id:
        # self.update_mongo.update_quote(summary, self._name)
        #         self.summary_tick_id = summary[0][64]
        self.watches.add(summary['symbol'])

        if verbose:
            # print("%s: Data Summary\r" % self._name)
            # print('\r', summary)
            # for i, data in enumerate(summary[0]):
            #     print(i, data)
            print("%s: LV2 Data Summary" % self._name)
            print(summary)
            # if summary[64] != self.summary_tick_id:
            #     print(
            #         "symbol:{}, ask{}, size:{}, bid:{} size:{} close:{}, last: "
            #         "{},high:{}, ?: {} tick_vol:{}, vol:{}, tick: {}".format(
            #             summary[0],
            #             summary[1],
            #             summary[4],
            #             summary[8],
            #             summary[11],
            #             summary[15],
            #             summary[22],
            #             summary[31],
            #             summary[32],
            #             summary[35],
            #             summary[65],
            #             summary[64]
            #         ))
            # self.summary_tick_id = summary[64]

    def process_update(self, update: np.array) -> None:
        summary = update[0]
        self.update_mongo.update_lv2(summary, self._name)
        self.watches.add(summary['symbol'])

        # self.update_mongo.update_quote(update, self._name)

        if verbose:
            print("%s: LV2 Data Update" % self._name)
            print(update)


# noinspection PyMethodMayBeStatic,PyMissingOrEmptyDocstring
class MyBarListener(VerboseBarListener):
    """
    Verbose version of SilentBarListener.

    See documentation for SilentBarListener member functions.

    """

    def feed_is_stale(self) -> None:
        super().feed_is_stale()
        relaunch_service()

    def __init__(self, name: str):
        super().__init__(name)
        self.watchers = {}
        self.update_mongo = UpdateMongo()

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        # print("%s: Process latest bar update:" % self._name)
        # print(bar_data)
        self.update_mongo.update_bars(bar_data, name=self._name)
        data = bar_data[0]
        if verbose:
            print("%s: Process latest bar update:" % self._name)
            print(UpdateMongo.tick_time(data[1], data[2]), UpdateMongo.tick_time(data[1], data[2]).timestamp(), data)

    def process_live_bar(self, bar_data: np.array) -> None:
        self.update_mongo.update_bars(bar_data, name=self._name, live=True)
        if verbose:
            print("%s: Process live bar:" % self._name)
            # print(bar_data)
            data = bar_data[0]
            print(UpdateMongo.tick_time(data[1], data[2]), UpdateMongo.tick_time(data[1], data[2]).timestamp(), data)

    def process_history_bar(self, bar_data: np.array) -> None:
        if verbose:
            print("%s: Process history bar:" % self._name)
        # print(bar_data)
        # data = bar_data[0]
        # key = "{}:{}:{}".format(data[0], data[1], data[2])
        # if key not in history_cache or (key in history_cache and history_cache[key] != data[2]):
        # print(UpdateMongo.tick_time(data[1], data[2]), UpdateMongo.tick_time(data[1], data[2]).timestamp(), data)
        self.update_mongo.update_bars(bar_data, name=self._name, history=True)
        #     history_cache[key] = data[2]
        # else:
        #     if  verbose:
        #         # print(UpdateMongo.tick_time(data[1], data[2]), UpdateMongo.tick_time(data[1], data[2]).timestamp(), data)
        #         print('in cache')

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if verbose:
            print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_symbol_limit_reached(self, symbol: str) -> None:
        if verbose:
            print("%s: Symbol Limit reached: %s" % (self._name, symbol))

    def process_replaced_previous_watch(self, symbol: str) -> None:
        if verbose:
            print("%s: Replaced previous watch: %s" % (self._name, symbol))

    def process_watch(self, symbol: str, interval: int, request_id: str):
        self.watchers['{}:{}'.format(symbol, interval)] = request_id
        if verbose:
            print("%s: Process watch: %s, %s, %s" %
                  (self._name, symbol, interval, request_id))

    def get_request_id_sec(self, symbol: str, interval: int):
        return self.watchers.get('{}:{}sec'.format(symbol, interval), None)


def get_level_1_quotes_and_trades(ticker: str, seconds: int, auto_unwatch=True):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = MyQuote(name="{} pyiqfeed-lvl1".format(ticker))
    quote_listener = MyQuoteListener("{} Level 1 Listener".format(ticker))
    quote_conn.add_listener(quote_listener)
    print('get_level_1_quotes_and_trades ' + ticker)
    mongo_conn = UpdateMongo()
    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        quote_conn.watch(ticker)
        # quote_conn.watch('NVDA')
        # quote_conn.regional_watch(ticker)
        # quote_conn.regional_watch('NVDA')
        # print(update_mongo.get_symbols())
        # for symbol in update_mongo.get_symbols():
        #     print(symbol, end=', ')
        #     quote_conn.watch(symbol)
        #     # quote_conn.regional_watch(symbol)

        quote_conn.news_on()
        time.sleep(5)
        quote_conn.request_watches()
        quote_conn.unwatch(ticker)
        time.sleep(5)
        quote_conn.request_watches()

        while quote_conn.reader_running():
            # quote_conn.request_stats()
            # try:
            # quote_conn.refresh(ticker)
            # for symbol in update_mongo.get_symbols():
            #     # print(symbol)
            #     quote_conn.refresh(symbol)

            # except Exception as e:
            #     print(e)
            # quote_conn.
            # quote_conn.read_message()

            time.sleep(3)
            stocks = mongo_conn.get_symbols()
            if auto_unwatch and ticker in stocks and not stocks[ticker]['auto'].get('lv1', 0):
                print('unwatch lv1', ticker)
                break

        quote_conn.unwatch(ticker)
        quote_conn.remove_listener(quote_listener)


def get_level_1_multi_quotes_and_trades(tickers: dict, seconds: int, auto_unwatch=True, listener=None):
    """Get level 1 quotes and trades for ticker for seconds seconds."""
    if not listener:
        listener = MyQuoteListener
    quote_conn = MyQuote(name="{} pyiqfeed-lvl1".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    quote_listener = listener("{} Level 1 Listener".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    quote_conn.add_listener(quote_listener)
    print('get_level_1_quotes_and_trades ' + ('auto_unwatch' if auto_unwatch else 'auto_trade'))

    mongo_conn = UpdateMongo()
    if auto_unwatch:
        tickers = mongo_conn.get_symbols()

    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        i = 0
        for ticker in tickers.keys():
            if auto_unwatch and not tickers[ticker]['auto'].get('lv1', 0):
                continue

            quote_conn.watch(ticker)
            if i % 20 == 0:
                time.sleep(3)
            i += 1
        quote_conn.request_watches()
        time.sleep(seconds)

        # quote_conn.watch('NVDA')
        # quote_conn.regional_watch(ticker)
        # quote_conn.regional_watch('NVDA')
        # print(update_mongo.get_symbols())
        # for symbol in update_mongo.get_symbols():
        #     print(symbol, end=', ')
        #     quote_conn.watch(symbol)
        #     # quote_conn.regional_watch(symbol)

        quote_conn.news_on()

        while quote_conn.reader_running():
            # quote_conn.request_stats()
            # try:
            # quote_conn.refresh(ticker)
            # for symbol in update_mongo.get_symbols():
            #     # print(symbol)
            #     quote_conn.refresh(symbol)

            # except Exception as e:
            #     print(e)
            # quote_conn.
            # quote_conn.read_message()
            if auto_unwatch:
                stocks = mongo_conn.get_symbols()
                # print(quote_listener.watches)
                for stock in stocks.keys():

                    if not stocks[stock]['auto'].get('lv1', 0) and stock in quote_listener.watches:
                        quote_conn.unwatch(stock)
                        print('unwatch lv1 ', stock)
                    elif stocks[stock]['auto'].get('lv1', 0) and stock not in quote_listener.watches:
                        quote_conn.watch(stock)
                        print('watch lv1 ', stock)

            quote_conn.request_watches()
            time.sleep(seconds)

        for stock in mongo_conn.get_symbols().keys():
            quote_conn.unwatch(stock)
        quote_conn.remove_listener(quote_listener)


def get_level_2_multi_quotes_and_trades(tickers: dict, seconds: int, auto_unwatch=True, listener=None):
    """Get level 1 quotes and trades for ticker for seconds seconds."""
    if not listener:
        listener = LV2Listener
    lv2_conn = MyLV2(name="{} pyiqfeed-lvl2".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    lv2_listener = listener("{} Level 2 Listener".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    lv2_conn.add_listener(lv2_listener)
    # print('get_level_2_quotes_and_trades ' + ('auto_unwatch' if auto_unwatch else 'auto_trade'))

    mongo_conn = UpdateMongo()
    if auto_unwatch:
        tickers = mongo_conn.get_symbols()

    def remove_watch(symbol: str):
        time.sleep(.5)
        lv2_listener.watches.remove(symbol)

    with iq.ConnConnector([lv2_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        lv2_conn.select_update_fieldnames(all_fields)
        i = 0
        for ticker in tickers.keys():
            if auto_unwatch and not tickers[ticker]['auto'].get('lv2', 0):
                continue

            lv2_conn.watch(ticker)
            if i % 20 == 0:
                time.sleep(3)
            i += 1
        lv2_conn.request_watches()
        time.sleep(seconds)
        while lv2_conn.reader_running():
            if auto_unwatch:
                stocks = mongo_conn.get_symbols()
                # print(lv2_listener.watches)
                for stock in stocks.keys():

                    if not stocks[stock]['auto'].get('lv2', 0) and stock in lv2_listener.watches:
                        lv2_conn.unwatch(stock)
                        print('unwatch lv2 ', stock)
                        remove_watch(stock)
                    elif stocks[stock]['auto'].get('lv2', 0) and stock not in lv2_listener.watches:
                        lv2_conn.watch(stock)
                        print('watch lv2 ', stock)

            # lv2_conn.request_watches()
            time.sleep(seconds)

        for stock in mongo_conn.get_symbols().keys():
            lv2_conn.unwatch(stock)
        lv2_conn.remove_listener(lv2_listener)


def get_regional_quotes(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-regional")
    quote_listener = MyBarListener("Regional Listener")
    quote_conn.add_listener(quote_listener)

    with iq.ConnConnector([quote_conn]) as connector:
        quote_conn.regional_watch(ticker)
        time.sleep(seconds)
        quote_conn.regional_unwatch(ticker)


def get_trades_only(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-trades-only")
    quote_listener = iq.VerboseQuoteListener("Trades Listener")
    quote_conn.add_listener(quote_listener)

    with iq.ConnConnector([quote_conn]) as connector:
        quote_conn.trades_watch(ticker)
        time.sleep(seconds)
        quote_conn.unwatch(ticker)


def get_live_interval_bars(ticker: str, bar_len: int, seconds: int, auto_unwatch=True):
    """Get real-time interval bars"""
    bar_conn = iq.BarConn(name='{} pyiqfeed-Example-interval-bars'.format(ticker))
    bar_listener = MyBarListener("{}-{}-bar-listener".format(ticker, bar_len))
    bar_conn.add_listener(bar_listener)
    print('get_live_interval_bars {}@{}'.format(ticker, bar_len))
    mongo_conn = UpdateMongo()

    with iq.ConnConnector([bar_conn]) as connector:
        bar_conn.watch(symbol=ticker, interval_len=bar_len,
                       interval_type='s', update=1, lookback_bars=look_back_bars)
        while 1:
            stocks = mongo_conn.get_symbols()
            if auto_unwatch and ticker in stocks and \
                    (not stocks[ticker]['auto'].get('chart', 0)
                     or stocks[ticker]['auto'].get('chart_inv', 0) != bar_len):
                bar_conn.unwatch(ticker)
                bar_conn.remove_listener(bar_listener)
                print('unwatch bar', ticker, bar_len)
                return
            time.sleep(seconds)


def get_live_multi_interval_bars(tickers: dict, bar_len: int, seconds: int, auto_unwatch=True):
    """Get real-time interval bars"""
    bar_conn = iq.BarConn(
        name='{} pyiqfeed-Example-interval-bars'.format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    bar_listener = MyBarListener("{}-bar-listener".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    bar_conn.add_listener(bar_listener)
    # print('get_live_interval_bars {}@{}'.format(str(tickers), bar_len))
    watching = {

    }
    mongo_conn = UpdateMongo()
    if auto_unwatch:
        tickers = mongo_conn.get_symbols()

    with iq.ConnConnector([bar_conn]) as connector:
        i = 0
        for ticker in tickers.keys():
            if tickers[ticker]['auto'].get('chart', 0):
                inv = tickers[ticker]['auto'].get('chart_inv', 30)
                bar_conn.watch(symbol=ticker,
                               interval_len=inv,
                               interval_type='s', update=1, lookback_bars=look_back_bars)
                watching[ticker] = inv
                print('watching {}@{}'.format(ticker, inv))
            if i % 20 == 0:
                time.sleep(3)
            i += 1
        bar_conn.request_watches()
        while 1:
            if auto_unwatch:
                new_tickers = mongo_conn.get_symbols()

                for ticker in new_tickers.keys():
                    new_inv = new_tickers[ticker]['auto'].get('chart_inv', 0)
                    old_inv = watching.get(ticker, 0)
                    watch = new_tickers[ticker]['auto'].get('chart', 0)
                    request_id = bar_listener.get_request_id_sec(ticker, old_inv)
                    if not watch and ticker in watching:
                        print('unwatch {}@{}'.format(ticker, old_inv))

                        bar_conn.unwatch(ticker, request_id)
                        # bar_conn.unwatch_all()
                        del watching[ticker]
                        continue

                    if not new_inv or new_inv != old_inv:
                        if ticker in watching:
                            print('unwatch {}@{}'.format(ticker, old_inv))
                            bar_conn.unwatch(ticker, request_id)
                            del watching[ticker]
                        if watch and new_inv != 0:
                            time.sleep(.5)
                            bar_conn.watch(symbol=ticker,
                                           interval_len=new_inv,
                                           interval_type='s', update=1, lookback_bars=look_back_bars)
                            print('watching {}@{}'.format(ticker, new_inv))
                            watching[ticker] = new_inv
            bar_conn.request_watches()
            time.sleep(seconds)


# def unwatch_live_interval_bar

def get_administrative_messages(seconds: int):
    """Run and AdminConn and print connection stats to screen."""

    admin_conn = iq.AdminConn(name="pyiqfeed-Example-admin-messages")
    admin_listener = iq.SilentAdminListener("Admin Listener")
    admin_conn.add_listener(admin_listener)

    with iq.ConnConnector([admin_conn]) as connector:
        admin_conn.set_admin_variables(product=dtn_product_id,
                                       login=dtn_login,
                                       password=dtn_password,
                                       autoconnect=True)
        admin_conn.client_stats_on()
        time.sleep(seconds)


def get_tickdata(ticker: str, max_ticks: int, num_days: int):
    """Show how to read tick-data"""

    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-tickdata")
    hist_listener = iq.VerboseIQFeedListener("History Tick Listener")
    hist_conn.add_listener(hist_listener)

    # Look at conn.py for request_ticks, request_ticks_for_days and
    # request_ticks_in_period to see various ways to specify time periods
    # etc.

    with iq.ConnConnector([hist_conn]) as connector:
        # Get the last 10 trades
        try:
            tick_data = hist_conn.request_ticks(ticker=ticker,
                                                max_ticks=max_ticks)
            print(tick_data)

            # Get the last num_days days trades between 10AM and 12AM
            # Limit to max_ticks ticks else too much will be printed on screen
            bgn_flt = datetime.time(hour=10, minute=0, second=0)
            end_flt = datetime.time(hour=12, minute=0, second=0)
            tick_data = hist_conn.request_ticks_for_days(ticker=ticker,
                                                         num_days=num_days,
                                                         bgn_flt=bgn_flt,
                                                         end_flt=end_flt,
                                                         max_ticks=max_ticks)
            print(tick_data)

            # Get all ticks between 9:30AM 5 days ago and 9:30AM today
            # Limit to max_ticks since otherwise too much will be printed on
            # screen
            today = datetime.date.today()
            sdt = today - datetime.timedelta(days=5)
            start_tm = datetime.datetime(year=sdt.year,
                                         month=sdt.month,
                                         day=sdt.day,
                                         hour=9,
                                         minute=30)
            edt = today
            end_tm = datetime.datetime(year=edt.year,
                                       month=edt.month,
                                       day=edt.day,
                                       hour=9,
                                       minute=30)

            tick_data = hist_conn.request_ticks_in_period(ticker=ticker,
                                                          bgn_prd=start_tm,
                                                          end_prd=end_tm,
                                                          max_ticks=max_ticks)
            print(tick_data)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_historical_bar_data(ticker: str, bar_len: int, bar_unit: str,
                            num_bars: int):
    """Shows how to get interval bars."""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-historical-bars")
    hist_listener = MyBarListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        # look at conn.py for request_bars, request_bars_for_days and
        # request_bars_in_period for other ways to specify time periods etc
        try:
            bars = hist_conn.request_bars(ticker=ticker,
                                          interval_len=bar_len,
                                          interval_type=bar_unit,
                                          max_bars=num_bars)
            print(bars)

            today = datetime.date.today()
            start_date = today - datetime.timedelta(days=10)
            start_time = datetime.datetime(year=start_date.year,
                                           month=start_date.month,
                                           day=start_date.day,
                                           hour=0,
                                           minute=0,
                                           second=0)
            end_time = datetime.datetime(year=today.year,
                                         month=today.month,
                                         day=today.day,
                                         hour=23,
                                         minute=59,
                                         second=59)
            bars = hist_conn.request_bars_in_period(ticker=ticker,
                                                    interval_len=bar_len,
                                                    interval_type=bar_unit,
                                                    bgn_prd=start_time,
                                                    end_prd=end_time)
            print(len(bars))
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_daily_data(ticker: str, num_days: int):
    """Historical Daily Data"""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-daily-data")
    hist_listener = iq.VerboseBarListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        try:
            daily_data = hist_conn.request_daily_data(ticker, num_days)
            print(daily_data)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_reference_data():
    """Markets, SecTypes, Trade Conditions etc"""
    table_conn = iq.TableConn(name="pyiqfeed-Example-reference-data")
    table_listener = MyBarListener("Reference Data Listener")
    table_conn.add_listener(table_listener)
    with iq.ConnConnector([table_conn]) as connector:
        table_conn.update_tables()
        print("Markets:")
        print(table_conn.get_markets())
        print("")

        print("Security Types:")
        print(table_conn.get_security_types())
        print("")

        print("Trade Conditions:")
        print(table_conn.get_trade_conditions())
        print("")

        print("SIC Codes:")
        print(table_conn.get_sic_codes())
        print("")

        print("NAIC Codes:")
        print(table_conn.get_naic_codes())
        print("")
        table_conn.remove_listener(table_listener)


def get_ticker_lookups(ticker: str):
    """Lookup tickers."""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Ticker-Lookups")
    lookup_listener = MyBarListener("TickerLookupListener")
    lookup_conn.add_listener(lookup_listener)

    with iq.ConnConnector([lookup_conn]) as connector:
        syms = lookup_conn.request_symbols_by_filter(
            search_term=ticker, search_field='s')
        print("Symbols with %s in them" % ticker)
        print(syms)
        print("")

        sic_symbols = lookup_conn.request_symbols_by_sic(83)
        print("Symbols in SIC 83:")
        print(sic_symbols)
        print("")

        naic_symbols = lookup_conn.request_symbols_by_naic(10)
        print("Symbols in NAIC 10:")
        print(naic_symbols)
        print("")
        lookup_conn.remove_listener(lookup_listener)


def get_equity_option_chain(ticker: str):
    """Equity Option Chains"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Eq-Option-Chain")
    lookup_listener = MyBarListener("EqOptionListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        # noinspection PyArgumentEqualDefault
        e_opt = lookup_conn.request_equity_option_chain(
            symbol=ticker,
            opt_type='pc',
            month_codes="".join(iq.LookupConn.call_month_letters +
                                iq.LookupConn.put_month_letters),
            near_months=None,
            include_binary=True,
            filt_type=0, filt_val_1=None, filt_val_2=None)
        print("Currently trading options for %s" % ticker)
        print(e_opt)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_chain(ticker: str):
    """Futures chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Chain")
    lookup_listener = MyBarListener("FuturesChainLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.futures_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_spread_chain(ticker: str):
    """Futures spread chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Spread-Lookup")
    lookup_listener = MyBarListener("FuturesSpreadLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_spread_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.futures_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures Spread symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_options_chain(ticker: str):
    """Futures Option Chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Options-Chain")
    lookup_listener = MyBarListener("FuturesOptionLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_option_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.call_month_letters +
                                iq.LookupConn.put_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures Option symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_news():
    """Exercise NewsConn functionality"""
    news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
    news_listener = MyBarListener("NewsListener")
    news_conn.add_listener(news_listener)

    with iq.ConnConnector([news_conn]) as connector:
        cfg = news_conn.request_news_config()
        print("News Configuration:")
        print(cfg)
        print("")

        print("Latest 10 headlines:")
        headlines = news_conn.request_news_headlines(
            sources=[], symbols=[], date=None, limit=10)
        print(headlines)
        print("")

        story_id = headlines[0].story_id
        story = news_conn.request_news_story(story_id)
        print("Text of story with story id: %s:" % story_id)
        print(story.story)
        print("")

        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)

        counts = news_conn.request_story_counts(
            symbols=["AAPL", "IBM", "TSLA"],
            bgn_dt=week_ago, end_dt=today)
        print("Number of news stories in last week for AAPL, IBM and TSLA:")
        print(counts)
        print("")


def combine_name(p: str, n: str) -> str:
    return "{}:{}".format(p, n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pyiqfeed example code")
    parser.add_argument('-l', action="store_true", dest='level_1',
                        help="Run Level 1 Quotes")
    parser.add_argument('-r', action="store_true", dest='regional_quotes',
                        help="Run Regional Quotes")
    parser.add_argument('-t', action="store_true", dest='trade_updates',
                        help="Run Trades Only Quotes")
    parser.add_argument('-i', action="store_true", dest='interval_data',
                        help="Run interval data")
    parser.add_argument("-a", action='store_true', dest='admin_socket',
                        help="Run Administrative Connection")
    parser.add_argument("-k", action='store_true', dest='historical_tickdata',
                        help="Get historical tickdata")
    parser.add_argument("-b", action='store_true', dest='historical_bars',
                        help="Get historical bar-data")
    parser.add_argument("-d", action='store_true', dest='historical_daily_data',
                        help="Get historical daily data")
    parser.add_argument("-f", action='store_true', dest='reference_data',
                        help="Get reference data")
    parser.add_argument("-c", action='store_true', dest='lookups_and_chains',
                        help="Lookups and Chains")
    parser.add_argument("-n", action='store_true', dest='news',
                        help="News related stuff")
    results = parser.parse_args()

    launch_service()
    # print(results)
    # get_level_1_quotes_and_trades(ticker="AMD", seconds=1)
    # get_tickdata(ticker="AMD", max_ticks=10000, num_days=4)
    # get_historical_bar_data(ticker="AMD",
    #                             bar_len=60,
    #                             bar_unit='s',
    #                             num_bars=100)
    # get_daily_data(ticker="AMD", num_days=10)
    get_live_interval_bars(ticker="AMD", bar_len=600, seconds=5)

    # if results.level_1:
    #     print(get_level_1_quotes_and_trades(ticker="AMD", seconds=30))
    # if results.regional_quotes:
    #     get_regional_quotes(ticker="AMD", seconds=120)
    # if results.trade_updates:
    #     get_trades_only(ticker="AMD", seconds=30)
    # if results.interval_data:
    #     get_live_interval_bars(ticker="AMD", bar_len=5, seconds=30)
    # if results.admin_socket:
    #     get_administrative_messages(seconds=30)
    # if results.historical_tickdata:
    #     get_tickdata(ticker="AMD", max_ticks=100, num_days=4)
    # if results.historical_bars:
    #     get_historical_bar_data(ticker="AMD",
    #                             bar_len=60,
    #                             bar_unit='s',
    #                             num_bars=100)
    # if results.historical_daily_data:
    #     get_daily_data(ticker="AMD", num_days=10)
    # if results.reference_data:
    #     get_reference_data()
    # if results.lookups_and_chains:
    #     get_ticker_lookups("SPH9GBM1")
    #     get_equity_option_chain("AMD")
    #     get_futures_chain("@VX")
    #     get_futures_spread_chain("@VX")
    #     get_futures_options_chain("CL")
    # if results.news:
    #     get_news()
