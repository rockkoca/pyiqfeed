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
import numpy as np
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

import robinhood.Robinhood as RB
from robinhood.credentials import *

verbose = 0
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


class UpdateMongo(object):
    def __init__(self):
        self.cache = {
            'bars': {},
            'update_mongo_time': {},
            'trend': {}
        }
        if sys.platform == 'darwin':
            self.client = MongoClient("mongodb://localhost:3001")
            self.db = self.client.meteor

        else:
            self.client = MongoClient("mongodb://localhost:27017")
            self.db = self.client.stock

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

    @staticmethod
    def _process_regional_quote(data: np.array) -> dict:
        if len(data) == 0:
            return {}
        else:
            fields = data[0]
            rgn_quote = dict()
            rgn_quote["symbol"] = fields[0].decode('ascii')
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
    def _process_quote(data: np.array) -> dict:
        if len(data) == 0:
            return {}
        else:
            fields = data[0]
            rgn_quote = dict()
            # print(fields)
            rgn_quote["symbol"] = fields[0].decode('ascii')
            rgn_quote["ask_price"] = str(fields[1])
            rgn_quote["ask_size"] = int(fields[4])
            rgn_quote["bid_price"] = str(fields[8])
            rgn_quote["bid_size"] = int(fields[11])
            rgn_quote['close'] = str(fields[15])
            rgn_quote['last'] = str(fields[22])
            rgn_quote['high'] = str(fields[31])
            rgn_quote['low'] = str(fields[37])
            rgn_quote['tick_vol'] = int(fields[35])
            rgn_quote['volume'] = int(fields[65])
            rgn_quote['tick'] = int(fields[64])

            dt = iq.field_readers.read_ccyymmdd(str(fields[43]).replace('-', ''))
            dt = iq.field_readers.date_us_to_datetime(dt, int(fields[36]))
            rgn_quote['tick_time'] = str(int(np.floor(dt.timestamp()))) + '000'
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
        dt = iq.field_readers.read_ccyymmdd(str(yymmdd).replace('-', ''))
        dt = iq.field_readers.date_us_to_datetime(dt, int(us))
        return dt

    def update_quote(self, data: np.array, name: str) -> None:
        col = self.db.quotes
        dic = self._process_quote(data)
        symbol = dic['symbol']
        update_meteor = name.startswith('auto_unwatch')
        if symbol == 'TOPS':
            return
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
            if 'low' not in new_dic:
                print(old, dic)

            if new_dic['tick'] == old.get('tick', 0):
                return
            # if update_meteor
            result = col.update_one(
                {'symbol': new_dic['symbol']},
                {
                    "$set": new_dic,
                },
                True
            )
            # print(result)

    def _process_bars(self, data: np.array) -> tuple:
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
            return fields[0].decode('ascii'), \
                   np.array(
                       [
                           str(int(self.tick_time(fields[1], fields[2]).timestamp())) + '000',
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
        symbol, ndarray = self._process_bars(data)
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

        if not history and update_meteor:
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
        if rebound > 1.9:
            # print(result)
            for k, v in result.items():
                print(k, v)
            self.insert_possible_rebound_stock(symbol, name, rebound)

    def insert_possible_rebound_stock(self, symbol: str, name: str, rebound: float):
        rank = 100000
        ins = self.db.instruments
        logs = self.db.logs
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
        stock = trader.instrument(symbol)

        def log(info):
            logs.insert({
                'type': 'log',
                'info': info + " {}".format(time.strftime("%H:%M:%S", time.localtime())),
                'date': datetime.datetime.utcnow()
            })

        if rebound > 4.5:  # the best so far
            rank = -100000
        elif rebound > 3.5:
            rank = -10000

        if not stock or not stock['tradeable']:
            print('none tradable stock {}'.format(symbol))
            return
        elif ins.find({'symbol': symbol}):  # check if the stock already in the watching list
            info = '{}, rebounding {}, already watching '.format(symbol, rebound)
            print(info)
            log(info)
        else:
            info = '{}, rebounding {}'.format(symbol, rebound)
            print(info)
            log(info)
            stock['auto'] = auto
            stock['rank'] = rank
            ins.insert(stock, True)
            self.update_history_bars_after_done(symbol, name)

    @staticmethod
    def bb_calculator(sample):
        bb = BBANDS(sample, timeperiod=20, matype=MA_Type.T3)
        return bb

    @staticmethod
    def sar_calculator(sample):
        sar = SAR(sample)
        return sar

    @staticmethod
    def rebound(indicators: dict, inputs: dict, symbol: str) -> (float, dict):
        bb = indicators['bb']
        sar = indicators['sar']
        open = inputs['open']
        close = inputs['close']
        low = inputs['low']
        close_above_bb_l = bb[-1][-1] < close[-1]
        up_sar = sar[-1] < close[-1]
        down_sar_pre = sar[-2] > close[-2]
        cross_bb_l = False
        green_bar = inputs['close'][-1] > inputs['open'][-1]

        # check if cross bb b before last one
        for i in range(-2, -6, -1):
            if bb[-1][i] > low[i]:
                cross_bb_l = True
                break
        result = {
            'symbol': symbol,
            'open': open[-1],
            'close': close[-1],
            'bb_low': bb[-1][-5:],
            'sar': sar[-5:],
            "close_above_bb_l": close_above_bb_l,
            "cross_bb_l": cross_bb_l,
            "down_sar_pre": down_sar_pre,
            "green_bar": green_bar,

        }
        # sar rebound + cross bb b then close above
        # this is the best
        if green_bar:
            # print(symbol, end=": ")
            # print("close_above_bb_l:{} up_sar:{} cross_bb_l:{} down_sar_pre:{} green_bar:{} len_sar: {}"
            #       .format(close_above_bb_l, up_sar, cross_bb_l, down_sar_pre, green_bar, len(sar)))
            # print('\tclose: {}\n\topen:{}\n\tbb_low: {}\n\tsar: {}\n\tlen_data: {}'
            #       .format(close[-1], open[-1], bb[-1][-5:], sar[-5:], len(open)))

            if close_above_bb_l and up_sar and cross_bb_l and down_sar_pre:
                return 5, result
            elif close_above_bb_l and up_sar and cross_bb_l:
                return 5, result
            elif close_above_bb_l and cross_bb_l:
                return 5, result
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


class MyQuoteListener(iq.SilentQuoteListener):
    def __init__(self, name: str):
        super().__init__(name)
        self.update_mongo = UpdateMongo()
        self.summary_tick_id = {}
        self.watches = {}

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
        if verbose:
            # print("%s: Fundamentals Received:" % self._name)
            # print(fund)
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


history_cache = {}


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
        data = bar_data[0]
        key = "{}:{}:{}".format(data[0], data[1], data[2])
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


def get_level_1_multi_quotes_and_trades(tickers: dict, seconds: int, auto_unwatch=True):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = MyQuote(name="{} pyiqfeed-lvl1".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    quote_listener = MyQuoteListener("{} Level 1 Listener".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
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
