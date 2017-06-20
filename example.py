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
import datetime
import pyiqfeed as iq
import numpy as np
from typing import Sequence
import time
from passwords import dtn_product_id, dtn_login, dtn_password
from pyiqfeed import *
from pymongo import MongoClient


def is_server() -> bool:
    return sys.platform != 'darwin'


if sys.platform == 'darwin':
    client = MongoClient("mongodb://localhost:3001")
    db = client.meteor

else:
    client = MongoClient("mongodb://localhost:27017")
    db = client.stock


class UpdateMongo(object):
    def __init__(self):
        pass

    def get_symbols(self) -> list:
        symbols = []
        col = db.instruments
        resultss = col.find()
        # print(resultss, 'symbols')
        for result in resultss:
            # print(result)
            symbols.append(result['symbol'])
        # print(symbols)
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
        col = db.quotes
        dic = self._process_regional_quote(data)
        if dic:
            # print(dic)
            keys = list(dic.keys())
            new_dic = {}
            for key in keys:
                if dic[key] != 'nan' and dic[key]:
                    new_dic[key] = dic[key]

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
            rgn_quote['tick_vol'] = int(fields[31])
            rgn_quote['volume'] = int(fields[31])
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

    def update_quote(self, data: np.array) -> None:
        col = db.quotes
        dic = self._process_quote(data)
        if dic:
            # print(dic)
            keys = list(dic.keys())
            new_dic = {}
            for key in keys:
                if dic[key] != 'nan' and dic[key]:
                    new_dic[key] = dic[key]

            result = col.update_one(
                {'symbol': new_dic['symbol']},
                {
                    "$set": new_dic,
                },
                True
            )
            # print(result)




update_mongo = UpdateMongo()


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="5.2.6.0",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch()

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
        self.summary_tick_id = 0

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        if not is_server():
            print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        if not is_server():
            print("%s: News Item Received" % self._name)
            print(news_item)

    def process_regional_rgn_quote(self, quote: np.array) -> None:
        if not is_server():
            print("%s: Regional Quote:" % self._name)
            print(quote)
        self.update_mongo.update_regional_quote(quote)

    def process_summary(self, summary: np.array) -> None:
        self.update_mongo.update_quote(summary)

        if not is_server():
            # print("%s: Data Summary\r" % self._name)
            # print('\r', summary)
            # for i, data in enumerate(summary[0]):
            #     print(i, data)

            summary = summary[0]
            if summary[64] != self.summary_tick_id:
                print(
                    "symbol:{}, ask{}, size:{}, bid:{} size:{} close:{}, last: {},high:{}, ?: {} tick_vol:{}, vol:{}, tick: {}".format(
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
        if not is_server():
            print("%s: Data Update" % self._name)
            print(update)

    def process_fundamentals(self, fund: np.array) -> None:
        if not is_server():
            # print("%s: Fundamentals Received:" % self._name)
            # print(fund)
            pass

    def process_auth_key(self, key: str) -> None:
        if not is_server():
            print("%s: Authorization Key Received: %s" % (self._name, key))

    def process_keyok(self) -> None:
        if not is_server():
            print("%s: Authorization Key OK" % self._name)

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        if not is_server():
            print("%s: Customer Information:" % self._name)
            print(cust_info)

    def process_watched_symbols(self, symbols: Sequence[str]):
        if not is_server():
            print("%s: List of subscribed symbols:" % self._name)
            print(symbols)

    def process_log_levels(self, levels: Sequence[str]) -> None:
        if not is_server():
            print("%s: Active Log levels:" % self._name)
            print(levels)

    def process_symbol_limit_reached(self, sym: str) -> None:
        if not is_server():
            print("%s: Symbol Limit Reached with subscription to %s" %
                  (self._name, sym))

    def process_ip_addresses_used(self, ip: str) -> None:
        if not is_server():
            print("%s: IP Addresses Used: %s" % (self._name, ip))


def get_level_1_quotes_and_trades(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = MyQuote(name="pyiqfeed-Example-lvl1")
    quote_listener = MyQuoteListener("Level 1 Listener")
    quote_conn.add_listener(quote_listener)
    print(
        'level 1'
    )
    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        # quote_conn.watch(ticker)
        # quote_conn.watch('NVDA')
        # quote_conn.regional_watch(ticker)
        # quote_conn.regional_watch('NVDA')
        print(update_mongo.get_symbols())
        for symbol in update_mongo.get_symbols():
            print(symbol, end=', ')
            quote_conn.watch(symbol)
            # quote_conn.regional_watch(symbol)

        quote_conn.news_on()

        while quote_conn.reader_running():
            # quote_conn.request_stats()
            try:
                quote_conn.refresh(ticker)
            except Exception as e:
                print(e)
            # quote_conn.
            # quote_conn.read_message()

            time.sleep(.1)
            # quote_conn.unwatch(ticker)
            # quote_conn.remove_listener(quote_listener)


def get_regional_quotes(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-regional")
    quote_listener = iq.VerboseQuoteListener("Regional Listener")
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


def get_live_interval_bars(ticker: str, bar_len: int, seconds: int):
    """Get real-time interval bars"""
    bar_conn = iq.BarConn(name='pyiqfeed-Example-interval-bars')
    bar_listener = iq.VerboseBarListener("Bar Listener")
    bar_conn.add_listener(bar_listener)

    with iq.ConnConnector([bar_conn]) as connector:
        bar_conn.watch(symbol=ticker, interval_len=bar_len,
                       interval_type='s', update=1, lookback_bars=10)
        time.sleep(seconds)


def get_administrative_messages(seconds: int):
    """Run and AdminConn and print connection stats to screen."""

    admin_conn = iq.AdminConn(name="pyiqfeed-Example-admin-messages")
    admin_listener = iq.VerboseAdminListener("Admin Listener")
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
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
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
            print(bars)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_daily_data(ticker: str, num_days: int):
    """Historical Daily Data"""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-daily-data")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
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
    table_listener = iq.VerboseIQFeedListener("Reference Data Listener")
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
    lookup_listener = iq.VerboseIQFeedListener("TickerLookupListener")
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
    lookup_listener = iq.VerboseIQFeedListener("EqOptionListener")
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
    lookup_listener = iq.VerboseIQFeedListener("FuturesChainLookupListener")
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
    lookup_listener = iq.VerboseIQFeedListener("FuturesSpreadLookupListener")
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
    lookup_listener = iq.VerboseIQFeedListener("FuturesOptionLookupListener")
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
    news_listener = iq.VerboseIQFeedListener("NewsListener")
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
    print(results)
    get_level_1_quotes_and_trades(ticker="AMD", seconds=1)

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
