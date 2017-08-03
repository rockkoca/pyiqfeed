#! /usr/bin/env python3
# coding=utf-8
from my_functions import *


def get_indexes():
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    listener = IndexWatcher
    quote_conn = MyQuote(name="pyiqfeed-index")
    quote_listener = listener("index Listener")
    quote_conn.add_listener(quote_listener)
    print('get_indexes started')

    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        for ticker in IndexWatcher.symbol_map.keys():
            print(f'watch {ticker}')
            quote_conn.watch(ticker)
            time.sleep(1)

        while quote_conn.reader_running():
            time.sleep(5)


def get_live_index_interval_bars(tickers: dict, bar_len: int, seconds: int, auto_unwatch=True):
    """Get real-time interval bars"""
    bar_conn = iq.BarConn(
        name='{} pyiqfeed-index_interval_bars'.format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    bar_listener = MyBarListener(
        "{}-index_interval_bars-listener".format('auto_unwatch' if auto_unwatch else 'auto_trade'))
    bar_conn.add_listener(bar_listener)
    # print('get_live_interval_bars {}@{}'.format(str(tickers), bar_len))
    # watching = {
    #
    # }
    # mongo_conn = UpdateMongo()
    # if auto_unwatch:
    tickers = {
        'SPX.XO': '^GSPC',
        'VIX.XO': '^VIX',
    }

    with iq.ConnConnector([bar_conn]) as connector:
        for ticker in tickers.keys():
            bar_conn.watch(symbol=ticker,
                           interval_len=bar_len,
                           interval_type='s',
                           update=1,
                           lookback_bars=look_back_bars,
                           request_id=f'{ticker}-{bar_len}-S-INDEX')
            print('watching index bar {}@{}'.format(ticker, bar_len))

        while 1:
            time.sleep(3)


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

    # wait 10 till the service is started
    threading.Timer(30, check_connection).start()

    indexes = threading.Timer(5, get_indexes)
    indexes.start()

    #
    # index_bars = threading.Timer(5, get_live_index_interval_bars, [{}, 60, 1, True])
    # index_bars.start()

    # Modify code below to connect to the socket etc as described above
    admin = iq.AdminConn(name="Launcher")
    admin_listener = iq.VerboseAdminListener("Launcher-listen")
    admin.add_listener(admin_listener)
    with iq.ConnConnector([admin]) as connected:
        admin.client_stats_on()
        # while not os.path.isfile(ctrl_file):
        while 1:
            time.sleep(10)
