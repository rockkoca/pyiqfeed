from get_stocks import *
from my_functions import *
import ujson as json
import sys

sys.setrecursionlimit(150000000)


class MockBarListener(MyBarListener):
    def __init__(self, name: str):
        super().__init__(name)
        self.caches = {}
        print(self.__class__, 'created')

    def process_history_bar(self, bar_data: np.array) -> None:
        if 1:
            print("%s: Process history bar:" % self._name)
        # print(bar_data)
        data = UpdateMongo.process_bars(bar_data)
        print(data)

        # self.update_mongo.update_bars(bar_data, name=self._name, history=True)


def make_np_list(symbol: str, arr: np.ndarray) -> np.ndarray:
    first = np.array([symbol.encode('ascii')])
    second = np.array(list(arr))
    to = np.concatenate((first, second))

    to[1] = str(to[1])
    to[2] = str(to[2])
    to[2] = to[2][:-13]
    # print(to)
    return to


def get_period_historical_bar_data(ticker: str, start: int, end=0, bar_len=15, num_bars=1000, bar_unit='s',
                                   ):
    global stop
    """Shows how to get interval bars."""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-historical-bars")
    hist_listener = MyBarListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)
    update_mongo = UpdateMongo()

    with iq.ConnConnector([hist_conn]) as connector:
        # look at conn.py for request_bars, request_bars_for_days and
        # request_bars_in_period for other ways to specify time periods etc
        try:
            # bars = hist_conn.request_bars(ticker=ticker,
            #                               interval_len=bar_len,
            #                               interval_type=bar_unit,
            #                               max_bars=num_bars)
            # print(bars)

            today = datetime.date.today()
            start_date = today - datetime.timedelta(days=start)
            start_time = datetime.datetime(year=start_date.year,
                                           month=start_date.month,
                                           day=start_date.day,
                                           hour=9,
                                           minute=30,
                                           second=0)
            end_date = today - datetime.timedelta(days=end)

            end_time = datetime.datetime(year=end_date.year,
                                         month=end_date.month,
                                         day=end_date.day,
                                         hour=16,
                                         minute=00,
                                         second=00)
            bars = hist_conn.request_bars_in_period(ticker=ticker,
                                                    interval_len=bar_len,
                                                    interval_type=bar_unit,
                                                    bgn_prd=start_time,
                                                    end_prd=end_time)
            print(np.array([ticker]).shape)
            print(np.array(list(bars[0])).shape)
            print(make_np_list('AMD', bars[0]))
            # print(type(bars[0][1]), type(bars[0][0]))
            # print(("%s000" % (np.floor(bars[0][0] + bars[0][1]).tolist().timestamp())))
            # with open('bars', 'w') as file:
            i = 0
            bars = bars[::-1]
            while i < len(bars):
                # json.dumps(bar.tolist())
                symbols = update_mongo.get_symbols()
                if symbols[ticker]['auto'].get('sp', 0) == 0:
                    bar = bars[i]
                    line = list(bar.tolist())
                    # print(line)
                    # file.write(json.dumps(list(map(str, line))) + '\n')
                    if i < 30:
                        hist_listener.process_history_bar(
                            [make_np_list(ticker, bar)]
                        )
                    else:
                        if i == 30:
                            time.sleep(5)
                        hist_listener.process_live_bar(
                            [make_np_list(ticker, bar)]
                        )
                    i += 1
                else:
                    time.sleep(.5)

                time.sleep(.1)

        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


if __name__ == '__main__':
    # lv1 = threading.Timer(5, get_historical_bar_data, ['amd', 1, False, MockBarListener])
    # lv1.start()
    time.sleep(5)
    get_period_historical_bar_data('JNUG', 2, 1)
