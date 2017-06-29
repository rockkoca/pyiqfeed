from get_stocks import *
from my_functions import *

# get sp500 stocks
stocks = get_sp500()


class Lv1Lstener(MyQuoteListener):
    def __init__(self, name: str):
        super().__init__(name)
        self.caches = {}
        print(self.__class__, 'created')

    def process_update(self, update: np.array):
        data = UpdateMongo.process_quote(update)
        symbol = data['symbol']
        if symbol not in self.caches:
            print(symbol, data['last'], data['volume'])
            self.caches[symbol] = 'printed'

    def process_summary(self, update: np.array):
        print(update)
        data = UpdateMongo.process_quote(update)
        symbol = data['symbol']
        if symbol not in self.caches:
            print(symbol, data['last'], data['volume'])
            self.caches[symbol] = 'printed'


lv1 = threading.Timer(5, get_level_1_multi_quotes_and_trades, [stocks, 1, False, Lv1Lstener])
lv1.start()
