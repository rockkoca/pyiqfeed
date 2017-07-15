# from get_stocks import *
# from my_functions import *
import requests
import time

# # get sp500 stocks
# stocks = get_sp500()
#
#
# class Lv1Lstener(MyQuoteListener):
#     test = 0
#
#     def __init__(self, name: str):
#         super().__init__(name)
#         self.caches = {}
#         print(self.__class__, 'created')
#
#     def process_update(self, update: np.array):
#         data = UpdateMongo.process_quote(update)
#         symbol = data['symbol']
#         if symbol not in self.caches:
#             print(symbol, data['last'], data['volume'])
#             self.caches[symbol] = 'printed'
#
#     def process_summary(self, update: np.array):
#         print(update)
#         data = UpdateMongo.process_quote(update)
#         symbol = data['symbol']
#         if symbol not in self.caches:
#             print(symbol, data['last'], data['volume'])
#             self.caches[symbol] = 'printed'


if __name__ == '__main__':
    # lv1 = threading.Timer(5, get_level_1_multi_quotes_and_trades, [stocks, 1, False, Lv1Lstener])
    # lv1.start()
    # Lv1Lstener.test = 1
    # print(Lv1Lstener.test)
    start = time.time()
    aws = requests.get('https://7dian.pw')

    print(time.time() - start)
    start = time.time()
    www = requests.get('https://www.7dian.pw')

    print(time.time() - start)

    print(www.content)
    print(aws.content)
    print(www.content == aws.content)
