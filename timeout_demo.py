# # from get_stocks import *
# # from my_functions import *
# import requests
# import time
#
# # # get sp500 stocks
# # stocks = get_sp500()
# #
# #
# # class Lv1Lstener(MyQuoteListener):
# #     test = 0
# #
# #     def __init__(self, name: str):
# #         super().__init__(name)
# #         self.caches = {}
# #         print(self.__class__, 'created')
# #
# #     def process_update(self, update: np.array):
# #         data = UpdateMongo.process_quote(update)
# #         symbol = data['symbol']
# #         if symbol not in self.caches:
# #             print(symbol, data['last'], data['volume'])
# #             self.caches[symbol] = 'printed'
# #
# #     def process_summary(self, update: np.array):
# #         print(update)
# #         data = UpdateMongo.process_quote(update)
# #         symbol = data['symbol']
# #         if symbol not in self.caches:
# #             print(symbol, data['last'], data['volume'])
# #             self.caches[symbol] = 'printed'
#
#
# if __name__ == '__main__':
#     # lv1 = threading.Timer(5, get_level_1_multi_quotes_and_trades, [stocks, 1, False, Lv1Lstener])
#     # lv1.start()
#     # Lv1Lstener.test = 1
#     # print(Lv1Lstener.test)
#     start = time.time()
#     aws = requests.get('https://7dian.pw')
#
#     print(time.time() - start)
#     start = time.time()
#     www = requests.get('https://www.7dian.pw')
#
#     print(time.time() - start)
#
#     print(www.content)
#     print(aws.content)
#     print(www.content == aws.content)

from __future__ import print_function

import sys
import numpy as np
import pylab

import talib
from talib.abstract import Function

TEST_LEN = int(sys.argv[1]) if len(sys.argv) > 1 else 100
r = np.arange(TEST_LEN)
idata = np.random.random(TEST_LEN)


def func_example():
    odata = talib.MA(idata)
    upper, middle, lower = talib.BBANDS(idata)
    kama = talib.KAMA(idata)
    plot(odata, upper, middle, lower, kama)


def abstract_example():
    sma = Function('sma')
    input_arrays = sma.get_input_arrays()
    for key in input_arrays.keys():
        input_arrays[key] = idata
    sma.set_input_arrays(input_arrays)
    odata = sma(30)  # timePeriod=30, specified as an arg

    bbands = Function('bbands', input_arrays)
    bbands.parameters = {
        'timeperiod': 20,
        'nbdevup': 2,
        'nbdevdn': 2
    }
    upper, middle, lower = bbands()  # multiple output values unpacked (these will always have the correct order)

    kama = Function('kama').run(input_arrays)  # alternative run() calling method.
    plot(odata, upper, middle, lower, kama)


def plot(odata, upper, middle, lower, kama):
    pylab.plot(r, idata, 'b-', label="original")
    pylab.plot(r, odata, 'g-', label="MA")
    pylab.plot(r, upper, 'r-', label="Upper")
    pylab.plot(r, middle, 'r-', label="Middle")
    pylab.plot(r, lower, 'r-', label="Lower")
    pylab.plot(r, kama, 'g', label="KAMA")
    pylab.legend()
    pylab.show()


if __name__ == '__main__':
    print('All functions (sorted by group):')
    groups = talib.get_function_groups()
    for group, functions in sorted(groups.items()):
        print('%s functions: %s' % (group, functions))

    if len(sys.argv) == 1 or sys.argv[1] == 'func':
        print('Using talib.func')
        func_example()
    else:
        print('Using talib.abstract')
        abstract_example()
