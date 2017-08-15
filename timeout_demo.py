# # # from get_stocks import *
from my_functions import *
# # import requests
# # import time
# #
# # # # get sp500 stocks
# # # stocks = get_sp500()
# # #
# # #
# # # class Lv1Lstener(MyQuoteListener):
# # #     test = 0
# # #
# # #     def __init__(self, name: str):
# # #         super().__init__(name)
# # #         self.caches = {}
# # #         print(self.__class__, 'created')
# # #
# # #     def process_update(self, update: np.array):
# # #         data = UpdateMongo.process_quote(update)
# # #         symbol = data['symbol']
# # #         if symbol not in self.caches:
# # #             print(symbol, data['last'], data['volume'])
# # #             self.caches[symbol] = 'printed'
# # #
# # #     def process_summary(self, update: np.array):
# # #         print(update)
# # #         data = UpdateMongo.process_quote(update)
# # #         symbol = data['symbol']
# # #         if symbol not in self.caches:
# # #             print(symbol, data['last'], data['volume'])
# # #             self.caches[symbol] = 'printed'
# #
# #
# # if __name__ == '__main__':
# #     # lv1 = threading.Timer(5, get_level_1_multi_quotes_and_trades, [stocks, 1, False, Lv1Lstener])
# #     # lv1.start()
# #     # Lv1Lstener.test = 1
# #     # print(Lv1Lstener.test)
# #     start = time.time()
# #     aws = requests.get('https://7dian.pw')
# #
# #     print(time.time() - start)
# #     start = time.time()
# #     www = requests.get('https://www.7dian.pw')
# #
# #     print(time.time() - start)
# #
# #     print(www.content)
# #     print(aws.content)
# #     print(www.content == aws.content)
#
# from __future__ import print_function
#
# import sys
# import numpy as np
# import pylab
#
# import talib
# from talib.abstract import Function
#
# TEST_LEN = int(sys.argv[1]) if len(sys.argv) > 1 else 100
# r = np.arange(TEST_LEN)
# idata = np.random.random(TEST_LEN)
#
#
# def func_example():
#     odata = talib.MA(idata)
#     upper, middle, lower = talib.BBANDS(idata)
#     kama = talib.KAMA(idata)
#     plot(odata, upper, middle, lower, kama)
#
#
# def abstract_example():
#     sma = Function('sma')
#     input_arrays = sma.get_input_arrays()
#     for key in input_arrays.keys():
#         input_arrays[key] = idata
#     sma.set_input_arrays(input_arrays)
#     odata = sma(30)  # timePeriod=30, specified as an arg
#
#     bbands = Function('bbands', input_arrays)
#     bbands.parameters = {
#         'timeperiod': 20,
#         'nbdevup': 2,
#         'nbdevdn': 2
#     }
#     upper, middle, lower = bbands()  # multiple output values unpacked (these will always have the correct order)
#
#     kama = Function('kama').run(input_arrays)  # alternative run() calling method.
#     plot(odata, upper, middle, lower, kama)
#
#
# def plot(odata, upper, middle, lower, kama):
#     pylab.plot(r, idata, 'b-', label="original")
#     pylab.plot(r, odata, 'g-', label="MA")
#     pylab.plot(r, upper, 'r-', label="Upper")
#     pylab.plot(r, middle, 'r-', label="Middle")
#     pylab.plot(r, lower, 'r-', label="Lower")
#     pylab.plot(r, kama, 'g', label="KAMA")
#     pylab.legend()
#     pylab.show()
#
#
# if __name__ == '__main__':
#     print('All functions (sorted by group):')
#     groups = talib.get_function_groups()
#     for group, functions in sorted(groups.items()):
#         print('%s functions: %s' % (group, functions))
#
#     if len(sys.argv) == 1 or sys.argv[1] == 'func':
#         print('Using talib.func')
#         func_example()
#     else:
#         print('Using talib.abstract')
#         abstract_example()

import multiprocessing
import threading
import time
import datetime as dt
import sys


# import numba


def consumer(ns, events, stream: sys.stdout):
    orders = {
        'buy': deque(),
        'sell': deque(),
    }

    def stop():
        while 1:
            events['stop'].wait()
            stream.write('Stopping, canceling all the buying order and selling all the position\n')
            events['pause'].clear()
            events['lv2'].set()  # make the lv2 wait stops

            exit(0)

    def pause():
        while 1:
            events['pause'].wait()
            stream.write('Pausing, canceling all the buying order and selling all the position\n')
            events['pause'].clear()

            events['resume'].wait()
            events['pause'].clear()
            events['resume'].clear()

    pause_detect_process = threading.Thread(target=pause, args=())
    stop_detect_process = threading.Thread(target=stop, args=())
    pause_detect_process.start()
    stop_detect_process.start()
    try:
        value = ns.lv1
    except Exception as err:
        print('Before event, consumer got:', str(err))
    while 1:
        # stream.write('Waiting for lv2\n')
        if events['lv2'].wait() and not events['stop'].is_set():
            # for i in range(100000):
            #     pass
            stream.write(f'After event, consumer got: {(dt.datetime.now() - ns.lv1).microseconds / 1000} ms {id(ns)}\n')
            # fast(stream)
            # stream.write(str(trader.get_account()))
            events['lv2'].clear()
            # if events['stop'].is_set():
            #     stream.write('Received stop single, stopping.')
            #     exit(0)
        while events['pause'].is_set():
            pass
        if events['stop'].is_set():
            stream.write('Stop is set, main process is stopping\n')
            while stop_detect_process.is_alive():
                pass
            stream.write('stop_detect_process finished, main process is stopping\n')

            exit(0)


# @numba.jit
def fast(stream: sys.stdout):
    for i in range(100000):
        pass
        # stream.write('in fast' + str(dt.datetime.now().timestamp()))


class Main(object):
    cache = {}
    mgr = multiprocessing.Manager()
    namespace = mgr.Namespace()
    process = {}
    processes = {}
    events_keys = ['lv2', 'stop', 'pause', 'resume']
    stream = sys.stdout

    def __init__(self):
        pass

    def create_process(self, symbol: str):
        namespace = self.mgr.Namespace()
        events = {key: multiprocessing.Event() for key in self.events_keys}
        print(id(namespace))

        self.processes[symbol] = {
            'process': multiprocessing.Process(target=consumer,
                                               args=(namespace, events, sys.stdout)),
            'data': namespace,
            'events': events
        }
        self.processes[symbol]['process'].start()

    def update(self, data=None):
        if data:
            self.processes['AMD']['data'].lv1 = data
            self.processes['AMD']['events']['lv2'].set()

            # print(self.process['AMD'], id(self.process['AMD']))
            # self.event.set()


if __name__ == '__main__':
    # mgr = multiprocessing.Manager()
    # namespace = mgr.Namespace()
    # event = multiprocessing.Event()
    # # p = multiprocessing.Process(target=producer, args=(namespace, event))
    # c = multiprocessing.Process(target=consumer, args=(namespace, event, sys.stdout))
    #
    # c.start()
    #
    # for i in range(10):
    #     namespace.value = i
    #     event.set()
    #     print(i)
    #     time.sleep(.5)
    #
    # c.join()

    main = Main()
    print(main.process)
    print(type(main.namespace))
    stock = 'AMD'
    main.create_process('AMD')
    time.sleep(1)
    main.update(dt.datetime.now())
    for i in range(10):
        main.update(dt.datetime.now())
        time.sleep(.05)
    time.sleep(5)
    main.update(dt.datetime.now())
    time.sleep(1)
    main.processes[stock]['events']['stop'].set()
    time.sleep(5)
