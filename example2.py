#! /usr/bin/env python3
# coding=utf-8
from my_functions import *
import concurrent.futures
import multiprocessing

if __name__ == "__main__":

    lv2 = threading.Timer(5, get_level_2_multi_quotes_and_trades, [{}, 1, True])
    lv2.start()

    while 1:
        time.sleep(1)
