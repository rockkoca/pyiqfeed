from my_functions import *
from collections import deque

import robinhood.Robinhood as RB
from robinhood.Robinhood import *
from robinhood.credentials import *


class Trader(object):
    def __init__(
            self,
            symbol: str,
            trader_client: RB,
            mongo_client: UpdateMongo,
            max_money: float = 100,
    ):
        self.symbol = symbol
        self.trader = trader_client
        self.mongo_client = mongo_client
        self.position = 0
        self.pending_orders = deque()
        self.max_money = max_money

    def update(self):
        pass



