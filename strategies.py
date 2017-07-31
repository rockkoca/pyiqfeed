from enum import Enum
from traders import *
from calculator import *


class Strategy(object):
    class Type(Enum):
        AMAZON_QUICK_IN_OUT: Strategy.amazon_quick_in_out

    def __init__(self, trader_client: Trader, symbol: str):
        self.trader_client = trader_client
        self.active_mode = False  # 是否在操作区域
        self.symbol = symbol

    def get_bars(self):
        data = self.trader_client.mongo_client.cache['bars'].get(self.symbol)['bars']
        # data = self.cache['bars'][symbol]['bars']
        bars = {
            'time': data[0],
            "open": data[1],
            'high': data[2],
            'low': data[3],
            'close': data[4],
            'volume': data[5]
        }
        return bars

    def get_lv1(self):
        data = self.trader_client.mongo_client.cache['lv1'].get(self.symbol)
        # self.cache['lv1'][symbol]
        return data

    def get_lv2(self):
        data = self.trader_client.mongo_client.cache['lv2'].get(self.symbol)
        # self.cache['lv1'][symbol]
        return data

    def entry(self, stop: float):
        pass

    def run(self, ty: Type) -> None:
        if ty not in self.Type.__members__:
            raise Exception('Not a valid strategy')

        if type(ty.value) == function:
            strategy = threading.Timer(1, ty.value)
            strategy.start()

    def amazon_quick_in_out(self):
        """
        买入点: 稍微高于 bid 并且低于布林带上限
        卖出点: 稍微低于 ask
        操作区域: MACD 和 KD 位于金叉内.
                 如果KD发生高位钝化, 这时需要判断价格是否跌破EMA10, 停止操作一旦跌破EMA10
        :return:
        """
        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            while 1:
                # self.is_active_zone(self.get_bars(), self.get_lv1())
                is_active_zone = executor.submit(self.is_active_zone, bars=self.get_bars(), lv1=self.get_lv1())
                try:
                    result = is_active_zone.result()
                except Exception as e:
                    print(e)
                else:
                    self.active_mode = result

                    if self.active_mode:
                        """
                        在这里 进行 买卖操作
                        """
                        lv2 = self.get_lv2()
                        pass

                time.sleep(.2)

    def is_active_zone(self, bars: dict, lv1: dict) -> bool:
        """
        如果 macd 和 kd 都处于金叉状态, 那么可以进行交易. 绿色蜡烛也应该是检测参数之一
        如果 macd 和 kd 都不出于金叉状态, 如果价格还运行在 sma10 的上方, 那么仍然可能可以进行交易.



        :return:
        """
        # bars = self.get_bars()
        k, d = Calculator.kd_calculator(bars)
        macd, single, diff = Calculator.macd_calculator(bars)
        if k > d and macd > single:
            self.active_mode = True
            return self.active_mode

        if self.active_mode:
            smas = Calculator.sma_calculator(bars)
            sma10 = smas[10]
            # lv1 = self.get_lv1()
            if macd < single or bars['low'][-1] < sma10[-1] or lv1['last'] < sma10[-1]:
                self.active_mode = False
                return self.active_mode

        return False
