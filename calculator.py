from my_functions import *


class Calculator(object):
    @staticmethod
    def bb_calculator(sample: dict) -> type(np.ndarray):
        bb = BBANDS(sample, 20, 2, 2)
        return bb

    @staticmethod
    def sar_calculator(sample: dict) -> type(np.ndarray):
        sar = SAREXT(sample)
        return np.abs(sar)

    @staticmethod
    def kd_calculator(sample: dict) -> type(np.ndarray):
        return STOCH(
            sample,
            fastk_period=3,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=9,
            slowd_matype=0
        )

    @staticmethod
    def macd_calculator(sample: dict) -> type(np.ndarray):
        return MACD(sample, fastperiod=5, slowperiod=35, signalperiod=5)

    @staticmethod
    def sma_calculator(sample: dict) -> dict:
        return {
            5: SMA(sample, timeperiod=5),
            10: SMA(sample, timeperiod=10),
            20: SMA(sample, timeperiod=20)
        }

    @staticmethod
    def kd_golden_cross(sample: dict):
        """
        测试是否是金叉状态.
        :param sample:
        :param upper_key:
        :param lower_key:
        """
        k, d = Calculator.kd_calculator(sample)
        if len(k) == 0 or len(d) == 0:
            return 0
        return k[-1] - d[-1]

    @staticmethod
    def macd_golden_cross(sample: dict):
        """
        测试是否是金叉状态. MACD 都可以用
        :param sample:
        :param upper_key:
        :param lower_key:
        """
        macd, signal, diff = Calculator.macd_calculator(sample)
        if len(macd) == 0 or len(signal) == 0:
            return 0
        return macd[-1] - signal[-1]

    @staticmethod
    def golden_cross(sample: dict):
        """
        测试是否是金叉状态. MACD 和 KD 都必须处于金叉
        :param sample:
        :param upper_key:
        :param lower_key:
        """
        return Calculator.kd_calculator(sample) > 0 and Calculator.macd_calculator(sample) > 0
