import unittest
from robinhood.Robinhood import *
import ujson

trader = Robinhood()
trader.login(username='rockkoca@gmail.com', password='end0405LESS')
print(trader.auth_token)
print(trader.portfolios())
print(ujson.dumps(trader.get_account()))
amd = trader.instrument('S')
print(amd)

# print(trader.limit_buy(instrument=amd, price='10.00', quantity=1))

class TestRB(unittest.TestCase):
    def test_login(self):
        self.assertGreater(trader.equity(), 25000, 'equity correct')

    def test_instrument(self):
        self.assertEqual(trader.instrument('S')['symbol'], 'S')
        self.assertEqual(trader.instrument('AMD')['symbol'], 'AMD')
        self.assertEqual(trader.instrument('NVDA')['symbol'], 'NVDA')

