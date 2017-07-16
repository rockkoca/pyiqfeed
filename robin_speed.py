from robinhood.Robinhood import *
from robinhood.credentials import *
from datetime import datetime as dt
from my_functions import *
import requests

update_mongo = UpdateMongo()
db = update_mongo.get_db()

amd = db.instruments.find_one({'symbol': 'AMD'})

# trader = Robinhood()
# trader.login(username=Credential.get_username(), password=Credential.get_password())

start = dt.datetime.now()

# trader.get_quote('AMD')
# requests.get('https://api.robinhood.com/quotes/?symbols=GPRO,DRYS,AMD')
# l = [x for x in range(200)]
# x = [l for i in range(200)]
# for i, l in enumerate(x):
#     x[i] = sorted(l)
order = trader.limit_buy(amd, 1, 1)

print(order)

# print(trader.url('https://api.robinhood.com/accounts/5SA59772/positions/940fc3f5-1db5-4fed-b452-f3a2e4562b5f/'))
end = dt.datetime.now()
print(trader.url(order['position']))

time.sleep(5)
print(trader.cancel_order(order))
# print(trader.cancel_order(order))


# print(trader.url_post("https://api.robinhood.com/orders/22e0ba99-b346-4591-b36d-55d4d617cb57/cancel/"))

used = end - start
us = used.microseconds
print(f'time used {us / 1000} ms or {us / 1000 / 1000} secs')
