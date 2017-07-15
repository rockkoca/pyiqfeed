from robinhood.Robinhood import *
from robinhood.credentials import *
from datetime import datetime as dt
import requests

trader = Robinhood()
trader.login(username=Credential.get_username(), password=Credential.get_password())

start = dt.now()

# trader.get_quote('AMD')
# requests.get('https://api.robinhood.com/quotes/?symbols=GPRO,DRYS,AMD')
l = [x for x in range(1000)]
x = [l for i in range(1000)]
for i, l in enumerate(x):
    x[i] = sorted(l)

end = dt.now()

used = end - start
us = used.microseconds
print(f'time used {us / 1000} ms or {us / 1000 / 1000} secs')
