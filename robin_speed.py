from robinhood.Robinhood import *
from robinhood.credentials import *
from datetime import datetime as dt

trader = Robinhood()
trader.login(username=Credential.get_username(), password=Credential.get_password())

start = dt.now()

trader.get_quote('AMD')

end = dt.now()

used = end - start
us = used.microseconds
print(f'time used {us / 1000} ms or {us / 1000 / 1000} secs')
