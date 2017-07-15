from robinhood.Robinhood import *
from robinhood.credentials import *
from datetime import datetime as dt

trader = Robinhood()
trader.login(username=Credential.get_username(), password=Credential.get_password())

start = dt.now()

trader.get_quote('AMD')

end = dt.now()

print(f'time used {(end - start).microseconds} microseconds or {(end - start).seconds} secs')
