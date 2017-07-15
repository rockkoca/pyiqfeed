from robinhood.Robinhood import *
from robinhood.credentials import *
import time

trader = Robinhood()
trader.login(username=Credential.get_username(), password=Credential.get_password())

start = time.time()

trader.get_quote('AMD')

end = time.time()

print(f'time used {end - start} seconds')
