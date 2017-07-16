# from my_functions import *
#
# update_mongo = UpdateMongo()
# # print(update_mongo.get_symbols())
# db = update_mongo.get_client().meteor
# print(db)
# col = db.quotes
# print(col)
#
# amd = col.find_one({'symbol': 'AMD'})
#
# for k, v in amd.items():
#     print(k, v)
# amd['ask_price'] = 18
# col.update_one({'symbol': 'AMD'},
#                {
#                    "$set": amd,
#                },
#                True)
# # ins = col.find_one({'instrument', 'https://api.robinhood.com/instruments/66ec1551-e033-4f9a-a46f-2b73aa529977/'})
# # print(ins)
import concurrent.futures
import time
import requests
from datetime import datetime as dt

start = dt.now()


# def test(second=3):
#     time.sleep(3)
#     print(f'I slept {second} seconds')
#     return second


def test_request():
    return requests.get('http://localhost:3000/f991eb733c339f4a5f9087354e9c683cd0b9969f/lv2-quick-sell/AMD').content


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        task = executor.submit(test_request)
        print('after submit')
        try:
            data = task.result()
            end = dt.now()
        except Exception as exc:
            print('%r generated an exception: %s' % ('', exc))
        else:
            # print('%r page is %d bytes' % ('', len(data)))
            used = end - start
            us = used.microseconds
            print(f'time used {us / 1000} ms or {us / 1000 / 1000} secs')
