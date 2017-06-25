import redis
import ujson
import concurrent.futures
import time


class Redis(object):
    def __init__(self):
        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)

    def set(self, key, val):
        return self.r.set(key, ujson.dumps(val))

    def get(self, key):
        return ujson.loads(self.r.get(key))


r = Redis()

r.set('foo', {
    'TIGER': 'LAOHU'
})
print(r.get('foo'))

r.set('str', 'str')
print(r.get('str'))


def test(sec):
    time.sleep(sec)
    print(r.get('str'))


pool = {}
# with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
#     executor.submit(test, 1)
#     executor.submit(test, 5)

start = time.time()

dic = {'date': '1460727000000', 'open': 55.3, 'high': 55.3, 'low': 55.25, 'close': 55.25, 'volume': 3399547},
big_data = [dic for i in range(720)]

for i in range(10000):
    # r.set('foo', big_data)
    # r.get('foo')
    ujson.loads(ujson.dumps(big_data))

print(time.time() - start)
