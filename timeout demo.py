import redis
import ujson
import concurrent.futures
import time
from my_functions import *

data = {}


def test(key):
    time.sleep(key)
    data[key] = key
    return data


# with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
#     executor.submit(test, 1)
#     executor.submit(test, 5)

# for future in concurrent.futures:

import concurrent.futures
import math

PRIMES = [
    '1122725350951231231231242943',
    '1125827059141241231234217441',
    '1122725350123123123123955249',
    '1152800951123123123123490743',
    '1157978481235234234234407470',
    '1099726899234234342234428544'
]


def is_prime(n):
    for i in range(10000):
        ujson.loads(ujson.dumps(PRIMES))
    return PRIMES


def delay(*args):
    print('delay 2 seconds')




def main():
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print(number, prime)

    print(time.time() - start)


if __name__ == '__main__':
    set_timeout(delay, 3)
    main()

    while 1:
        time.sleep(1)
