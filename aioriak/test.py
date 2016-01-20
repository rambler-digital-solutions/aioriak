import os
os.environ['PYTHONASYNCIODEBUG'] = '1'

import asyncio
from client import RiakClient
from riak import RiakClient as ReferenceClient
# from pprint import pprint

rc = ReferenceClient(host='localhost')
b = rc.bucket_type('counter_map').bucket('counters')
print('keys:', len(b.get_keys()), b.get_keys()[:3])
key = b.get_keys()[0]
obj = b.get(key)
print('RIAK:', obj)
# print(obj.data)

loop = asyncio.get_event_loop()

async def test():
    # client = await RiakClient.create('paywall-app01.dev.zerg.rambler.ru',
    client = await RiakClient.create('localhost',
                                     loop=loop)
    bucket_type = client.bucket_type('counter_map')
    bucket = bucket_type.bucket('counters')
    keys = await bucket.get_keys()
    print(keys[:3])
    print((await bucket.get(key)))
    print((await client.bucket('rules').get('rules')))

    '''res = await conn.get(
        bucket_type=b'counter_map', bucket=b'counters',
        key=b'2015-12-51-578c8259a3d946dab994abb97717a469-lenta')
    print(res)'''

loop.run_until_complete(test())
