import os
os.environ['PYTHONASYNCIODEBUG'] = '1'

import asyncio
from client import RiakClient
from riak import RiakClient as ReferenceClient
from pprint import pprint

rc = ReferenceClient(host='localhost')
b = rc.bucket_type('counter_map').bucket('counters')
b2 = rc.bucket('rules')
pprint(b2.get_properties())
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
    print((await client.bucket('rules').get('rules')).data)
    rawdata = client.bucket_type('rawdata')
    events = rawdata.bucket('events')
    pprint(await rawdata.get_properties())
    # await rawdata.set_property('search_index', 'events')
    # pprint(await rawdata.get_properties())
    # print(await events.get((await events.get_keys())[0]))
    keys = await events.get_keys()
    print(await events.get(keys[1]))

loop.run_until_complete(test())
