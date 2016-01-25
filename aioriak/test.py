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
    keys = await bucket.get_keys()
    print(keys[:3])
    print((await bucket.get(key)))
    print((await client.bucket('rules').get('rules')).data)

    '''res = await conn.get(
        bucket_type=b'counter_map', bucket=b'counters',
        key=b'2015-12-51-578c8259a3d946dab994abb97717a469-lenta')
    print(res)'''

loop.run_until_complete(test())
