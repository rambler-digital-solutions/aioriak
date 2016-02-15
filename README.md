[![Build Status](https://travis-ci.org/rambler-digital-solutions/aioriak.svg?branch=master)](https://travis-ci.org/rambler-digital-solutions/aioriak)
# Python asyncio client for Riak #

## Installation ##
The minimal versions of Python for use with this client are Python 3.5.x.

### From Source ###

```bash
python setup.py install
```
## Using example ##

```python
client = await RiakClient.create('localhost', loop=loop)
bucket_type = client.bucket_type('default')
bucket = bucket_type.bucket('example')
obj = await bucket.get('key')
print(obj.data)
```