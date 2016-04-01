[![Build Status](https://travis-ci.org/rambler-digital-solutions/aioriak.svg?branch=master)](https://travis-ci.org/rambler-digital-solutions/aioriak)
[![Coverage Status](https://coveralls.io/repos/github/rambler-digital-solutions/aioriak/badge.svg?branch=master)](https://coveralls.io/github/rambler-digital-solutions/aioriak?branch=master)
[![GitHub issues](https://img.shields.io/github/issues/rambler-digital-solutions/aioriak.svg)](https://github.com/rambler-digital-solutions/aioriak/issues)
[![PyPI version](https://badge.fury.io/py/aioriak.svg)](https://badge.fury.io/py/aioriak)
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
