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

### Features ###

| Feature            | Status|
|--------------------|:-----:|
| Riak KV operations |  Yes  |
| Riak Datatypes     |  Yes  |
| Riak BucketTypes   |  Yes  |
| Custom resolver    |  Yes  |
| Node list support  |   No  |
| Custom quorum      |   No  |
| Connections Pool   |   No  |
| Operations timout  |   No  |
| Security           |   No  |
| Tested python ver. | 3.5.x |
| Tested Riak version| 2.1.3 |


## Using example ##

```python
client = await RiakClient.create('localhost', loop=loop)
bucket_type = client.bucket_type('default')
bucket = bucket_type.bucket('example')
obj = await bucket.get('key')
print(obj.data)
```

## Testing ##

### Docker based testing ###

You can use docker for running:

```bash
DOCKER_CLUSTER=1 python setup.py test
```
