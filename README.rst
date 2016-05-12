.. image:: https://travis-ci.org/rambler-digital-solutions/aioriak.svg?branch=master
    :target: https://travis-ci.org/rambler-digital-solutions/aioriak
    :alt: Build Status 
   
.. image:: https://coveralls.io/repos/github/rambler-digital-solutions/aioriak/badge.svg?branch=master 
    :target: https://coveralls.io/github/rambler-digital-solutions/aioriak?branch=master
    :alt: Coverage Status 

.. image:: https://img.shields.io/github/issues/rambler-digital-solutions/aioriak.svg
    :target: https://github.com/rambler-digital-solutions/aioriak/issues
    :alt: GitHub issues 

.. image:: https://badge.fury.io/py/aioriak.svg  
    :target: https://badge.fury.io/py/aioriak 
    :alt: PyPI version 


Asyncio (:pep:`3156`) Riak client library.
This project is based on official Bash python client library
(https://github.com/basho/riak-python-client).

Features
--------

================================  ==============================
Riak KV operations                  Yes
Riak Datatypes                      Yes
Riak BucketTypes                    Yes
Custom resolver                     Yes
Node list support                   No
Custom quorum                       No
Connections Pool                    No
Operations timout                   No
Security                            No
Riak Search                         No
MapReduce                           No
Tested python versions              `3.5.0, 3.5.1 <travis_>`_
Tested Riak versions                `2.1.3, 2.1.4 <travis_>`_
================================  ==============================

Documentation
-------------
You can read the docs here: `Documentation`_

Installation
------------

The easiest way to install aioriak is by using the package on PyPi::

    pip install aioriak

Requirements
------------

- Python >= 3.5
- riak>=2.1.3

Using exampe
------------

.. code-block:: python

    client = await RiakClient.create('localhost', loop=loop)
    bucket_type = client.bucket_type('default')
    bucket = bucket_type.bucket('example')
    obj = await bucket.get('key')
    print(obj.data)

Testing
-------

Docker based testing
--------------------

You can use docker for running:

.. code-block:: bash
    DOCKER_CLUSTER=1 python setup.py test

Contribute
----------

- Issue Tracker: https://github.com/rambler-digital-solutions/aioriak/issues
- Source Code: https://github.com/rambler-digital-solutions/aioriak

Feel free to file an issue or make pull request if you find any bugs or have
some suggestions for library improvement.

License
-------

The aioriak is offered under `MIT license`_.

----

.. _MIT license: https://raw.githubusercontent.com/rambler-digital-solutions/aioriak/master/LICENSE.txt
.. _travis: https://travis-ci.org/rambler-digital-solutions/aioriak
.. _Documentation: http://aioriak.readthedocs.io/
