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

.. image:: https://pyup.io/repos/github/rambler-digital-solutions/aioriak/shield.svg
     :target: https://pyup.io/repos/github/rambler-digital-solutions/aioriak/
     :alt: Updates


Asyncio (:pep:`3156`) Riak client library.
This project is based on official Basho python client library
(https://github.com/basho/riak-python-client).

Features
--------

================================  ==============================
Riak KV operations                  Yes
Riak Datatypes                      Yes
Riak BucketTypes                    Yes
Custom resolver                     Yes
Node list support                   WIP
Custom quorum                       No
Connections Pool                    No
Operations timeout                  No
Security                            No
Riak Search                         WIP
MapReduce                           WIP
Tested python versions              `3.5, 3.6 <travis_>`__
Tested Riak versions                `2.1.4, 2.2.3 <travis_>`__
================================  ==============================

Documentation
-------------
You can read the docs here: `Documentation <Docs_>`__

Installation
------------

The easiest way to install aioriak is by using the package on PyPi::

    pip install aioriak

Requirements
------------

- Python >= 3.5
- riak>=2.7.0

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
.. _Docs: http://aioriak.readthedocs.io/
