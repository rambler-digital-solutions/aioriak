.. aioriak documentation master file, created by
    sphinx-quickstart on Sat Apr  2 10:53:29 2016.
    You can adapt this file completely to your liking, but it should at least
    contain the root `toctree` directive.

Welcome to aioriak's documentation!
===================================

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
Node list support                   WIP
Custom quorum                       No
Connections Pool                    Yes
Operations timout                   No
Security                            No
Riak Search                         WIP
MapReduce                           WIP
Tested python versions              `3.5, 3.6, 3.7 <travis_>`__
Tested Riak versions                `2.1.3, 2.1.4 <travis_>`__
================================  ==============================

Installation
------------

The easiest way to install aioriak is by using the package on PyPi::

    pip install aioriak

Requirements
------------

- Python >= 3.5
- riak==2.7.0

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

Contents
========

.. toctree::
    :maxdepth: 4
    
    client
    pool
    bucket
    object
    datatypes

#    query
#    security
#    advanced

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _MIT license: https://raw.githubusercontent.com/rambler-digital-solutions/aioriak/master/LICENSE.txt
.. _travis: https://travis-ci.org/rambler-digital-solutions/aioriak
