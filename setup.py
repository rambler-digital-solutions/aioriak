from setuptools import setup, find_packages
from commands import (docker_build, docker_start, docker_stop, setup_riak,
                      create_bucket_types, Test)

setup(
    name='aioriak',
    version='0.1.0',
    description='Async implementation of Riak DB python client',
    author='Makc Belousov',
    author_email='m.belousov@rambler-co.ru',
    url='https://github.com/rambler-digital-solutions/aioriak',
    keywords='riak asyncio client',
    packages=find_packages(exclude=('*.tests',)),
    include_package_data=True,
    zip_safe=False,
    license='MIT',
    install_requires=[
        'python3-riak-pb==2.1.0.6',
        'riak==2.3.0',
    ],
    tests_require=['nose==1.3.7',
                   'coverage==4.0.3'],
    cmdclass={
        'test': Test,
        'docker_build': docker_build,
        'docker_start': docker_start,
        'docker_stop': docker_stop,
        'setup_riak': setup_riak,
        'create_bucket_types': create_bucket_types,
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
