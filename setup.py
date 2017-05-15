from setuptools import setup, find_packages
import codecs
import os
from pip.req import parse_requirements


HERE = os.path.abspath(os.path.dirname(__file__))

test_reqs = parse_requirements('requirements-test.txt', session='pass')
test_requirements = [str(ir.req) for ir in test_reqs]


def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), 'rb', 'utf-8') as f:
        return f.read()


setup(
    name='aioriak',
    version='1.0.0',
    description='Async implementation of Riak DB python client',
    long_description=read('README.rst'),
    author='Makc Belousov',
    author_email='m.belousov@rambler-co.ru',
    url='https://github.com/rambler-digital-solutions/aioriak',
    keywords='riak asyncio client',
    packages=find_packages(exclude=('*.tests',)),
    include_package_data=True,
    zip_safe=False,
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=test_requirements,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
