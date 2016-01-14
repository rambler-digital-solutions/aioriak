from setuptools import setup, find_packages

setup(
    name='aioriak',
    version='0.0.1',
    description='Async implementation of python client for Riak DB',
    author='Makc Belousov',
    author_email='m.belousov@rambler-co.ru',
    long_description='',
    url='',
    # package_dir={'': ''},
    packages=find_packages('', exclude=('*.tests',)),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'python3-riak-pb==2.1.0.6',
        'riak==2.3.0',
    ],
)
