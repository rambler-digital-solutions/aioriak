from distutils.core import Command
from setuptools.command.test import test as TestCommand
from distutils.errors import DistutilsOptionError
from distutils import log
from subprocess import Popen, PIPE, call
import os
import sys
import time
import json


# Exception classes used by this module.
class CalledProcessError(Exception):
    """This exception is raised when a process run by check_call() or
    check_output() returns a non-zero exit status.
    The exit status will be stored in the returncode attribute;
    check_output() will also store the output in the output attribute.
    """
    def __init__(self, returncode, cmd, output=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % (self.cmd,
                                                                  self
                                                                  .returncode)


def check_output(*popenargs, **kwargs):
    '''Run command with arguments and return its output as a byte string.
    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.
    The arguments are the same as for the Popen constructor.  Example:
    >>> check_output(["ls", "-l", "/dev/null"])
    'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'
    The stdout argument is not allowed as it is used internally.
    To capture standard error in the result, use stderr=STDOUT.
    >>> import sys
    >>> check_output(["/bin/sh", "-c",
    ...               "ls -l non_existent_file ; exit 0"],
    ...              stderr=sys.stdout)
    'ls: non_existent_file: No such file or directory\n'
    '''
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be '
                         'overridden.')
    process = Popen(*popenargs, stdout=PIPE, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get('args')
        if cmd is None:
            cmd = popenargs[0]
        raise CalledProcessError(retcode, cmd, output=output)
    return output


def get_node_ip(node='riak01'):
    state = json.loads(check_output(['docker', 'inspect', node]).decode())
    return state[0]['NetworkSettings']['IPAddress']


class docker(object):
    docker_submodule_path = 'aioriak/tests/docker'

    def use_docker(self):
        return bool(int(os.environ.get('DOCKER_CLUSTER', 0)))

    def _check_retcode(self, retcode, args):
        if retcode:
            cmd = args[0]
            raise CalledProcessError(retcode, cmd)
        else:
            return True

    def cluster_is_started(self, node='riak01'):
        return node in check_output(['docker', 'ps']).decode()


class docker_build(Command, docker):
    user_options = []
    description = 'Setup riak cluster with docker'
    verbose = False

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))
        print(self.verbose)

    def finalize_options(self):
        pass

    def run(self):
        args = ['make', '-C', self.docker_submodule_path, 'build']
        retcode = call(args)
        self._check_retcode(retcode, args)

    sub_commands = [('docker_setup', lambda self: True)]


class docker_start(Command, docker):
    user_options = []
    description = 'Start riak cluster'
    verbose = False

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))
        os.environ['DOCKER_RIAK_AUTOMATIC_CLUSTERING'] = '1'
        os.environ['DOCKER_RIAK_CLUSTER_SIZE'] = '3'
        os.environ['DOCKER_RIAK_BACKEND'] = 'memory'

    def finalize_options(self):
        pass

    def run(self):
        args = ['make', '-C', self.docker_submodule_path, 'start-cluster']
        retcode = call(args)
        if self._check_retcode(retcode, args):
            time.sleep(3)


class docker_stop(Command, docker):
    user_options = []
    description = 'Stop riak cluster'
    verbose = False

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))

    def finalize_options(self):
        pass

    def run(self):
        if self.use_docker() and self.cluster_is_started():
            args = ['make', '-C', self.docker_submodule_path, 'stop-cluster']
            retcode = call(args)
            self._check_retcode(retcode, args)


class create_bucket_types(Command):
    '''
    Creates bucket-types appropriate for testing. By default this will create:
    * `pytest-maps` with ``{"datatype":"map"}``
    * `pytest-sets` with ``{"datatype":"set"}``
    * `pytest-counters` with ``{"datatype":"counter"}``
    * `pytest-consistent` with ``{"consistent":true}``
    * `pytest-write-once` with ``{"write_once": true}``
    * `pytest-mr`
    * `pytest` with ``{"allow_mult":false}``
    '''

    description = "create bucket-types used in integration tests"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script')
    ]

    _props = {
        'pytest-maps': {'datatype': 'map'},
        'pytest-sets': {'datatype': 'set'},
        'pytest-counters': {'datatype': 'counter'},
        'pytest-consistent': {'consistent': True},
        'pytest-write-once': {'write_once': True},
        'pytest-mr': {},
        'pytest': {'allow_mult': False}
    }

    def initialize_options(self):
        self.riak_admin = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        for name in self._props:
            self._create_and_activate_type(name, self._props[name])

    def check_btype_command(self, *args):
        cmd = self._btype_command(*args)
        return check_output(cmd)

    def _create_and_activate_type(self, name, props):
        # Check status of bucket-type
        exists = False
        active = False
        try:
            status = self.check_btype_command('status', name)
        except CalledProcessError as e:
            status = e.output

        exists = ('not an existing bucket type' not in status.decode('ascii'))
        active = ('is active' in status.decode('ascii'))

        if exists or active:
            log.info('Update {} bucket-type'.format(name))
            self.check_btype_command('update', name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))
        else:
            log.info('Create  {} bucket-type'.format(name))
            self.check_btype_command('create', name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))

        if not active:
            log.info('Activate {} bucket-type'.format(name))
            self.check_btype_command('activate', name)

    def _btype_command(self, *args):
        cmd = self.riak_admin + ['bucket-type']
        cmd.extend(args)
        return cmd


class setup_riak(Command, docker):
    user_options = []
    description = 'Setup riak cluster'
    verbose = False

    user_options = []

    def get_riak_admin(self):
        return os.environ.get('RIAK_ADMIN', None)

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))
        self.riak_admin = self.get_riak_admin()

    def finalize_options(self):
        if self.riak_admin is None and not self.use_docker():
            raise DistutilsOptionError("riak-admin option not set")
        if self.use_docker():
            self.riak_admin = ['docker', 'exec', '-i', '-t', 'riak01',
                               'riak-admin']
        else:
            self.riak_admin = self.riak_admin.split()

    def run(self):
        bucket = self.distribution.get_command_obj('create_bucket_types')
        bucket.riak_admin = self.riak_admin

        for cmd_name in self.get_sub_commands():
            self.run_command(cmd_name)

    sub_commands = [
        ('docker_build', lambda self: self.use_docker()),
        ('docker_start',
         lambda self: self.use_docker() and not self.cluster_is_started()),
        ('create_bucket_types', None)]


class Test(TestCommand, docker):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import nose
        result = nose.run(argv=['nosetests'])
        if self.use_docker():
            self.run_command('docker_stop')
        sys.exit(not result)
