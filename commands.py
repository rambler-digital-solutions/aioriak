from distutils.core import Command
from subprocess import Popen, PIPE, call
import os
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


class docker:
    docker_submodule_path = 'aioriak/tests/docker'

    def _check_retcode(self, retcode, args):
        if retcode:
            cmd = args[0]
            raise CalledProcessError(retcode, cmd)
        else:
            return True


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
        os.environ['DOCKER_RIAK_CLUSTER_SIZE'] = '5'
        os.environ['DOCKER_RIAK_BACKEND'] = 'memory'

    def finalize_options(self):
        pass

    def run(self):
        args = ['make', '-C', self.docker_submodule_path, 'start-cluster']
        retcode = call(args)
        if self._check_retcode(retcode, args):
            time.sleep(30)


class docker_stop(Command, docker):
    user_options = []
    description = 'Stop riak cluster'
    verbose = False

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))

    def finalize_options(self):
        pass

    def run(self):
        args = ['make', '-C', self.docker_submodule_path, 'stop-cluster']
        retcode = call(args)
        self._check_retcode(retcode, args)


class setup_riak(Command, docker):
    user_options = []
    description = 'Setup riak cluster'
    verbose = False

    def initialize_options(self):
        self.verbose = bool(int(os.environ.get('VERBOSE', 0)))
        self.use_docker = bool(int(os.environ.get('RIAK_CLUSTER', 0)))

    def finalize_options(self):
        pass

    def run(self):
        if self.use_docker:
            print('init riak docker cluster')
        else:
            print('setup riak instance')
