#! /usr/bin/env python
# coding: utf-8


"""
UNSUPPORTED: Avoid using this script as its behavior might not work as of version 1.7
"""

import sys
import getpass
import os

from puliclient import Graph, Task, TaskGroup, Command, RUNNING, DONE, CANCELED


def username():
    return getpass.getuser()


def gethost():
#    return os.getenv('PULIHOST', 'puliserver')
    return os.getenv('PULIHOST', 'localhost')


def getport():
    return int(os.getenv('PULIPORT', 8004))


def submit(job, poolName=None, maxRN=-1):
    g = Graph(job.name, job, username(), poolName, maxRN)
    print g.submit(gethost(), getport())


def compile(job):
    g = Graph(job.name, job, username())
    from pprint import pprint
    pprint(g.toRepresentation())


def process(script, compile=False):
    globals = {
        'TaskGroup': TaskGroup,
        'Task': Task,
        'Command': Command,
        'RUNNING': RUNNING,
        'DONE': DONE,
        'CANCELED': CANCELED,
        'submit': compile if compile else submit
    }
    execfile(script, globals)


def main():
    scripts = sys.argv[1:]
    compile = '-c' in sys.argv[2:]
    for script in scripts:
        process(script, compile)

if __name__ == '__main__':
    main()
