#! /usr/bin/env python
# coding: utf-8

import sys
import getpass
import os
import subprocess

from octopus.client import api

def username():
    return getpass.getuser()

def gethost():
    return os.getenv('PULIHOST', 'localhost')

def getport():
    return int(os.getenv('PULIPORT', 8004))

def submit(job):
    g = api.Graph(job.name, job, username())
    from pprint import pprint
    pprint(g.toRepresentation())
    print g.submit(gethost(), getport())

def main():
    name = sys.argv[1]
    commandline = subprocess.list2cmdline(sys.argv[2:])
    arguments = {'args': commandline}
    runner = "octopus.core.jobtypes.runners.commandline.CommandLine"
    t = api.Task(name, arguments, runner)
    g = api.Graph(name, t, username())
    print g.submit(gethost(), getport())

if __name__ == '__main__':
    main()
