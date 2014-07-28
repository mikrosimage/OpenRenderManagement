#!/usr/bin/python2.6
# -*- coding: utf8 -*-
from __future__ import with_statement

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"

from fabric.api import *
from fabric.contrib.console import confirm
from fabric.colors import green, blue, red

env.timeout = 5

env.hosts = ['vfxpc64']
# env.user = 'render'
# env.password = 'r3nd3r'
env.target_path = '/datas/jsa/puli_runtime'
env.shared_path = '/datas/jsa/puli_shared'
env.source_path = '/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli'
env.disable_known_hosts = True

def deploy_server( source_path=env.source_path, target_path=env.target_path ):

    print(green("Deploy puli server", bold=True))
    print(green(" - source path = %s" % source_path, bold=True))
    print(green(" - target path = %s" % target_path, bold=True))
    print(green(" - target host = %s" % env.hosts, bold=True))
    print(green(" - steps:", bold=True))
    print(green("   1. install core apps", bold=True))
    print(green("   2. install API files", bold=True))
    print(green("   3. install launch scripts", bold=True))

    result = prompt(green("\nContinue ?", bold=True), default='y')
    if result != 'y':
        abort("Interrupted by user.") 


    run("mkdir -p %s" % target_path)
    print(blue("Install core apps", bold=True))
    run("rsync -r %s/src/octopus %s" % (source_path, target_path))

    print(blue("Install API", bold=True))
    run("mkdir -p %s/puliclient" % target_path)
    run("rsync -r %s/src/puliclient/__init__.py %s/puliclient" % (source_path, target_path))
    run("rsync -r %s/src/puliclient/jobs.py %s/puliclient" % (source_path, target_path))

    print(blue("Install scripts", bold=True))
    run("mkdir -p %s/scripts" % target_path)
    run("rsync -r %s/src/puliclient/__init__.py %s/puliclient" % (source_path, target_path))

    print(blue("Install startup scripts", bold=True))
    run("mkdir -p %s/scripts" % target_path)
    run("rsync -r %s/scripts/dispatcherd.py %s/scripts" % (source_path, target_path))
    
    print(blue("Install util scripts", bold=True))
    run("rsync -r %s/scripts/util/jobcleaner.py %s/scripts" % (source_path, target_path))

def deploy_server_conf( source_path=env.source_path, target_path=env.target_path ):

    print(green("Deploy config files on host(s): %s"%env.hosts, bold=True))
    print(green(" - source path = %s" % source_path, bold=True))
    print(green(" - target path = %s" % target_path, bold=True))
    print(green(" - target host = %s" % env.hosts, bold=True))
    print(green(" - Copy following file:", bold=True))
    print(green("   - config.ini", bold=True))
    print(green("   - licences.lst", bold=True))
    
    result = prompt(green("\nContinue ?", bold=True), default='y')
    if result != 'y':
        abort("Interrupted by user.") 

    print(blue("Install config", bold=True))
    run("mkdir -p %s/conf" % target_path)
    run("rsync -r %s/etc/puli/config.ini %s/conf" % (source_path, target_path))
    run("rsync -r %s/etc/puli/licences.lst %s/conf" % (source_path, target_path))


def deploy_worker( dry_run=True ):
    print(blue("Deploy puli worker on network path"))

def deploy_tools( dry_run=True ):
    print(blue("Deploy puli tools on network path"))

