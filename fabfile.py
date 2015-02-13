#!/usr/bin/python2.6
# -*- coding: utf8 -*-
from __future__ import with_statement
"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2013, Mikros Image"

from fabric.api import *
from fabric.colors import green, blue

env.timeout = 5
env.disable_known_hosts = True

env.source_path = ''
env.target_path = ''
env.shared_path = ''
env.common_path = ''
env.logdir = ''
env.confdir = ''
env.local = False
env.force = False


@task()
def deploy_server(source_path=env.source_path, target_path=env.target_path, force=env.force):
    """
    Install dispatcher subsytem on server host

    Used when installing dispatcher subsystem locally on the server host.
    WARNING: it does not install config files (use deploy_server_conf in addition)

    Installation layout is the following:
    tartget_path/
        octopus/
            dispatcherd.py
            jobcleaner.py
        puliclient/
            __init__.py
           jobs.py
           runner.py
           server.py
        pulitools/
    """
    print ""
    print(green("Deploy puli server", bold=True))
    print(green(" - source path = %s" % source_path, bold=True))
    print(green(" - target path = %s" % target_path, bold=True))
    print(green(" - target host = %s" % env.hosts, bold=True))
    print(green(" - steps:", bold=True))
    print(green("   1. install core apps", bold=True))
    print(green("   2. install API files", bold=True))
    print(green("   3. install scripts", bold=True))

    if not force:
        result = prompt(green("\nContinue ?", bold=True), default='y')
        if result != 'y':
            abort("Interrupted by user.")

    run("sudo mkdir -p %s" % target_path)
    run("sudo rsync -r %s/src/ %s" % (source_path, target_path))
    run("sudo rsync -r %s/scripts/util/jobcleaner.py %s/scripts" % (source_path, target_path))

    # print(blue("Install API", bold=True))
    # # run("sudo mkdir -p %s/puliclient" % target_path)
    # run("sudo rsync -r %s/src/puliclient/__init__.py %s/puliclient" % (source_path, target_path))
    # run("sudo rsync -r %s/src/puliclient/jobs.py %s/puliclient" % (source_path, target_path))

    # print(blue("Install startup scripts", bold=True))
    # run("sudo mkdir -p %s/scripts" % target_path)
    # run("sudo rsync -r %s/scripts/dispatcherd.py %s/scripts" % (source_path, target_path))
    # run("sudo rsync -r %s/scripts/util/jobcleaner.py %s/scripts" % (source_path, target_path))


@task()
def deploy_server_conf(source_path=env.source_path, target_path=env.target_path):
    """
    Install config files on server host

    Layout:
    tartget_path/
        conf/
            config.ini
            licences.lst
    """
    print ""
    print(green("Deploy config files on host(s): %s" % env.hosts, bold=True))
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
    run("sudo mkdir -p %s/conf" % target_path)
    run("sudo rsync -r %s/etc/puli/config.ini %s/conf" % (source_path, target_path))
    run("sudo rsync -r %s/etc/puli/licences.lst %s/conf" % (source_path, target_path))


@task()
def deploy_on_shared_storage(source_path=env.source_path, shared_path=env.shared_path, force=env.force):
    """
    Install full distribution on a shared storage (i.e. dispatcher, worker, API and tools)
    """

    print ""
    print(green("Deploy sources, API and tools on network path", bold=True))
    print(green(" - source path = %s" % source_path, bold=True))
    print(green(" - shared path = %s" % shared_path, bold=True))
    print(green(" - steps:", bold=True))
    print(green("   1. install core apps", bold=True))
    print(green("   2. install API files", bold=True))
    print(green("   3. install scripts", bold=True))

    if not force:
        result = prompt(green("\nContinue ?", bold=True), default='y')
        if result != 'y':
            abort("Interrupted by user.")

    local("mkdir -p %s" % shared_path)
    local("rsync -r %s/src/ %s" % (source_path, shared_path))

    # print(blue("Install API", bold=True))
    # local("mkdir -p %s/puliclient" % shared_path)
    # local("rsync -r %s/src/puliclient/__init__.py %s/puliclient" % (source_path, shared_path))
    # local("rsync -r %s/src/puliclient/jobs.py %s/puliclient" % (source_path, shared_path))

    # print(blue("Install scripts", bold=True))
    # local("mkdir -p %s/scripts" % shared_path)
    # local("rsync -r %s/scripts/dispatcherd.py %s/scripts" % (source_path, shared_path))
    # local("rsync -r %s/scripts/workerd.py %s/scripts" % (source_path, shared_path))
    # local("rsync -r %s/scripts/util/jobcleaner.py %s/scripts" % (source_path, shared_path))


# @task()
# def deploy_tools_on_shared_storage(source_path=env.source_path, shared_path=env.shared_path):
#     """
#     Install tools sources on a shared storage
#     """
#     print ""
#     print(green("Deploy puli tools on network path", bold=True))
#     print(green(" - source path = %s" % source_path, bold=True))
#     print(green(" - shared path = %s" % shared_path, bold=True))

#     result = prompt(green("\nContinue ?", bold=True), default='y')
#     if result != 'y':
#         abort("Interrupted by user.")

#     local("mkdir -p %s" % shared_path)
#     print(blue("Install core apps", bold=True))
#     local("rsync -r %s/src/pulitools %s/src" % (source_path, shared_path))


@task()
def create_launcher(shared_path=env.shared_path, common_path=env.common_path):
    """
    Create launcher scripts for core tools: pul_query, pul_rn...
    """
    print ""
    print(green("Create launchers on a shared folder (must be listed in user's PATH)", bold=True))
    print(green(" - shared path = %s" % shared_path, bold=True))
    print(green(" - common path = %s" % common_path, bold=True))

    result = prompt(green("\nContinue ?", bold=True), default='y')
    if result != 'y':
        abort("Interrupted by user.")

    local("mkdir -p %s" % common_path)

    template = '''#!/bin/bash
export PYTHONPATH=__PULI_INSTALL_PATH__:${PYTHONPATH}
export PATH=__PULI_INSTALL_PATH__/__TOOL__:${PATH}

__EXEC__ "$@"
'''

    with hide('running', 'stdout'):
        # Replace install path, the folder in which the tools sources are installed.
        template = template.replace('__PULI_INSTALL_PATH__', shared_path)

        print(blue("Create pul_query", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_query")
        local("echo '%s' > %s/pul_query" % (currContent, common_path))
        local("chmod +x %s/pul_query" % common_path)

        print(blue("Create pul_rn", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_rn")
        local("echo '%s' > %s/pul_rn" % (currContent, common_path))
        local("chmod +x %s/pul_rn" % common_path)

        print(blue("Create pul_pause", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_set_pause --set 1")
        local("echo '%s' > %s/pul_pause" % (currContent, common_path))
        local("chmod +x %s/pul_pause" % common_path)

        print(blue("Create pul_cancel", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_set_status --set 5")
        local("echo '%s' > %s/pul_cancel" % (currContent, common_path))
        local("chmod +x %s/pul_cancel" % common_path)

        print(blue("Create pul_restart", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_set_status --set 1")
        local("echo '%s' > %s/pul_restart" % (currContent, common_path))
        local("chmod +x %s/pul_restart" % common_path)

        print(blue("Create pul_resume", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliquery")
        currContent = currContent.replace('__EXEC__', "pul_set_pause --set 0")
        local("echo '%s' > %s/pul_resume" % (currContent, common_path))
        local("chmod +x %s/pul_resume" % common_path)

        print(blue("Create pul_stats", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/statsviewer")
        currContent = currContent.replace('__EXEC__', "stats.py")
        local("echo '%s' > %s/pul_stats" % (currContent, common_path))
        local("chmod +x %s/pul_stats" % common_path)

        print(blue("Create puliexec", bold=True))
        currContent = template
        currContent = currContent.replace('__TOOL__', "pulitools/puliexec")
        currContent = currContent.replace('__EXEC__', "puliexec.py")
        local("echo '%s' > %s/puliexec" % (currContent, common_path))
        local("chmod +x %s/puliexec" % common_path)


@task()
def update_settings(target_path=env.target_path, log_dir=env.logdir, conf_dir=env.confdir, force=env.force, exec_local=env.local):
    """
    Insert path to LOGDIR and CONFDIR in dispatcher settings file.
    """

    print ""
    print(green("Update settings on server:", bold=True))
    print(green(" - target_path  = %s" % target_path, bold=True))
    print(green(" - log_dir = %s" % log_dir, bold=True))
    print(green(" - conf_dir = %s" % conf_dir, bold=True))
    if not force:
        result = prompt(green("\nContinue ?", bold=True), default='y')
        if result != 'y':
            abort("Interrupted by user.")

    settings_file = "%s/octopus/dispatcher/settings.py" % target_path

    if exec_local:
        local("sed -i \"s:__LOGDIR_PLACEHOLDER__:%s:g\" %s" % (log_dir, settings_file))
        local("sed -i 's:__CONFDIR_PLACEHOLDER__:%s:g' %s" % (conf_dir, settings_file))
    else:
        run("sudo sed -i 's:__LOGDIR_PLACEHOLDER__:%s:g' %s" % (log_dir, settings_file))
        run("sudo sed -i 's:__CONFDIR_PLACEHOLDER__:%s:g' %s" % (conf_dir, settings_file))
