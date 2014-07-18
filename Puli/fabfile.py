from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.colors import green, blue, red

env.timeout = 1

env.hosts = ['vfxpc64']
# env.user = 'render'
# env.password = 'r3nd3r'
env.local_path = '/tmp/puli'
env.shared_path = '/tmp/shared/puli'
env.source_path = '/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli'
env.disable_known_hosts = True

def deploy_server( local_path=env.local_path ):

    print(green("Deploy puli server", bold=True))
    print(green(" - host = %s" % env.hosts, bold=True))
    print(green(" - install path = %s" % local_path, bold=True))
    print(green(" - steps:", bold=True))
    print(green("   1. install core apps", bold=True))
    print(green("   2. install API files", bold=True))
    print(green("   3. install launch scripts", bold=True))

    run("mkdir -p %s" % local_path)
    print(blue("Install core apps"))
    run("rsync -vr %s/src/octopus %s" % (env.source_path, local_path))

    print(blue("Install API", bold=True))
    run("mkdir -p %s/puliclient" % local_path)
    run("rsync -vr %s/src/puliclient/__init__.py %s/puliclient" % (env.source_path, local_path))
    run("rsync -vr %s/src/puliclient/jobs.py %s/puliclient" % (env.source_path, local_path))

    print(blue("Install scripts"))
    run("mkdir -p %s/scripts" % local_path)
    run("rsync -vr %s/src/puliclient/__init__.py %s/puliclient" % (env.source_path, local_path))

    print(blue("Install startup scripts"))
    run("mkdir -p %s/scripts" % local_path)
    run("rsync -vr %s/scripts/dispatcherd.py %s/scripts" % (env.source_path, local_path))
    run("rsync -vr %s/scripts/pulicleaner.py %s/scripts" % (env.source_path, local_path))

    print(blue("Install util scripts"))
    run("mkdir -p %s/scripts/util" % local_path)
    run("rsync -vr %s/scripts/util/jobcleaner.py %s/scripts" % (env.source_path, local_path))

def deploy_worker( dry_run=True ):
    print(blue("Deploy puli worker on network path"))

def deploy_tools( dry_run=True ):
    print(blue("Deploy puli tools on network path"))
