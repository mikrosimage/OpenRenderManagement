#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Disk And Execution MONitor (Daemon)

Configurable daemon behaviors:

   1.) The current working directory set to the "/" directory.
   2.) The current file creation mode mask set to 0.
   3.) Close all open files (1024).
   4.) Redirect standard I/O streams to "/dev/null".

A failed call to fork() now raises an exception.

References:
   1) Advanced Programming in the Unix Environment: W. Richard Stevens
   2) Unix Programming Frequently Asked Questions:
         http://www.erlenstar.demon.co.uk/unix/faq_toc.html
"""

__author__ = 'Chad J. Schroeder'
__copyright__ = 'Copyright (C) 2005 Chad J. Schroeder'

__revision__ = '$Id$'
__version__ = '0.2'

# Standard Python modules.

import os
import subprocess
import time
import logging
import pwd
import atexit
import signal
import sys
import optparse

# Default daemon parameters.
# File mode creation mask of the daemon.

# UMASK = 0

# Default working directory for the daemon.

# WORKDIR = '/'

# # Default maximum for the number of available file descriptors.

# MAXFD = 1024

# PID file 
PIDFILE = "/var/run/puli/respawner.pid"


# # The standard I/O file descriptors are redirected to /dev/null by default.

# if hasattr(os, 'devnull'):
#     REDIRECT_TO = os.devnull
# else:
#     REDIRECT_TO = '/dev/null'

def daemonize(username=""):
    """
    Detach a process from the controlling terminal and run it in the
    background as a daemon.
    """

    if os.fork() != 0:
        os._exit(0)

    os.setsid()
    
    if username:
        uid = pwd.getpwnam(username)[2]
        os.setuid(uid)
    
    if os.fork() != 0:
        os._exit(0)

    # create the pidfile
    pidfile = file( PIDFILE, "w" )
    pidfile.write("%d\n" % os.getpid())
    pidfile.close()

    # register a cleanup callback
    pidfile = os.path.abspath( PIDFILE )
    def delpidfile():
        os.remove(pidfile)
    atexit.register(delpidfile)
    def delpidfileonSIGTERM(a, b):
        os.remove(pidfile)
        sys.exit()
    signal.signal(signal.SIGTERM, delpidfileonSIGTERM)

    # Redirect standard ios
    os.chdir("/")
    f = os.open(os.devnull, os.O_RDONLY)
    os.dup2(f, sys.stdin.fileno())
    os.close(f)
    f = os.open(os.devnull, os.O_WRONLY)
    os.dup2(f, sys.stdout.fileno())
    os.close(f)
    f = os.open(os.devnull, os.O_WRONLY)
    os.dup2(f, sys.stderr.fileno())
    os.close(f)


def pollRestartFile( pFile ):
    """
    Called periodically (every REFRESH_DELAY) to check if the restart file exists.
    If it does, it restarts the puliworker service (using systemd)

    :param pFile: absolute path of the "restart file" to check
    :raise Exception: when the worker cannot be restarted (however in daemon mode exception are dumped in /var/log/messages only)
    """
    try:
        if os.path.isfile( pFile ):
            try:

                # stop the worker
                logging.info( "Stopping the worker" )
                # subprocess.call(["/usr/bin/sudo", "/etc/init.d/puliworker", "stop"])
                subprocess.call(["/usr/bin/sudo", "systemctl", "stop", "puliworker.service"])

                # remove killfile and restartfile
                logging.info("Removing comtrol files:" )
                logging.info("  RESTART_FILE = %s" % options.RESTART_FILE )
                logging.info("     KILL_FILE = %s" % options.KILL_FILE )
                os.remove( options.RESTART_FILE )
                os.remove( options.KILL_FILE )

                # start the worker
                logging.info( "Starting the worker" )
                # subprocess.call(["/usr/bin/sudo", "/etc/init.d/puliworker", "start"])
                subprocess.call(["/usr/bin/sudo", "systemctl", "start", "puliworker.service"])

            except Exception, e:
                logging.error("Impossible to restart the worker properly: %r", e)
    except Exception, e:
        logging.error("Error checking the restartfile: %r", e)


def process_args():
    parser = optparse.OptionParser()

    parser.add_option("-K", "--kill-file", action="store", dest="KILL_FILE", help="change the kill file", default="/tmp/render/killfile")
    parser.add_option("-R", "--restart-file", action="store", dest="RESTART_FILE", help="change the restart file", default="/tmp/render/restartfile")
    parser.add_option("-D", "--refresh-delay", action="store", dest="REFRESH_DELAY", type="int", help="set a refresh in seconds", default=5)

    return parser.parse_args()


if __name__ == '__main__':

    logging.basicConfig( format='%(asctime)s - %(levelname)s - %(message)s',
                         filename="/var/log/puli/respawner.log",
                         level=logging.DEBUG )

    logging.info("")
    logging.info("----------------------------------")
    logging.info("Parse options and arguments.")
    options, args = process_args()

    print type(options.REFRESH_DELAY)

    logging.info("Create daemon.")
    daemonize()

    logging.info("Start monitoring:")
    logging.info("  REFRESH_DELAY = %s" % options.REFRESH_DELAY )
    logging.info("   RESTART_FILE = %s" % options.RESTART_FILE )
    logging.info("      KILL_FILE = %s" % options.KILL_FILE )
    while True:
        pollRestartFile( options.RESTART_FILE )
        time.sleep( options.REFRESH_DELAY )
