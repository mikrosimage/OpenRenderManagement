'''
Used by Worker to spawn a new process.
'''

__author__      = "Olivier Derpierre"
__copyright__   = "Copyright 2009, Mikros Image"

import logging
import os
import subprocess
import resource
from octopus.worker import settings

LOGGER = logging.getLogger("process")
CLOSE_FDS = (os.name != 'nt')


def setlimits():
    # the use of os.setsid is necessary to create a processgroup properly for the commandwatcher
    # it creates a new session in which the cmdwatcher is the leader of the new process group
    os.setsid()

    # set the limit of open files for ddd
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        if settings.LIMIT_OPEN_FILES < hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (settings.LIMIT_OPEN_FILES, hard))
    except Exception,e:
        LOGGER.error("Setting ressource limit failed: RLIMT_NOFILE [%r,%r] --> [%r,%r]" % (soft, hard, settings.LIMIT_OPEN_FILES, hard) )
        raise e


def spawnCommandWatcher(pidfile, logfile, args, env):
    '''
    logfile is a file object
    '''
    devnull = file(os.devnull, "r")
    # normalize environment
    envN = os.environ.copy()
    for key in env:
        envN[str(key)] = str(env[key])

    LOGGER.info("Starting subprocess, log: %r, args: %r" % (logfile, args) )        
    try:
        # pid = subprocess.Popen(args, bufsize=-1, stdin=devnull, stdout=logfile,
        #                    stderr=subprocess.STDOUT, close_fds=CLOSE_FDS,
        #                    preexec_fn=setlimits, env=envN).pid
        process = subprocess.Popen(args, bufsize=-1, stdin=devnull, stdout=logfile,
                           stderr=subprocess.STDOUT, close_fds=CLOSE_FDS,
                           preexec_fn=setlimits, env=envN)

    except Exception,e:
        LOGGER.error("Impossible to start subprocess: %r" % e)        
        raise e

    file(pidfile, "w").write(str(process.pid))
    return CommandWatcherProcess(process, pidfile, process.pid)


class CommandWatcherProcess(object):
    def __init__(self, process, pidfile, pid):
        self.process = process
        self.pidfile = pidfile
        self.pid = pid

    def kill(self):
        '''Kill the process.'''
        if os.name != 'nt':
            from signal import SIGTERM
            from errno import ESRCH

            # PHASE 1
            try:
                # do not kill the process, kill the whole process group!
                LOGGER.warning("Trying to kill process group %s" % str(self.pid))
                os.killpg(self.pid, SIGTERM)
                return
            except OSError, e:
                LOGGER.error("A problem occured")
                # If the process is dead already, let it rest in peace.
                # Else, we have a problem, so reraise.
                if e.args[0] != ESRCH:
                    raise

            # PHASE 2
            try:
                # the commandwatcher did not have time to setpgid yet, let's just kill the process
                # FIXME there still is room for a race condition there
                os.kill(self.pid, SIGTERM)
            except OSError, e:
                # If the process is dead already, let it rest in peace.
                # Else, we have a problem, so reraise.
                if e.args[0] != ESRCH:
                    raise

            # PHASE 3
            try:
                # attempt to fix a race condition:
                # if we kill the watcher but the watcher had the time to
                # create processgroup and start another process in between
                # phases 1 and 2, then attempt to kill the processgroup.
                os.killpg(self.pid, SIGTERM)
            except OSError, e:
                # If the process is dead already, let it rest in peace.
                # Else, we have a problem, so reraise.
                if e.args[0] != ESRCH:
                    raise
        else:
            os.popen("taskkill /PID  %d" % self.pid)
