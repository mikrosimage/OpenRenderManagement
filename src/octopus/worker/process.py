'''
Created on Sep 16, 2009

@author: bud
'''

import logging
import os
import subprocess

LOGGER = logging.getLogger("process")
CLOSE_FDS = (os.name != 'nt')

def spawnCommandWatcher(pidfile, logfile, args, env):
    '''
    logfile is a file object
    '''
    devnull = file(os.devnull, "r")
    # normalize environment
    envN = {}
    for key in env:
        envN[str(key)] = str(env[key])
    pid = subprocess.Popen(args, bufsize=0, stdin=devnull, stdout=logfile,
                           stderr=subprocess.STDOUT, close_fds=CLOSE_FDS,
                           env=envN).pid
    file(pidfile, "w").write(str(pid))
    return CommandWatcherProcess(pidfile, pid)


class CommandWatcherProcess(object):
    def __init__(self, pidfile, pid):
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

