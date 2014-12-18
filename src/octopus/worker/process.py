'''
Used by Worker to spawn a new process.
'''

__author__ = "Olivier Derpierre"
__copyright__ = "Copyright 2009, Mikros Image"

import logging
import os
import sys
import subprocess
import resource
from octopus.worker import settings

LOGGER = logging.getLogger("main.process")
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
    except Exception, e:
        LOGGER.error("Setting ressource limit failed: RLIMT_NOFILE [%r,%r] --> [%r,%r]" % (soft, hard, settings.LIMIT_OPEN_FILES, hard))
        raise e


def spawnRezManagedCommandWatcher(pidfile, logfile, args, watcherPackages, env):
    '''
    | Uses rez module to start a process with a proper rez env.

    :param pidfile: full path to the comand pid file (usally /var/run/puli/cw<command_id>.pid)
    :param logfile: file object to store process log content
    :param args:
    :param watcherPackages:
    :param env:

    :return: a CommandWatcherProcess object holding command watcher process handle
    '''
    try:
        from rez.resources import clear_cache
        from rez.resolved_context import ResolvedContext
        from rez.resolver import ResolverStatus
    except ImportError as e:
        LOGGER.error("Unable to load rez package in a rez managed environment.")
        raise e

    try:
        if watcherPackages is None:
            LOGGER.warning("No package specified for this command, it might not find the runner for this command.")
            watcherPackagesList = []
        elif type(watcherPackages) is str:
            watcherPackagesList = watcherPackages.split()
        else:
            watcherPackagesList = watcherPackages

        clear_cache()
        context = ResolvedContext(watcherPackagesList)
        success = (context.status == ResolverStatus.solved)
        if not success:
            context.print_info(buf=sys.stderr)
            raise

        # normalize environment
        envN = os.environ.copy()
        for key in env:
            envN[str(key)] = str(env[key])

        proc = context.execute_shell(
            command=args,
            shell=None,
            stdin=False,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            block=False,
            parent_environ=envN
        )

        LOGGER.info("Starting subprocess, log: %r, args: %r" % (logfile.name, args))
    except Exception as e:
        LOGGER.error("Impossible to start process: %s" % e)
        raise e

    file(pidfile, "w").write(str(proc.pid))
    return CommandWatcherProcess(proc, pidfile, proc.pid)


def spawnCommandWatcher(pidfile, logfile, args, env):
    '''
    logfile is a file object
    '''
    devnull = file(os.devnull, "r")

    # HACK prepend PYTHONPATH with mikros base path for old process
    # sys.path.insert(0, "/s/apps/lin/puli")
    # print "DBG pytpath: %r" % os.getenv("PYTHONPATH")
    # print "DBG syspath: %r" % sys.path
    # tmp = "/s/apps/lin/puli:%s" % os.getenv("PYTHONPATH")
    # os.putenv("PYTHONPATH", "/s/apps/lin/puli")

    # normalize environment
    envN = os.environ.copy()
    for key in env:
        envN[str(key)] = str(env[key])

    LOGGER.info("Starting subprocess, log: %r, args: %r" % (logfile.name, args))
    try:
        # pid = subprocess.Popen(args, bufsize=-1, stdin=devnull, stdout=logfile,
        #                    stderr=subprocess.STDOUT, close_fds=CLOSE_FDS,
        #                    preexec_fn=setlimits, env=envN).pid
        process = subprocess.Popen(
            args, bufsize=-1, stdin=devnull, stdout=logfile,
            stderr=logfile, close_fds=CLOSE_FDS,
            preexec_fn=setlimits, env=envN)

    except Exception, e:
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
            # import pudb;pu.db
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
