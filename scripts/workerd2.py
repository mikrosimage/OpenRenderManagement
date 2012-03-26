#! /usr/bin/env python
'''
Created on Jul 19, 2010

@author: dev
'''


import logging
import optparse
import os
import sys
import atexit

from octopus.worker import make_worker, settings

def daemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    # Perform first fork.
    try:
        pid = os.fork( )
        if pid > 0:
            sys.exit(0) # Exit first parent.
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %sn" % (e.errno, e.strerror))
        sys.exit(1)
    # Decouple from parent environment.
    os.chdir("/")
    os.umask(0)
    os.setsid( )
    # Perform second fork.
    try:
        pid = os.fork( )
        if pid > 0:
            sys.exit(0) # Exit second parent.
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %sn" % (e.errno, e.strerror))
        sys.exit(1)
    # The process is now daemonized, redirect standard file descriptors.
    for f in sys.stdout, sys.stderr: f.flush( )
    si = file(stdin, 'r')
    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(si.fileno( ), sys.stdin.fileno( ))
    os.dup2(so.fileno( ), sys.stdout.fileno( ))
    os.dup2(se.fileno( ), sys.stderr.fileno( ))


def process_args():
    parser = optparse.OptionParser()
    parser.add_option("-P", "--pid-file", action="store", dest="PIDFILE", help="change the pid file")
    parser.add_option("-W", "--commandwatchers-pid-dir", action="store", dest="PID_DIR", help="change the directory where pid files for command watchers are stored")
    parser.add_option("-d", "--daemon", action="store_true", dest="DAEMONIZE", default=False, help="daemonize the dispatcher")
    parser.add_option("-b", "--bind", action="store", type="string", dest="ADDRESS", metavar="HOST", help="change the HOST the web service is bound on")
    parser.add_option("-p", "--port", action="store", type="int", dest="PORT", metavar="PORT", help="change the PORT the web service is listening on")
    parser.add_option("-u", "--run-as", action="store", type="string", dest = "RUN_AS", metavar="USER", help="run the dispatcher as USER")
    parser.add_option("-D", "--debug", action="store_true", dest="DEBUG", help="changes the default log level to DEBUG")
    parser.add_option("-C", "--console", action="store_true", dest="CONSOLE", default=False, help="output logs to the console")
    options, args = parser.parse_args()
    if args:
        settings.loadSettingsFile(args[0])
    for setting in dir(settings):
        if hasattr(options, setting) and getattr(options, setting) is not None:
            setattr(settings, setting, getattr(options, setting))
    parser.destroy()
    return options

def setup_logging(options):
    if not os.path.exists(settings.LOGDIR):
        os.makedirs(settings.LOGDIR, 0755)
    
    logFile = os.path.join(settings.LOGDIR, "worker_%s_%d.log" % (settings.ADDRESS, settings.PORT))
    
    fileHandler = logging.FileHandler(logFile, "w", "UTF-8")
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger = logging.getLogger() 
    logger.addHandler(fileHandler)
    
    if options.CONSOLE:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logging.Formatter("%(levelname)6s [%(name)s] %(message)s"))
        consoleHandler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(consoleHandler)

    debugLevel = logging.DEBUG if options.DEBUG else logging.INFO

    logging.getLogger('worker').setLevel(debugLevel)
    logging.getLogger('webservice').setLevel(debugLevel)

def main():
    options = process_args()
    setup_logging(options)
    workerApplication = make_worker()
    if options.DAEMONIZE:
        daemonize()
    workerApplication.mainLoop()

if __name__ == '__main__':
    main()
