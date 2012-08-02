#! /usr/bin/env python
'''
Created on Aug 10, 2009

@author: bud
'''

import logging
import logging.handlers
import optparse
import os
import sys
import atexit
import signal

from octopus.worker import make_worker, settings

def daemonize(username=""):
    if os.fork() != 0:
        os._exit(0)
    os.setsid()
    if os.name != 'nt':
        import pwd
        if username:
            uid = pwd.getpwnam(username)[2]
            os.setuid(uid)
    if os.fork() != 0:
        os._exit(0)
    # create the pidfile
    pidfile = file(settings.PIDFILE, "w")
    pidfile.write("%d\n" % os.getpid())
    pidfile.close()
    # register a cleanup callback
    pidfile = os.path.abspath(settings.PIDFILE)
    def delpidfile():
        os.remove(pidfile)
    atexit.register(delpidfile)
    def delpidfileonSIGTERM(a, b):
        sys.exit()
    signal.signal(signal.SIGTERM, delpidfileonSIGTERM)
    #
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
    
    logFile = os.path.join(settings.LOGDIR, "worker.log")
    
#    fileHandler = logging.FileHandler(logFile, "w", "UTF-8")
    fileHandler = logging.handlers.RotatingFileHandler(logFile, 'w', 1048576, 1, "UTF-8")
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
        daemonize(settings.RUN_AS)
    workerApplication.mainLoop()

if __name__ == '__main__':
    main()
