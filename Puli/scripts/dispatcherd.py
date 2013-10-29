#!/usr/bin/env python
'''
Created on Aug 10, 2009

@author: bud
'''

import logging
import logging.handlers
import optparse
import os
import pwd
import sys
import atexit
import signal
import tornado
import time

#
# Init singleton object holding reloadable config values
# Must be done in the very first place because some import might ask for config value
#
from octopus.dispatcher import settings
from octopus.core import singletonconfig

singletonconfig.load( settings.CONFDIR + "/config.ini" )

#
# Init the rest of dispatcher app
#
# from octopus.dispatcher import make_dispatcher
from octopus.core.framework import WSAppFramework
from octopus.dispatcher.webservice.webservicedispatcher import WebServiceDispatcher
from octopus.dispatcher.dispatcher import Dispatcher

os.path.dirname(__file__) + "/../logs/dispatcher/dispatcher.log"


def daemonize(username=""):
    if os.fork() != 0:
        os._exit(0)
    os.setsid()
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
    parser.add_option("-d", "--daemon", action="store_true", dest="DAEMONIZE", default=False, help="daemonize the dispatcher")
    parser.add_option("-b", "--bind", action="store", type="string", dest="ADDRESS", metavar="HOST", help="change the HOST the web service is bound on")
    parser.add_option("-p", "--port", action="store", type="int", dest="PORT", metavar="PORT", help="change the PORT the web service is listening on")
    parser.add_option("-u", "--run-as", action="store", type="string", dest="RUN_AS", metavar="USER", help="run the dispatcher as USER")
    parser.add_option("-D", "--debug", action="store_true", dest="DEBUG", help="changes the default log level to DEBUG")
    parser.add_option("-C", "--console", action="store_true", dest="CONSOLE", default=False, help="output logs to the console")
    options, args = parser.parse_args()
    # override defaults with settings from file
    if args:
        settings.loadSettingsFile(args[0])
    # override settings with options
    for setting in dir(settings):
        if hasattr(options, setting) and getattr(options, setting) is not None:
            setattr(settings, setting, getattr(options, setting))
    return options

def setup_logging(options):
    if not os.path.exists(settings.LOGDIR):
        os.makedirs(settings.LOGDIR, 0755)

    logFile = os.path.join(settings.LOGDIR, "dispatcher.log")


    fileHandler = logging.handlers.RotatingFileHandler(logFile, 
                    maxBytes=singletonconfig.get('CORE','LOG_SIZE'), 
                    backupCount=singletonconfig.get('CORE','LOG_BACKUPS'), 
                    encoding="UTF-8")
    # fileHandler = logging.handlers.RotatingFileHandler(logFile, maxBytes=2097152, backupCount=10, encoding="UTF-8")

    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger = logging.getLogger()
    debugLevel = logging.DEBUG if options.DEBUG else logging.WARNING
    logger.setLevel(debugLevel)
    logger.addHandler(fileHandler)

    if options.CONSOLE and not options.DAEMONIZE:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logging.Formatter("%(asctime)s %(name)10s %(levelname)6s %(message)s", '%Y-%m-%d %H:%M:%S'))
        consoleHandler.setLevel(debugLevel)
        logger.addHandler(consoleHandler)

    logging.getLogger('dispatcher').setLevel(debugLevel)
    logging.getLogger('webservice').setLevel(logging.ERROR)


def make_dispatcher():
    return WSAppFramework( applicationClass=Dispatcher, webServiceClass=WebServiceDispatcher, port=settings.PORT )

def main():
    options = process_args()
    setup_logging(options)

    if options.DAEMONIZE:
        daemonize(settings.RUN_AS)

    dispatcherApplication = make_dispatcher()

    periodic = tornado.ioloop.PeriodicCallback( dispatcherApplication.loop, singletonconfig.get('CORE','MASTER_UPDATE_INTERVAL') )
    periodic.start()
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt, SystemExit:
        logging.getLogger('dispatcher').info("Exit event caught: closing dispatcher...")

if __name__ == '__main__':
    main()
#    import cProfile
#    cProfile.run("main()")
