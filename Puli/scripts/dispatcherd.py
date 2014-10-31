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
import subprocess


#
# Init singleton object holding reloadable config values
# Must be done in the very first place because some import might ask for config value
#
from octopus.dispatcher import settings
from octopus.core import singletonconfig

singletonconfig.load(settings.CONFDIR + "/config.ini")

#
# Init the rest of dispatcher app
#
from octopus.core.framework import WSAppFramework
from octopus.dispatcher.webservice.webservicedispatcher import WebServiceDispatcher
from octopus.dispatcher.dispatcher import Dispatcher


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

    mainLog = os.path.join(settings.LOGDIR, "dispatcher.log")
    assignLog = os.path.join(settings.LOGDIR, "assign.log")

    fileHandler = logging.handlers.RotatingFileHandler(
        mainLog,
        maxBytes=singletonconfig.get('CORE', 'LOG_SIZE'),
        backupCount=singletonconfig.get('CORE', 'LOG_BACKUPS'),
        encoding="UTF-8")

    assignHandler = logging.handlers.RotatingFileHandler(
        assignLog,
        maxBytes=singletonconfig.get('CORE', 'LOG_SIZE'),
        backupCount=singletonconfig.get('CORE', 'LOG_BACKUPS'),
        encoding="UTF-8")

    fileHandler.setFormatter(logging.Formatter("%(asctime)s %(name)10s %(levelname)s %(message)s"))
    assignHandler.setFormatter(logging.Formatter("%(asctime)s %(name)10s %(levelname)s %(message)s"))

    logLevel = logging.DEBUG if options.DEBUG else singletonconfig.get('CORE', 'LOG_LEVEL')

    # Must be set otherwise it will receive the statsLog data, but not higher than DEBUG otherwise we might loose some info if reconfig with higher lvl
    fileHandler.setLevel(logging.DEBUG)

    # Create main logger
    logging.getLogger().addHandler(fileHandler)
    logging.getLogger().setLevel(logLevel)

    # Create a specific logger for assignment information (force level to INFO)
    logging.getLogger('assign').addHandler(assignHandler)
    logging.getLogger('assign').setLevel(logging.DEBUG)
    logging.getLogger('assign').propagate = False  # cut event to avoid getting this to the root log

    if options.CONSOLE and not options.DAEMONIZE:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logging.Formatter("%(asctime)s %(name)10s %(levelname)6s %(message)s", '%Y-%m-%d %H:%M:%S'))
        consoleHandler.setLevel(logLevel)
        logging.getLogger().addHandler(consoleHandler)

    logging.getLogger('main.dispatcher').setLevel(logLevel)
    logging.getLogger('main.webservice').setLevel(logging.ERROR)


def make_dispatcher():
    return WSAppFramework(applicationClass=Dispatcher, webServiceClass=WebServiceDispatcher, port=settings.PORT)


def main():
    options = process_args()
    setup_logging(options)

    logging.getLogger('main').warning("")
    logging.getLogger('main').warning("-----------------------------------------------")
    logging.getLogger('main').warning("Starting PULI server on port:%d.", settings.PORT)
    logging.getLogger('main').warning("-----------------------------------------------")
    logging.getLogger('main').warning(" version = %s" % settings.VERSION)
    logging.getLogger('main').warning(" command = %s" % " ".join(sys.argv))
    logging.getLogger('main').warning("  daemon = %r" % options.DAEMONIZE)
    logging.getLogger('main').warning(" console = %r" % options.CONSOLE)
    logging.getLogger('main').warning("    port = %s" % settings.PORT)
    logging.getLogger('main').warning("--")

    if options.DAEMONIZE:
        logging.getLogger('main').warning("make current process a daemon and redirecting stdout/stderr to logfile")
        daemonize(settings.RUN_AS)

        try:
            # Redirect stdout and stderr to log file (using the first handler set in logging)
            sys.stdout = logging.getLogger().handlers[0].stream
            sys.stderr = logging.getLogger().handlers[0].stream
        except Exception:
            logging.getLogger('main').error("Unexpected error occured when redirecting stdout/stderr to logfile")

    logging.getLogger('main').warning("creating dispatcher main application")
    server = make_dispatcher()

    # Define a periodic callback to process DB/COMPLETION/ASSIGNMENT updates
    periodic = tornado.ioloop.PeriodicCallback(server.loop, singletonconfig.get('CORE', 'MASTER_UPDATE_INTERVAL'))
    periodic.start()
    try:
        logging.getLogger('main').warning("starting tornado main loop")
        tornado.ioloop.IOLoop.instance().start()
    except (KeyboardInterrupt, SystemExit):
        server.application.shutdown()

    # If restart flag is set (via /restart webservice)
    if server.application.restartService:
        logging.getLogger('main').warning("Restarting service...")

        try:
            # Restart server using a specific command
            subprocess.check_call(settings.RESTART_COMMAND.split())
        except subprocess.CalledProcessError, e:
            logging.getLogger('main').warning("Impossible to restart systemd unit (error: %s)" % e)
        except AttributeError, e:
            logging.getLogger('main').warning("Dispatcher settings do not define: RESTART_COMMAND")

    logging.getLogger('main').warning("Bye.")


if __name__ == '__main__':
    main()
