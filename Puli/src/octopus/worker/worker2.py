#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import os
import sys
import time
import platform
try:
    import simplejson as json
except ImportError:
    import json
import subprocess
import multiprocessing
import re
import requests

from octopus.worker import settings
from octopus.core.enums import rendernode
from octopus.core.enums import command
from octopus.worker.model.command import Command
from octopus.commandwatcher import commandwatcher2 as commandwatcher
from octopus.worker.process import spawnCommandWatcher

LOGGER = logging.getLogger('worker')


class Worker2(object):
    """This is the main class of the puli worker."""

    class CommandWatcher(object):
        """Inner class that represents a command watcher."""
        def __init__(self):
            self.id = None
            self.processId = None
            self.startTime = None
            self.processObj = None
            self.timeOut = None
            self.commandId = None
            self.command = None
            self.modified = True
            self.finished = False

    @property
    def modifiedCommandWatchers(self):
        return (watcher for watcher in self.commandWatchers.values() if watcher.modified)

    @property
    def finishedCommandWatchers(self):
        return (watcher for watcher in self.commandWatchers.values() if watcher.finished and not watcher.modified)

    def __init__(self, framework):
        super(Worker2, self).__init__()
        logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
        LOGGER.info('Starting worker on %s:%d...', settings.ADDRESS,
                    settings.PORT)
        self.framework = framework
        self.commandWatchers = {}
        self.commands = {}
        self.port = settings.PORT
        self.computerName = '%s:%d' % (settings.ADDRESS, settings.PORT)

        # url to connect to
        self.url = 'http://%s:%d' % (settings.DISPATCHER_ADDRESS,
                                     settings.DISPATCHER_PORT)

        # sysinfos params
        self.lastSysInfosMessageTime = 0
        self.sysInfosMessagePeriod = 6

        # pid directory handling
        self.PID_DIR = os.path.dirname(settings.PIDFILE)
        if not os.path.isdir(self.PID_DIR):
            LOGGER.warning('Worker pid directory does not exist, creating...')
            try:
                os.makedirs(self.PID_DIR, 0777)
                LOGGER.info('Worker pid directory created.')
            except OSError:
                LOGGER.error('Failed to create pid directory.')
                sys.exit(1)
        elif not os.access(self.PID_DIR, os.R_OK | os.W_OK):
            LOGGER.error('Missing read or write access on %s',
                         self.PID_DIR)
            sys.exit(1)

        # initial status, according to the presence of the killfile
        if os.path.isfile(settings.KILLFILE):
            self.status = rendernode.RN_PAUSED
        else:
            self.status = rendernode.RN_IDLE
        self.toberestarted = False

    def prepare(self):
        for name in (name for name in dir(settings) if name.isupper()):
            LOGGER.info('settings.%s = %r' % (name, getattr(settings,
                        name)))
        # try to register the worker
        self.registerWorker()

    def updateCommandWatcher(self, commandWatcher):
        cmd = commandWatcher.command
        dct = {}
        if cmd.completion is not None:
            dct["completion"] = cmd.completion
        if cmd.status is not None:
            dct["status"] = cmd.status
        if cmd.validatorMessage is not None:
            dct["validatorMessage"] = cmd.validatorMessage
        if cmd.errorInfos is not None:
            dct["errorInfos"] = cmd.errorInfos
        dct['message'] = cmd.message
        dct['id'] = cmd.id
        r = requests.put(self.url + "/rendernodes/%s/commands/%d/"
                         % (self.computerName, commandWatcher.commandId),
                         data=json.dumps(dct), stream=False)
        if r.status_code == requests.codes.ok:
            commandWatcher.modified = False
        elif r.status_code == 404:
            LOGGER.warning('Removing stale command %d' % commandWatcher.commandId)
            self.removeCommandWatcher(commandWatcher)
        else:
            LOGGER.warning('Update of command %d failed. (%d)' % (commandWatcher.commandId, r.status_code))

    def removeCommandWatcher(self, commandWatcher):
        LOGGER.info('Removing command watcher for command %d' % commandWatcher.commandId)
        del self.commandWatchers[commandWatcher.commandId]
        del self.commands[commandWatcher.commandId]
        self.status = rendernode.RN_IDLE
        try:
            os.remove(commandWatcher.processObj.pidfile)
        except OSError, e:
            from errno import ENOENT
            err, msg = e.args
            LOGGER.exception(msg)
            if err != ENOENT:
                raise

    def killCommandWatchers(self):
        for commandWatcher in self.commandWatchers.values():
            LOGGER.warning("Aborting command %d", commandWatcher.commandId)
            commandWatcher.processObj.kill()
            commandWatcher.finished = True

    def fetchSysInfos(self):
        infos = {}
        caracs = {"os": platform.system().lower()}
        # get cpu info
        if os.path.isfile('/proc/cpuinfo'):
            try:
                f = open('/proc/cpuinfo', 'r')
                for line in f.readlines():
                    if 'model name' in line:
                        caracs["cpuname"] = line.split(':')[1].strip()
                    elif 'MHz' in line:
                        speedStr = line.split(':')[1].strip()
                        infos["speed"] = "%.1f" % (float(speedStr) / 1000)
                        break
                f.close()
            except:
                LOGGER.warning("Could not get cpu infos.")
        # get distrib infos
        if os.path.isfile('/etc/mik-release'):
            try:
                f = open('/etc/mik-release', 'r')
                for line in f.readlines():
                    if 'MIK-VERSION' in line or 'MIK-RELEASE' in line:
                        caracs["mikdistrib"] = line.split()[1]
                    elif 'openSUSE' in line:
                        if '=' in line:
                            caracs["distribname"] = line.split('=')[1].strip()
                        else:
                            caracs["distribname"] = line
                        break
                f.close()
            except:
                LOGGER.warning("Could not get mik-release infos.")
        # get opengl version
        p = subprocess.Popen("glxinfo",
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        output, errors = p.communicate()
        outputList = output.split("\n")
        for line in outputList:
            if "OpenGL version string" in line:
                oglpattern = re.compile("(\d.\d.\d)")
                res = oglpattern.search(line)
                caracs["openglversion"] = res.group()
                break
        else:
            LOGGER.warning("Could not get opengl version.")
        # get total memory
        memTotal = 1024
        if os.path.isfile('/proc/meminfo'):
            try:
                # get total memory
                f = open('/proc/meminfo', 'r')
                for line in f.readlines():
                    if line.split()[0] == 'MemTotal:':
                        memTotal = line.split()[1]
                        break
                f.close()
            except:
                LOGGER.warning("Could not get total memory info.")
        infos['ram'] = int(memTotal) / 1024
        # additional infos
        infos['cores'] = multiprocessing.cpu_count()
        infos['caracteristics'] = caracs
        infos['name'] = self.computerName
        infos['port'] = self.port
        infos['status'] = self.status
        infos['pools'] = []
        return infos

    def registerWorker(self):
        data = json.dumps(self.fetchSysInfos())
        while True:
            try:
                LOGGER.info("Trying to register worker with dispatcher...")
                r = requests.post(self.url + "/rendernodes/%s/"
                                  % self.computerName, data=data, stream=False)
                if r.status_code == requests.codes.ok:
                    LOGGER.info('Register done!')
                    break
                elif r.status_code == 304:
                    LOGGER.info('Worker already registered.')
                    break
                else:
                    LOGGER.warning('Failed, retrying in 10s.')
                    time.sleep(10)
            except requests.ConnectionError:
                LOGGER.warning('Failed, retrying in 10s.')
                time.sleep(10)

    def mainLoop(self):
        now = time.time()

        # check if the killfile is present
        if os.path.isfile(settings.KILLFILE):
            if self.status != rendernode.RN_PAUSED:
                with open(settings.KILLFILE, 'r') as f:
                    data = f.read()
                if len(data) != 0:
                    data = int(data)
                LOGGER.warning("Killfile detected, pausing worker...")
                # kill cmd watchers, if the flag in the killfile is set to -1
                killproc = False
                if data == -1:
                    LOGGER.warning("Flag -1 detected in killfile, killing render...")
                    killproc = True
                    self.killCommandWatchers()
                if data == -2:
                    LOGGER.warning("Flag -2 detected in killfile, scheduling restart...")
                    self.toberestarted = True
                if data == -3:
                    LOGGER.warning("Flag -3 detected in killfile, killing render and scheduling restart")
                    killproc = True
                    self.toberestarted = True
                    self.killCommandWatchers()
                self.pauseWorker(True, killproc)
        else:
            self.toberestarted = False
            # if no killfile present and worker is paused, unpause it
            if self.status == rendernode.RN_PAUSED:
                self.pauseWorker(False, False)

        # if the worker is marked to be restarted, create restartfile
        if self.toberestarted:
            LOGGER.warning("Restarting...")
            rf = open("/tmp/render/restartfile", 'w')
            rf.close()

        # Waits for any child process, non-blocking (this is necessary to clean up finished process properly)
        #
        try:
            pid, stat = os.waitpid(-1, os.WNOHANG)
            if pid:
                LOGGER.warning("Cleaned process %s" % str(pid))
        except OSError:
            pass

        # Send updates for every modified command watcher.
        #
        for commandWatcher in self.modifiedCommandWatchers:
            self.updateCommandWatcher(commandWatcher)

        # Attempt to remove finished command watchers
        #
        for commandWatcher in self.finishedCommandWatchers:
            LOGGER.info("Removing command watcher %d (status=%r, finished=%r, modified=%r)", commandWatcher.command.id, commandWatcher.command.status, commandWatcher.finished, commandWatcher.modified)
            self.removeCommandWatcher(commandWatcher)

        # Kill watchers that timeout and remove dead command watchers
        # that are not flagged as modified.
        #
        for commandWatcher in self.commandWatchers.values():
            # add the test on running state because a non running command can not timeout (Olivier Derpierre 17/11/10)
            if commandWatcher.timeOut and commandWatcher.command.status == command.CMD_RUNNING:
                responding = (now - commandWatcher.startTime) <= commandWatcher.timeOut
                if not responding:
                    # time out has been reached
                    LOGGER.warning("Timeout on command %d", commandWatcher.commandId)
                    commandWatcher.processObj.kill()
                    commandWatcher.finished = True
                    self.updateCompletionAndStatus(commandWatcher.commandId, None, command.CMD_CANCELED, None)

        # time resync
        now = time.time()
        if (now - self.lastSysInfosMessageTime) > self.sysInfosMessagePeriod:
            self.sendSysInfosMessage()
            self.lastSysInfosMessageTime = now

        # let's be CPU friendly
        time.sleep(0.05)

    def stop(self):
        self.killCommandWatchers()
        try:
            pid, stat = os.waitpid(-1, os.WNOHANG)
            if pid:
                LOGGER.warning("Cleaned process %s" % str(pid))
        except OSError:
            pass

    def updateCompletionAndStatus(self, commandId, completion, status, message):
        try:
            commandWatcher = self.commandWatchers[commandId]
        except KeyError:
            LOGGER.warning("Attempt to update completion and status of unregistered  command %d" % commandId)
        else:
            commandWatcher.modified = True
            if commandWatcher.command.status == command.CMD_CANCELED:
                return
            if completion is not None:
                commandWatcher.command.completion = completion
            if message is not None:
                commandWatcher.command.message = message
            if status is not None:
                commandWatcher.command.status = status
                if command.isFinalStatus(status):
                    commandWatcher.finished = True

    # FIXME to be renamed setPauseStatus
    def pauseWorker(self, paused, killproc):
        dct = {}
        dct['paused'] = paused
        dct['killproc'] = killproc
        data = json.dumps(dct)
        LOGGER.info("Trying to set the paused mode...")
        r = requests.put(self.url + "/rendernodes/%s/paused/"
                         % self.computerName, data=data, stream=False)
        if r.status_code == requests.codes.ok:
            if paused:
                self.status = rendernode.RN_PAUSED
                LOGGER.info("Worker is paused.")
            else:
                self.status = rendernode.RN_IDLE
                LOGGER.info("Worker is awake.")
        else:
            LOGGER.warning("Could not set the paused mode :")
            LOGGER.warning("(%d) %s" % (r.status_code, r.text))

    def sendSysInfosMessage(self):
        if self.status is not rendernode.RN_PAUSED:
            infos = {}
            infos['status'] = self.status
            try:
                r = requests.put(self.url + "/rendernodes/%s/sysinfos"
                                 % self.computerName,
                                 data=json.dumps(infos),
                                 stream=False)
                if r.status_code != requests.codes.ok:
                    LOGGER.warning("Could not send sysinfos :")
                    LOGGER.warning("(%d) %s" % (r.status_code, r.text))
            except requests.ConnectionError, e:
                LOGGER.exception("An exception occured : %s" % e)

    def updateSysInfos(self, ticket):
        infos = self.fetchSysInfos()
        r = requests.put(self.url + "/rendernodes/%s/sysinfos"
                         % self.computerName,
                         data=json.dumps(infos),
                         stream=False)
        if r.status_code != requests.codes.ok:
            LOGGER.warning("Could not send sysinfos :")
            LOGGER.warning("(%d) %s" % (r.status_code, r.text))

    def addCommandApply(self, ticket, commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment):
        if self.status != rendernode.RN_PAUSED:
            newCommand = Command(commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment=environment)
            self.addCommandWatcher(newCommand)
            # FIXME is this really necessary ??
            self.commands[commandId] = newCommand
            LOGGER.info("Added command %d {runner: %s, arguments: %s}", commandId, runner, repr(arguments))

    def stopCommandApply(self, ticket, commandId):
        commandWatcher = self.commandWatchers[commandId]
        commandWatcher.processObj.kill()
        self.updateCompletionAndStatus(commandId, 0, command.CMD_CANCELED, "killed")
        LOGGER.info("Stopped command %r", commandId)

    def updateCommandApply(self, ticket, commandId, status, completion, message):
        self.updateCompletionAndStatus(commandId, completion, status, message)
        LOGGER.info("Updated command id=%r status=%r completion=%r message=%r" % (commandId, status, completion, message))

    def updateCommandValidationApply(self, ticket, commandId, validatorMessage, errorInfos):
        try:
            commandWatcher = self.commandWatchers[commandId]
        except KeyError:
            ticket.status = ticket.ERROR
            ticket.message = "No such command watcher."
        else:
            commandWatcher.command.validatorMessage = validatorMessage
            commandWatcher.command.errorInfos = errorInfos
            LOGGER.info("Updated validation info id=%r validatorMessage=%r errorInfos=%r" % (commandId, validatorMessage, errorInfos))

    def addCommandWatcher(self, command):
        newCommandWatcher = self.CommandWatcher()
        newCommandWatcher.commandId = command.id

        logdir = os.path.join(settings.LOGDIR, command.relativePathToLogDir)
        outputFile = os.path.join(logdir, '%d.log' % (command.id))
        scriptFile = commandwatcher.__file__
        workerPort = self.framework.webService.port
        pidFile = os.path.join(self.PID_DIR, "cw%s.pid" % newCommandWatcher.commandId)

        # create the logdir if it does not exist
        if not os.path.isdir(logdir):
            try:
                os.makedirs(logdir, 0777)
            except OSError, e:
                import errno
                err = e.args[0]
                if err != errno.EEXIST:
                    raise

        logFile = open(outputFile, "w")

        piddir = os.path.dirname(pidFile)
        if not os.path.isdir(piddir):
            try:
                os.makedirs(piddir, 0777)
            except OSError, e:
                err = e.args[0]
                if err != errno.EEXIST:
                    raise

        args = [sys.executable,
                "-u",
                scriptFile,
                outputFile,
                str(workerPort),
                str(command.id),
                command.runner,
                command.validationExpression]
        args.extend(('%s=%s' % (str(name), str(value)) for (name, value) in command.arguments.items()))

        watcherProcess = spawnCommandWatcher(pidFile, logFile, args, command.environment)
        newCommandWatcher.processObj = watcherProcess
        newCommandWatcher.startTime = time.time()
        newCommandWatcher.timeOut = None
        newCommandWatcher.command = command
        newCommandWatcher.processId = watcherProcess.pid

        self.commandWatchers[command.id] = newCommandWatcher
        LOGGER.info("Started command %d", command.id)
