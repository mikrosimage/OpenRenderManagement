import logging
import socket
import os
import sys
import time
import platform

from octopus.core.framework.mainloopapplication import MainLoopApplication
from octopus.core.communication.requestmanager import RequestManager
from octopus.core.enums import command as COMMAND
from octopus.core.enums import rendernode
from octopus.core.tools import json, httplib
from octopus.worker import settings
from octopus.worker.model.command import Command
from octopus.worker.process import CommandWatcherProcess, spawnCommandWatcher

#DEBUG = True
LOGGER = logging.getLogger("worker")
COMPUTER_NAME_TEMPLATE = "%s:%d"

class Worker(MainLoopApplication):

    class CommandWatcher(object):
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
        LOGGER.info("Starting worker on %s:%d.",
                    settings.ADDRESS,
                    settings.PORT)
        self.ip = socket.gethostbyname(settings.ADDRESS)
        super(Worker, self).__init__(self)
        self.framework = framework
        self.data = None
        self.requestManager = RequestManager(settings.DISPATCHER_ADDRESS,
                                             settings.DISPATCHER_PORT)
        self.commandWatchers = {}
        self.commands = {}
        self.port = settings.PORT
        self.computerName = COMPUTER_NAME_TEMPLATE % (settings.ADDRESS,
                                                      settings.PORT)
        self.lastSysInfosMessageTime = 0
        self.sysInfosMessagePeriod = 6
        self.httpconn = httplib.HTTPConnection(settings.DISPATCHER_ADDRESS, settings.DISPATCHER_PORT)
        self.PID_DIR = os.path.dirname(settings.PIDFILE)
        if not os.path.exists(self.PID_DIR):
            LOGGER.warning("Worker pid directory does not exist.")
            try:
                os.makedirs(self.PID_DIR, 0777)
                LOGGER.info("Worker pid directory created.")
            except OSError:
                LOGGER.error("Failed to create pid directory.")
                sys.exit(1)
        elif not os.path.isdir(self.PID_DIR):
            LOGGER.error("Pid directory is not a directory.")
            sys.exit(1)
        elif not os.access(self.PID_DIR, os.R_OK | os.W_OK):
            LOGGER.error("Missing read or write access on %s", self.PID_DIR)
            sys.exit(1)
        self.status = rendernode.RN_BOOTING
        self.updateSys = False
        self.isPaused = False
        self.speed = 1.0
        self.cpuName = ""
        self.distrib = ""
        self.mikdistrib = ""
        

    def prepare(self):
        for name in (name for name in dir(settings) if name.isupper()):
            LOGGER.info("settings.%s = %r", name, getattr(settings, name))
#        self.reloadCommandWatchers()
        self.registerWorker()


#   def reloadCommandWatchers(self):
#       import glob
#       import re
#       pathname = os.path.join(self.PID_DIR, "cw*.pid")
#       for pidfile in glob.glob(pathname):
#           basepidfile = os.path.basename(pidfile)
#           print "pidfile", pidfile
#           match = re.match(r"cw(\d+).pid", basepidfile)
#           if match is None:
#               continue
#           commandId = int(match.groups()[0])
#           commandWatcherPID = int(file(pidfile, "r").read())
#           print "found PID for command %d: %d" % (commandId, commandWatcherPID)
#           command = Command(commandId, None, {}, "")
#           cw = self.CommandWatcher()
#           cw.commandId = commandId
#           cw.processObj = CommandWatcherProcess(pidfile, commandWatcherPID)
#           cw.startTime = os.path.getmtime(pidfile)
#           cw.timeOut = None
#           cw.command = command
#           cw.processId = commandWatcherPID
#           self.commandWatchers[cw.commandId] = cw
#           self.commands[commandId] = command

    
    def getNbCores(self):
        nb = 1
        if os.path.exists('/proc/stat'):
            try:
                # get nb cores
                f = open('/proc/stat', 'r')
                for line in f.readlines():
                    if line.startswith("cpu"):
                        nb = nb+1
                f.close()
                nb=nb-2
            except:
                pass
        return nb

    
    def getTotalMemory(self):
        memTotal = 1024
        if os.path.exists('/proc/meminfo'):
            try:
                # get total memory
                f = open('/proc/meminfo', 'r')
                for line in f.readlines():
                    if line.split()[0] == 'MemTotal:':
                        memTotal = line.split()[1]
                        f.close()
                        break
            except:
                pass
        return int(memTotal) / 1024
    
    
    def getCpuInfo(self):
        if os.path.exists('/proc/cpuinfo'):
            try:
                # get cpu speed
                f = open('/proc/cpuinfo', 'r')
                for line in f.readlines():
                    if 'model name' in line:
                        self.cpuName = line.split(':')[1]
                    elif 'MHz' in line:
                        speedStr = line.split(':')[1].strip()
                        self.speed = "%.1f" % (float(speedStr)/1000)
                        break
                f.close()
            except:
                pass
            
    def getDistribName(self):
        if os.path.exists('/etc/mik-release'):
            try:
                f = open('/etc/mik-release', 'r')
                for line in f.readlines():
                    if 'openSUSE' in line:
                        self.distrib = line
                    elif 'MIK-VERSION' in line:
                        self.mikdistrib = line.split('=')[1].strip()
                        break
                f.close()
            except:
                pass
    
    
    def updateSysInfos(self, ticket):
        self.updateSys = True


    def fetchSysInfos(self):
        infos = {}
        if self.updateSys:
            self.getCpuInfo()
            self.getDistribName()
            infos['cores'] = self.getNbCores()
            infos['ram'] = self.getTotalMemory()
            self.updateSys = False
        infos['name'] = self.computerName
        #infos['ip'] = self.ip
        infos['port'] = self.port
        infos['status'] = self.status
        # system info values:
        infos['caracteristics'] = {"os":platform.system().lower(), "softs":[], "cpuname":self.cpuName, "distribname":self.distrib, "mikdistrib":self.mikdistrib}
        infos['pools'] = []
        infos['speed'] = float(self.speed)
        return infos
    

    def registerWorker(self):
        '''Register the worker in the dispatcher.'''
        self.updateSys = True
        infos = self.fetchSysInfos()
        dct = json.dumps(infos)
        headers = {}
        headers['content-length'] = len(dct)

        while True:
            try:
                LOGGER.info("Boot process... registering worker")
                url = "/rendernodes/%s/" % self.computerName
                resp = self.requestManager.post(url, dct, headers)
            except RequestManager.RequestError, e:
                if e.status != 409:
                    msg = "Dispatcher (%s:%s) is not reachable. We'll retry..."
                    msg %= (settings.DISPATCHER_ADDRESS, settings.DISPATCHER_PORT)
                    LOGGER.exception(msg)
                else:
                    LOGGER.info("Boot process... worker already registered")
                    break
            else:
                if resp == 'ERROR':
                    LOGGER.warning('Worker registration failed.')
                else:
                    LOGGER.info("Boot process... worker registered")
                    break
            time.sleep(1.0)

        self.sendSysInfosMessage()
    

    def buildUpdateDict(self, command):
        dct = {}
        if command.completion != None:
            dct["completion"] = command.completion
        if command.status != None:
            dct["status"] = command.status
        if command.validatorMessage != None:
            dct["validatorMessage"] = command.validatorMessage
        if command.errorInfos != None:
            dct["errorInfos"] = command.errorInfos
        dct['message'] = command.message
        dct['id'] = command.id
        return dct


    def updateCommandWatcher(self, commandWatcher):
        while True:
            url = "/rendernodes/%s/commands/%d/" % (self.computerName, commandWatcher.commandId)
            body = json.dumps(self.buildUpdateDict(commandWatcher.command))
            headers = {'Content-Length': len(body)}
            try:
                self.httpconn.request('PUT', url, body, headers)
                response = self.httpconn.getresponse()
            except httplib.HTTPException:
                LOGGER.exception('"PUT %s" failed', url)
            except socket.error:
                LOGGER.exception('"PUT %s" failed', url)
            else:
                if response.status == 200:
                    response = response.read()
                    commandWatcher.modified = False
                elif response.status == 404:
                    LOGGER.warning('removing stale command %d', commandWatcher.commandId)
                    response = response.read()
                    self.removeCommandWatcher(commandWatcher)
                else:
                    data = response.read()
                    print "unexpected status %d: %s %s" % (response.status, response.reason, data)
                return
            finally:
                self.httpconn.close()
            LOGGER.warning('Update of command %d failed.', commandWatcher.commandId)


    def pauseWorker(self, paused, killproc):
        while True:
            url = "/rendernodes/%s/paused/" % (self.computerName)
            dct = {}
            dct['paused'] = paused
            dct['killproc'] = killproc
            body = json.dumps(dct)
            headers = {'Content-Length': len(body)}
            try:
                self.httpconn.request('PUT', url, body, headers)
                response = self.httpconn.getresponse()
            except httplib.HTTPException:
                LOGGER.exception('"PUT %s" failed', url)
            except socket.error:
                LOGGER.exception('"PUT %s" failed', url)
            else:
                if response.status == 200:
                    if paused:
                        self.isPaused = True
                        LOGGER.info("Worker has been put in paused mode")
                    else:
                        self.isPaused = False
                        LOGGER.info("Worker awakes from paused mode")
                return
            finally:
                self.httpconn.close()


    def mainLoop(self):
        try:
            now = time.time()
            # check if the killfile is present
            #
            if os.path.isfile(settings.KILLFILE):
                if not self.isPaused:
                    with open(settings.KILLFILE, 'r') as f:
                        data = f.read()
                    if len(data) != 0:
                        data = int(data)
                    LOGGER.warning("Killfile detected, pausing worker")
                    # kill cmd watchers, if the flag in the killfile is set to -1
                    killproc = False
                    if data == -1:
                        LOGGER.warning("Flag -1 detected in killfile")
                        killproc = True
                        for commandWatcher in self.commandWatchers.values():
                            LOGGER.warning("Aborting command %d", commandWatcher.commandId)
                            commandWatcher.processObj.kill()
                            commandWatcher.finished = True
                            self.updateCompletionAndStatus(commandWatcher.commandId, None, COMMAND.CMD_READY, None)
                    self.pauseWorker(True, killproc)
            else:
                # if no killfile present and worker is paused, unpause it
                if self.isPaused:
                    self.pauseWorker(False, False)
    
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
                # add the test on running state because a non running command can not timeout (bud 17/11/10)
                if commandWatcher.timeOut and commandWatcher.command.status == COMMAND.CMD_RUNNING:
                    responding = (now - commandWatcher.startTime) <= commandWatcher.timeOut
                    if not responding:
                        # time out has been reached
                        LOGGER.warning("Timeout on command %d", commandWatcher.commandId)
                        commandWatcher.processObj.kill()
                        commandWatcher.finished = True
                        self.updateCompletionAndStatus(commandWatcher.commandId, None, COMMAND.CMD_CANCELED, None)
            
            # This is necessary in the case of blocked 'working' state
            #if not self.commandWatchers and len(self.commands.values()) != 0:
            #    # FIXME patch for a weird bug : sometimes, there is a phantom command in the list, even if there is no more command processes
            #    LOGGER.warning("No more cmd watcher but still command in list, reset the RN on the dispatcher")
            #    self.commands.clear()
            #    datas = {}
            #    datas['nomorecmd'] = 1
            #    dct = json.dumps(datas)
            #    headers = {}
            #    headers['content-length'] = len(dct)
            #    self.requestManager.put("/rendernodes/%s/reset" % self.computerName, dct, headers)
            
            # time resync
            now = time.time()
            if (now - self.lastSysInfosMessageTime) > self.sysInfosMessagePeriod:
                self.sendSysInfosMessage()
                self.lastSysInfosMessageTime = now
    
            self.httpconn.close()
            
            # let's be CPU friendly
            time.sleep(0.05)
        except:
            LOGGER.error("A problem occured : " + repr(sys.exc_info()))


    def sendSysInfosMessage(self):
        infos = self.fetchSysInfos()
        #infos['ip'] = self.ip
        #infos['port'] = self.port
        #infos['status'] = self.status
        #infos['os'] = platform.system().lower()
        dct = json.dumps(infos)
        headers = {}
        headers['content-length'] = len(dct)

        try:
            self.requestManager.put("/rendernodes/%s/sysinfos" % self.computerName, dct, headers)
        except RequestManager.RequestError, err:
            if err.status == 404:
                # the dispatcher doesn't know the worker
                # it may have been launched before the dispatcher itself
                # and not be mentioned in the tree.description file
                self.registerWorker()
            else:
                raise
        except httplib.BadStatusLine:
            LOGGER.exception('Sending sys infos has failed with a BadStatusLine error')


    def connect(self):
        return httplib.HTTPConnection(settings.DISPATCHER_ADDRESS, settings.DISPATCHER_PORT)


    def removeCommandWatcher(self, commandWatcher):
        # remove this request !!! the worker should not interfere with the dispatcher model like this
        # NOT THREAD SAFE !!!!
        # 1. send a DELETE request to the dispatcher
        # 2. on success, remove the command watcher from the list and remove the pid file
        #LOGGER.info("before connect")
        #conn = self.connect()
        #path = '/rendernodes/%s/commands/%d' % (self.computerName, commandWatcher.command.id)
        #LOGGER.info("path : " + path)
        #r = Request('DELETE', path, {'Accept': 'application/json'})
        #r.commandWatcher = commandWatcher
        #LOGGER.info("calling...")
        #r.call(conn, self._onRemoveCommandWatcherResponse, self._onRemoveCommandWatcherError)


    #def _onRemoveCommandWatcherResponse(self, request, response):
    #    commandWatcher = request.commandWatcher
        print "\nREMOVING COMMAND WATCHER %d\n" % commandWatcher.command.id
    #    if response.status == 200:
        LOGGER.info('Removing command watcher for command %d', commandWatcher.commandId)
    #    elif response.status == 403 or response.status == 404:
    #        LOGGER.warning('removing command watcher for obsolete command %d', commandWatcher.commandId)
    #    else:
    #        LOGGER.error('command watcher removal request returned unexpected status %d', response.status)
    #         return
        del self.commandWatchers[commandWatcher.commandId]
        del self.commands[commandWatcher.commandId]
        try:
            os.remove(commandWatcher.processObj.pidfile)
            self.status = rendernode.RN_IDLE
        except OSError, e:
            from errno import ENOENT
            err, msg = e.args
            LOGGER.exception(msg)
            if err != ENOENT:
                raise


    #def _onRemoveCommandWatcherError(self, request, error):
    #    LOGGER.debug('removeCommandWatcher failed in exception')
    #    if error == None:
    #        LOGGER.debug('Unexpected error:' + sys.exc_info()[0])
    #    if settings.DEBUG:
    #        raise error


    def updateCompletionAndStatus(self, commandId, completion, status, message):
        try:
            commandWatcher = self.commandWatchers[commandId]
        except KeyError:
            LOGGER.warning("attempt to update completion and status of unregistered  command %d", commandId)
        else:
            commandWatcher.modified = True
            if commandWatcher.command.status == COMMAND.CMD_CANCELED:
                return
            if completion is not None:
                commandWatcher.command.completion = completion
            if message is not None:
                commandWatcher.command.message = message
            if status is not None:
                commandWatcher.command.status = status
                if COMMAND.isFinalStatus(status):
                    commandWatcher.finished = True


    def addCommandApply(self, ticket, commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment):
        newCommand = Command(commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment=environment)
        self.commands[commandId] = newCommand
        self.addCommandWatcher(newCommand)
        LOGGER.info("Added command %d {runner: %s, arguments: %s}", commandId, runner, repr(arguments))


    ##
    #
    # @param id the integer value identifying the command
    # @todo add a ticket parameter
    # @todo find a clean way to stop the processes so that they \
    #       can call their after-execution scripts
    #
    def stopCommandApply(self, ticket, commandId):
        commandWatcher = self.commandWatchers[commandId]
        commandWatcher.processObj.kill()
        self.updateCompletionAndStatus(commandId, 0, COMMAND.CMD_CANCELED, "killed")
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

        commandId = command.id

        from octopus.commandwatcher import commandwatcher
        logdir = os.path.join(settings.LOGDIR, command.relativePathToLogDir)
        outputFile = os.path.join(logdir, '%d.log' % (commandId))
        commandWatcherLogFile = outputFile

        scriptFile = commandwatcher.__file__

        workerPort = self.framework.webService.port
        pythonExecutable = sys.executable

        pidFile = os.path.join(self.PID_DIR, "cw%s.pid" % newCommandWatcher.commandId)
        
        # create the logdir if it does not exist
        if not os.path.exists(logdir):
            try:
                os.makedirs(logdir, 0777)
            except OSError, e:
                import errno
                err = e.args[0]
                if err != errno.EEXIST:
                    raise
        logFile = file(outputFile, "w")
        
        d = os.path.dirname(pidFile)
        if not os.path.exists(d):
            try:
                os.makedirs(d, 0777)
            except OSError, e:
                err = e.args[0]
                if err != errno.EEXIST:
                    raise

        args = [
            pythonExecutable,
            "-u",
            scriptFile,
            commandWatcherLogFile,
            str(workerPort),
            str(command.id),
            command.runner,
            command.validationExpression,
        ]
        args.extend(('%s=%s' % (str(name), str(value)) for (name, value) in command.arguments.items()))

        watcherProcess = spawnCommandWatcher(pidFile, logFile, args, command.environment)
        newCommandWatcher.processObj = watcherProcess
        newCommandWatcher.startTime = time.time()
        newCommandWatcher.timeOut = None
        newCommandWatcher.command = command
        newCommandWatcher.processId = watcherProcess.pid

        self.commandWatchers[command.id] = newCommandWatcher

        LOGGER.info("Started command %d", command.id)
