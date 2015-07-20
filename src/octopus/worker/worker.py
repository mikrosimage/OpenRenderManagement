import logging
import socket
import os
import signal
import sys
import time
import datetime
import platform
try:
    import simplejson as json
except ImportError:
    import json
import httplib

import subprocess
from subprocess import PIPE

try:
    import psutil
except ImportError:
    print("WARNING: impossible to import psutil")

from octopus.core.framework.mainloopapplication import MainLoopApplication
from octopus.core.communication.requestmanager import RequestManager
from octopus.core.enums import command as COMMAND
from octopus.core.enums import rendernode
from octopus.core.enums.rendernode import *

from octopus.worker import settings
from octopus.worker import config

from octopus.worker.model.command import Command
from octopus.worker.process import spawnCommandWatcher
from octopus.worker.process import spawnRezManagedCommandWatcher

LOGGER = logging.getLogger("worker")
COMPUTER_NAME_TEMPLATE = "%s:%d"

KILOBYTES = 0
MEGABYTES = 1
GIGABYTES = 2


class WorkerInternalException(Exception):
    """
    Custom exception to handle internal failure during worker's execution.
    """
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class Worker(MainLoopApplication):
    """
    Worker application based on the main framework.
    It is an independant remote daemon communicating with the dispatcher via WS and in charge of handling job execution with the help
    of CommandWatcher subprocess.
    """

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

        def __repr__(self):
            return str(
                "CommandWatcher(id=%r, processId=%r, startTime=%r, processObj=%r, timeOut=%r, commandId=%r, command=%r, modified=%r, finished=%r)" %
                (self.id, self.processId, self.startTime, self.processObj, self.timeOut, self.commandId, self.command, self.modified, self.finished)
            )

        def __str__(self):
            startTime = datetime.datetime.fromtimestamp(int(self.startTime)).strftime("%Y-%m-%d_%H:%M:%S")
            returncode = self.processObj.process.returncode if self.processObj.process is not None else "Invalid process"
            return str("CommandWatcher: pid=%r, commandId=%r, returncode=%r, startTime=%s" % (self.processId, self.commandId, returncode, startTime))

    @property
    def modifiedCommandWatchers(self):
        """
        Property of the Worker class. An iterable list of the modified command watchers
        :rtype: list of CommandWatcher
        """
        return (watcher for watcher in self.commandWatchers.values() if watcher.modified)

    @property
    def finishedCommandWatchers(self):
        """
        Property of the Worker class. An iterable list of the finished command watchers
        :rtype: list of CommandWatcher
        """
        return (watcher for watcher in self.commandWatchers.values() if watcher.finished and not watcher.modified)

    def __init__(self, framework):
        super(Worker, self).__init__(self)
        LOGGER.info("---")
        LOGGER.info("Initializing worker")
        LOGGER.info("---")

        self.framework = framework
        self.data = None
        self.requestManager = RequestManager(settings.DISPATCHER_ADDRESS,
                                             settings.DISPATCHER_PORT)
        self.commandWatchers = {}
        self.commands = {}
        self.port = settings.PORT
        self.computerName = COMPUTER_NAME_TEMPLATE % (settings.ADDRESS,
                                                      settings.PORT)

        self.createDate = time.time()
        self.lastSysInfosMessageTime = 0
        self.lastFullSysInfoUpdate = 0
        self.registerDate = 0

        self.httpconn = httplib.HTTPConnection(settings.DISPATCHER_ADDRESS, settings.DISPATCHER_PORT)
        self.PID_DIR = os.path.dirname(settings.PIDFILE)
        if not os.path.isdir(self.PID_DIR):
            LOGGER.warning("Worker pid directory %s does not exist, creating..." % self.PID_DIR)
            try:
                os.makedirs(self.PID_DIR, 0777)
                LOGGER.info("Worker pid directory created.")
            except OSError:
                LOGGER.error("Failed to create pid directory.")
                sys.exit(1)
        elif not os.access(self.PID_DIR, os.R_OK | os.W_OK):
            LOGGER.error("Missing read or write access on %s", self.PID_DIR)
            sys.exit(1)
        self.status = rendernode.RN_BOOTING
        self.updateSys = False
        self.isPaused = False
        self.toberestarted = False
        self.speed = 1.0
        self.cpuName = "undefined"
        self.distrib = "undefined"
        self.mikdistrib = "undefined"
        self.openglversion = ""

    def prepare(self):
        LOGGER.info("Before registering: prepare worker.")
        for name in (name for name in dir(settings) if name.isupper()):
            LOGGER.info("settings.%s = %r", name, getattr(settings, name))
        self.registerWorker()

    def getNbCores(self):
        import multiprocessing
        return multiprocessing.cpu_count()

    def getTotalMemory(self):
        memTotal = 1024
        if os.path.isfile('/proc/meminfo'):
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

    def getFreeMem(self, pUnit=MEGABYTES):
        """
        | Starts a shell process to retrieve amount of free memory on the worker's system.
        | The amount of memory is transmitted in MEGABYTES, but can be specified to another unit
        | To estimate this, we retrieve specific values in /proc/meminfo:
        | Result = MemFree + Buffers + Cached

        :param pUnit: An integer representing the unit to which the value is converted (DEFAULT is MEGABYTES).
        :return: An integer representing the amount of FREE memory on the system
        :raise: OSError if subprocess fails. Returns "-1" if no correct value can be retrieved.
        """

        try:
            freeMemStr = subprocess.Popen(["awk",
                                           "/MemFree|Buffers|^Cached/ {free+=$2} END {print  free}",
                                           "/proc/meminfo"], stdout=PIPE).communicate()[0]
        except OSError, e:
            LOGGER.warning("Error when retrievieng free memory: %r", e)

        if freeMemStr == '':
            return -1

        freeMem = int(freeMemStr)

        if pUnit is MEGABYTES:
            freeMem = int(freeMem / 1024)
        elif pUnit is GIGABYTES:
            freeMem = int(freeMem / (1024 * 1024))

        return freeMem

    def getSwapUsage(self):
        """
        | Uses psutil module to retrieve usage swap percentage. The value is transmitted as a float in range [0-1]
        :return: A float indicating the amount of swap currently used on the system
        """
        swapUsage = 0.0
        try:
            try:
                swapUsage = psutil.swap_memory().percent
            except psutil.Error:
                LOGGER.warning("An error occured when retrieving swap percentage.")
        except NameError, e:
            LOGGER.debug("Impossible to use psutil module on this host: %r" % e)
        except Exception, e:
            LOGGER.warning("An unexpected error occured: %r" % e)

        return swapUsage

    def getCpuInfo(self):
        if os.path.isfile('/proc/cpuinfo'):
            try:
                # get cpu speed
                f = open('/proc/cpuinfo', 'r')
                for line in f.readlines():
                    if 'model name' in line:
                        self.cpuName = line.split(':')[1].strip()
                        speedStr = line.split('@')[1].strip()
                        self.speed = speedStr.split('GHz')[0].strip()
                        break
                f.close()
            except:
                pass

    def getDistribName(self):
        '''
        | Retrieves indicators of mik release and os release.
        | Handles several cases reflecting the history of Mikros distrib info declaration
        | This method values to:
        |   - self.mikdistrib
        |   - self.distrib
        '''
        if os.path.isfile('/etc/mik-release'):
            try:
                f = open('/etc/mik-release', 'r')
                for line in f.readlines():
                    if 'MIK-VERSION' in line or 'MIK-RELEASE' in line:
                        self.mikdistrib = line.split()[1]
                    elif 'openSUSE' in line:
                        if '=' in line:
                            self.distrib = line.split('=')[1].strip()
                        else:
                            self.distrib = line
                        break
                f.close()
            except:
                pass
        elif os.path.isfile('/etc/mikrelease'):
            try:
                f = open('/etc/mikrelease', 'r')
                for line in f.readlines():
                    if 'mikrelease' in line:
                        self.mikdistrib = line.split()[1]
                    elif 'Fedora' in line:
                        if '=' in line:
                            self.distrib = line.split('=')[1].strip()
                        else:
                            self.distrib = line
                        break
                f.close()
            except:
                pass
        else:
            self.mikdistrib = "unknown"
            self.distrib = "unknown"

    def getOpenglVersion(self):
        import subprocess
        import re
        p = subprocess.Popen("glxinfo", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = p.communicate()
        outputList = output.split("\n")
        for line in outputList:
            if "OpenGL version string" in line:
                LOGGER.info("found : %s" % line)
                oglpattern = re.compile("(\d.\d.\d)")
                res = oglpattern.search(line)
                self.openglversion = res.group()
                break

    def updateSysInfos(self, ticket):
        self.updateSys = True

    def fetchSysInfos(self):
        infos = {}
        if self.updateSys:
            self.getCpuInfo()
            self.getDistribName()
            self.getOpenglVersion()
            infos['cores'] = self.getNbCores()
            infos['ram'] = self.getTotalMemory()
            infos['systemFreeRam'] = self.getFreeMem()
            infos['systemSwapPercentage'] = self.getSwapUsage()
            infos["puliversion"] = settings.VERSION
            infos["createDate"] = self.createDate

            self.updateSys = False
            # system info values:
            infos['caracteristics'] = {"os": platform.system().lower(),
                                       "softs": [],
                                       "cpuname": self.cpuName,
                                       "distribname": self.distrib,
                                       "mikdistrib": self.mikdistrib,
                                       "openglversion": self.openglversion}
        infos['name'] = self.computerName
        infos['port'] = self.port
        infos['status'] = self.status
        infos['pools'] = []
        infos['speed'] = float(self.speed)
        return infos

    # def setPerformanceIndex(self, ticket, performance):
    #     """
    #     NOTE: never called ???

    #     Send sys infos to the dispatcher
    #     req: PUT /rendernodes/<currentRN>/sysinfos
    #     """

    #     LOGGER.warning("set perf idx")
    #     dct = json.dumps({'performance': performance})
    #     headers = {}
    #     headers['content-length'] = len(dct)

    #     LOGGER.warning(dct)

    #     try:
    #         self.requestManager.put("/rendernodes/%s/sysinfos" % self.computerName, dct, headers)
    #     except RequestManager.RequestError, err:
    #         if err.status == 404:
    #             # the dispatcher doesn't know the worker
    #             # it may have been launched before the dispatcher itself
    #             # and not be mentioned in the tree.description file
    #             self.registerWorker()
    #         else:
    #             raise
    #     except httplib.BadStatusLine:
    #         LOGGER.exception('Sending sys infos has failed with a BadStatusLine error')

    def registerWorker(self):
        '''Register the worker in the dispatcher.'''
        self.updateSys = True
        self.registerDate = time.time()

        infos = self.fetchSysInfos()

        # Add specific info when registering (initially it was the same info at register and periodic utpdate
        infos["createDate"] = self.createDate
        infos["puliversion"] = settings.VERSION

        dct = json.dumps(infos)
        # FIXME if a command is currently running on this worker, notify the dispatcher
        # if len(self.commands.items()):
        #     dct['commands'] = self.commands.items()
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
            # try to register to dispatcher every 10 seconds
            time.sleep(config.WORKER_REGISTER_DELAY_AFTER_FAILURE)

        # once the worker is registered, ensure the RN status is correct according to the killfile presence
        if os.path.isfile(settings.KILLFILE):
            self.pauseWorker(True, False)
        else:
            self.pauseWorker(False, False)

        self.sendSysInfosMessage()

    def buildUpdateDict(self, command):
        dct = {}
        if command.completion is not None:
            dct["completion"] = command.completion
        if command.status is not None:
            dct["status"] = command.status
        if command.validatorMessage is not None:
            dct["validatorMessage"] = command.validatorMessage
        if command.errorInfos is not None:
            dct["errorInfos"] = command.errorInfos
        if hasattr(command, 'stats'):
            dct["stats"] = command.stats

        dct['message'] = command.message
        dct['id'] = command.id
        return dct

    def updateCommandWatcher(self, commandWatcher):
        """
        | Send info to the dispatcher about currently running command watcher.
        | Called from the mainloop every time a command has been tagged "modified"
        | req: PUT /rendernodes/<currentRN>/commands/<commandId>/

        :param commandWatcher: the commandWatcher object we will send an update about
        """

        maxRetry = max(1, config.WORKER_REQUEST_MAX_RETRY_COUNT)
        delayRetry = config.WORKER_REQUEST_DELAY_AFTER_REQUEST_FAILURE
        i = 0

        while i < maxRetry:
            url = "/rendernodes/%s/commands/%d/" % (self.computerName, commandWatcher.commandId)
            body = json.dumps(self.buildUpdateDict(commandWatcher.command))
            headers = {'Content-Length': len(body)}

            try:
                self.httpconn.request('PUT', url, body, headers)
                response = self.httpconn.getresponse()
            except httplib.HTTPException, e:
                LOGGER.error('"PUT %s" failed (error:%r)', url, e)
            except socket.error, e:
                LOGGER.error('"PUT %s" failed (error:%r)', url, e)
            except Exception, e:
                LOGGER.info('"PUT %s" failed (unhandled exception: %r', url, e)
            else:
                if response.status == 200:
                    response = response.read()
                    commandWatcher.modified = False
                    break
                elif response.status == 404:
                    LOGGER.warning('removing stale command %d', commandWatcher.commandId)
                    response = response.read()
                    self.removeCommandWatcher(commandWatcher)
                    break
                else:
                    data = response.read()
                    LOGGER.warning("unexpected status %d: %s %s" % (response.status, response.reason, data))
            finally:
                self.httpconn.close()

            LOGGER.warning('Update of command %d failed (attempt %d of %d)', commandWatcher.commandId, i, maxRetry)
            LOGGER.warning('Next retry will occur in %.2f s' % delayRetry)
            time.sleep(delayRetry)
            i += 1
            delayRetry *= 1.5

        # Request error encountered "maxRetry" times, removing the command watcher to avoid
        # recalling this update in next main loop iter
        # if i == maxRetry:
        #     LOGGER.exception('Update of command %d failed repeatedly, removing watcher.', commandWatcher.commandId )
        #     self.removeCommandWatcher(commandWatcher)

    def pauseWorker(self, paused, killproc):
        """
        | Called from mainloop when checking killfile presence (also checked when registering).
        | Send a request to the dispatcher to update rendernode's state.
        | req: PUT /rendernodes/<currentRN>/paused/

        :param paused: boolean flag indicating the status to set for this worker
        :param killproc: boolean flag indicating if all running processes must be killed or not
        """
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
                        self.status = rendernode.RN_PAUSED
                        self.isPaused = True
                        LOGGER.info("Worker has been put in paused mode")
                    else:
                        self.status = rendernode.RN_IDLE
                        self.isPaused = False
                        LOGGER.info("Worker awakes from paused mode")
                return
            finally:
                self.httpconn.close()

    def killCommandWatchers(self):
        for commandWatcher in self.commandWatchers.values():
            LOGGER.warning("Aborting command %d", commandWatcher.commandId)
            commandWatcher.processObj.kill()
            commandWatcher.finished = True
        self.ensureNoMoreRender()

    @staticmethod
    def getToKeepPIDs():
        result = []
        for pIDFileStr in settings.PIDFILES_TO_KEEP:
            try:
                pIDFile = open(pIDFileStr, 'r')
                if pIDFile :
                    pid = pIDFile.read()
                    if pid :
                        result.append(int(pid.rstrip('\n')))
            except:
                continue
        return result


    def ensureNoMoreRender(self):
        """
        Force kill of all process attached to the current EUID. this ensures that all command processes have been killed.
        :return: a boolean indicating success
        """

        try:
            # Ensure we don't have anymore rendering process
            keepPID = [os.getpid(), os.getppid()].extend(self.getToKeepPIDs())
            effectiveUID = os.geteuid()
            LOGGER.debug("PID to keep: %r" % keepPID)

            # Get pids of current user (usually 'render')
            renderProcessList = subprocess.check_output(["ps", "-u", str(effectiveUID), "-o", "pid", "h"]).split()

            # Filter the list to preserve the current process and parent process
            killPID = [pid for pid in renderProcessList if int(pid) not in keepPID]
            # LOGGER.debug("PID to kill: %r" % killPID)

            # Send SIGKILL to everyone else
            for pid in killPID:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                    LOGGER.debug("SIGKILL sent to %s" % pid)
                except OSError:
                    LOGGER.debug("Impossible to send SIGKILL to %s, the process has vanished." % pid)
                    continue
        except Exception as err:
            LOGGER.error("Error when killing render processes. Some processes of a closed command might still run (%s)" % err)
            return False
        return True

    def getKillfileInfo(self):
        """
        :return: Boolean indicating success
        """
        killfileExists = os.path.isfile(settings.KILLFILE)
        killfileContent = None

        if killfileExists:
            with open(settings.KILLFILE, 'r') as f:
                data = f.read()
            if len(data) != 0:
                try:
                    killfileContent = int(data)
                except ValueError:
                    LOGGER.warning("Invalid content in killfile.")

        return {
            'exists': killfileExists,
            'content': killfileContent
        }

    def setKillfileInfo(self, content=None):
        """
        :return: Boolean indicating success
        """
        if content is None:
            LOGGER.warning('Put empty killfile')
            with open(settings.KILLFILE, 'w') as f:
                pass
        elif content in [-1, -2, -3]:
            with open(settings.KILLFILE, 'w') as f:
                try:
                    f.write(str(content))
                except IOError as e:
                    LOGGER.warning("Error when storing value in killfile: file=%s value=%s" % (settings.KILLFILE, content))
                    return False
        else:
            LOGGER.warning("Trying to store invalid value in killfile (expected: -1, -2, -3 or None), received: %r" % content)
            return False
        return True

    def removeKillfile(self):
        """
        :return: Boolean indicating success
        """

        if os.path.isfile(settings.KILLFILE):
            try:
                os.remove(settings.KILLFILE)
                LOGGER.warning("Killfile \"%s\" removed" % settings.KILLFILE)
            except Exception as e:
                LOGGER.warning("Error, impossible to remove the kill file: \"%s\" (%s)" % (settings.KILLFILE, e))
                return False
        return True

    def mainLoop(self):
        """
        | Worker main loop:
        | - check kill file and set new status (paused, toberestartted...)
        | - update every modified command watcher for this RN
        | - remove finished commandWatchers for this RN
        | - clean "dead" commandWatchers ("dead" means a timeout val is set on the command and RUNNING time is more thant timeout val)
        | - check ping delay and resync with server if enough time elapsed
        """
        # try:
        now = time.time()

        #
        # check if the killfile is present
        #
        killfileInfo = self.getKillfileInfo()

        if killfileInfo.get('exists'):
            if not self.isPaused:
                # If RN is IDLE/RUNNING/ASSIGNED

                if killfileInfo.get('content') == -1:
                    LOGGER.warning("Flag -1 detected in killfile, killing render and pause RN")
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.setKillfileInfo(None)

                if killfileInfo.get('content') == -2:
                    LOGGER.warning("Flag -2 detected in killfile, schedule restart")
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.toberestarted = True
                    self.removeKillfile()

                if killfileInfo.get('content') == -3:
                    LOGGER.warning("Flag -3 detected in killfile, killing render and schedule restart")
                    self.toberestarted = True
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.removeKillfile()
                else:
                    LOGGER.warning("Empty killfile, pausing worker")
                    self.pauseWorker(paused=True, killproc=False)

            else:
                # If RN is already paused

                # If killfile has a content (i.e. action is needed), exec action, else nothing to do
                if killfileInfo.get('content') == -1:
                    LOGGER.warning("Flag -1 detected in killfile, killing render")
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.setKillfileInfo(None)
                if killfileInfo.get('content') == -2:
                    LOGGER.warning("Flag -2 detected in killfile, schedule restart and put empty killfile to keep pause")
                    self.toberestarted = True
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.setKillfileInfo(None)
                if killfileInfo.get('content') == -3:
                    LOGGER.warning("Flag -3 detected in killfile, killing render and schedule restart")
                    self.toberestarted = True
                    self.killCommandWatchers()
                    self.pauseWorker(paused=True, killproc=True)
                    self.setKillfileInfo(None)
                # else:
                #     LOGGER.warning("Empty killfile, let worker in pause")
        else:
            # self.toberestarted = False
            # if no killfile present and worker is paused, unpause it
            if self.isPaused:
                self.pauseWorker(False, False)

        # if the worker is paused and marked to be restarted, exit program
        # Once the program has ended, the systemd service manager will automatocally restart it.
        # if self.isPaused and self.toberestarted:
        if self.toberestarted:
            # self.removeKillfile()

            LOGGER.warning("Exiting worker")
            self.framework.stop()

        #
        # Waits for any child process, non-blocking (this is necessary to clean up finished process properly)
        #
        try:
            pid, stat = os.waitpid(-1, os.WNOHANG)
            if pid:
                LOGGER.info("Cleaned process %s" % str(pid))

                # Check if pid is still in command watchers
                # In this case, clean the cmdwatcher and put cmd in error
                for commandWatcher in self.commandWatchers.values():
                    if pid == commandWatcher.processId:

                        if commandWatcher.command.status == COMMAND.CMD_RUNNING:
                            LOGGER.warning("Command was considered RUNNING: set to ERROR")
                            newStatus = COMMAND.CMD_ERROR
                        else:
                            LOGGER.warning("Keep current status: %r", commandWatcher.command.status)
                            newStatus = commandWatcher.command.status

                        LOGGER.warning("CommandWatcher killed but still referenced: %s", commandWatcher)
                        commandWatcher.finished = True

                        self.updateCompletionAndStatus(commandWatcher.commandId, commandWatcher.command.completion, newStatus, "Command termination not properly tracked.")

        except OSError:
            pass

        #
        # Send updates for every modified command watcher.
        #
        for commandWatcher in self.modifiedCommandWatchers:
            self.updateCommandWatcher(commandWatcher)

        #
        # Attempt to remove finished command watchers
        #
        for commandWatcher in self.finishedCommandWatchers:
            LOGGER.info("Removing command watcher %d (status=%r, finished=%r, modified=%r)", commandWatcher.command.id, commandWatcher.command.status, commandWatcher.finished, commandWatcher.modified)
            self.removeCommandWatcher(commandWatcher)

        #
        # Kill watchers that timeout and remove dead command watchers
        # that are not flagged as modified.
        #
        for commandWatcher in self.commandWatchers.values():
            # add the test on running state because a non running command can not timeout (Olivier Derpierre 17/11/10)
            if commandWatcher.timeOut and commandWatcher.command.status == COMMAND.CMD_RUNNING:
                responding = (now - commandWatcher.startTime) <= commandWatcher.timeOut
                if not responding:
                    # time out has been reached
                    LOGGER.warning("Timeout on command %d", commandWatcher.commandId)
                    commandWatcher.processObj.kill()
                    commandWatcher.finished = True
                    self.updateCompletionAndStatus(commandWatcher.commandId, None, COMMAND.CMD_CANCELED, None)

        # time resync
        now = time.time()

        if (now - self.lastFullSysInfoUpdate) > config.WORKER_MAX_SYSINFO_DELAY:
            # Every WORKER_MAX_SYSINFO_DELAY a request is sent to ensure a complete set of data is present on the server
            # - WORKER_MAX_SYSINFO_DELAY should be higher that WORKER_SYSINFO_DELAY
            # - WORKER_MAX_SYSINFO_DELAY could be several minutes to avoid flooding the network
            self.updateSysInfos(0)
            self.lastFullSysInfoUpdate = now

        if (now - self.lastSysInfosMessageTime) > config.WORKER_SYSINFO_DELAY:
            # Every WORKER_SYSINFO_DELAY, sends a minimal set of data to the server
            self.sendSysInfosMessage()
            self.lastSysInfosMessageTime = now

        self.httpconn.close()

        # let's be CPU friendly
        time.sleep(0.05)
        # except:
        #     LOGGER.error("A problem occured : " + repr(sys.exc_info()))

    def sendSysInfosMessage(self):
        """
        | Send sys infos to the dispatcher, the request content holds the RN status and free memory only, it has to be kept
        | very small to avoid nerwork flood.
        | req: PUT /rendernodes/<currentRN>/sysinfos

        :raise : Exception
        """

        # we don't need to send the whole dict of sysinfos
        infos = {}

        if self.updateSys:
            # If necessary (i.e. specified by user via WS)
            infos = self.fetchSysInfos()
            self.updateSys = False

        infos['status'] = self.status
        infos['systemFreeRam'] = self.getFreeMem()
        infos['systemSwapPercentage'] = self.getSwapUsage()

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

        LOGGER.debug('Sys infos transmitted to the server: %r' % dct)

    def connect(self):
        return httplib.HTTPConnection(settings.DISPATCHER_ADDRESS, settings.DISPATCHER_PORT)

    def removeCommandWatcher(self, commandWatcher):
        print "\nREMOVING COMMAND WATCHER %d\n" % commandWatcher.command.id

        del self.commandWatchers[commandWatcher.commandId]
        del self.commands[commandWatcher.commandId]
        try:
            os.remove(commandWatcher.processObj.pidfile)
            if self.status is not rendernode.RN_PAUSED:
                # Only set status to IDLE if RN was not marked as pause (via mylawn or pulback)
                self.status = rendernode.RN_IDLE

        except OSError, e:
            from errno import ENOENT
            err, msg = e.args
            LOGGER.exception(msg)
            if err != ENOENT:
                raise

    def updateCompletionAndStatus(self, commandId, completion, status, message, stats=None):
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

            # Add a stats dict that will allow runner to send back useful data on the server.
            # Data can be large, need to avoid to send it every command update.
            # The stats value is None when no update need to be updated on the server.
            commandWatcher.command.stats = stats

    def addCommandApply(self, ticket, commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment, runnerPackages=None, watcherPackages=None):
        if not self.isPaused:
            try:
                newCommand = Command(commandId, runner, arguments, validationExpression, taskName, relativePathToLogDir, environment=environment, runnerPackages=runnerPackages, watcherPackages=watcherPackages)
                self.commands[commandId] = newCommand
                self.addCommandWatcher(newCommand)
                LOGGER.info("Added command %d {runner: %s, arguments: %s}", commandId, runner, repr(arguments))
            except Exception, e:
                LOGGER.error("Error during command init: %r" % e)
                raise e
        else:
            raise WorkerInternalException("Worker flag 'isPaused' is on.")

    ##
    #
    # @param id the integer value identifying the command
    # @todo add a ticket parameter
    # @todo find a clean way to stop the processes so that they \
    #       can call their after-execution scripts
    #
    def stopCommandApply(self, ticket, commandId):
        try:
            commandWatcher = self.commandWatchers[commandId]
        except KeyError:
            LOGGER.warning("Attempt to stop an unregistered command %d", commandId)
        else:
            commandWatcher.processObj.kill()
            self.updateCompletionAndStatus(commandId, 0, COMMAND.CMD_CANCELED, "killed")
            self.ensureNoMoreRender()
            LOGGER.info("Stopped command %r", commandId)

    def updateCommandApply(self, ticket, commandId, status, completion, message, stats):
        self.updateCompletionAndStatus(commandId, completion, status, message, stats)
        # LOGGER.info("Updated command id=%r status=%r completion=%r message=%r stats=%r" % (commandId, status, completion, message, stats))

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

        # Fix PB PRECOMPILATION (notamment quand on exec en local, il peut y avoir un mix)
        # scriptFile = commandwatcher.__file__
        scriptFile = commandwatcher.__file__[:-1] if commandwatcher.__file__.endswith(".pyc") else commandwatcher.__file__
        LOGGER.info("Use commandWatcher script file: %r", scriptFile)

        workerPort = self.framework.webService.port

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
                import errno
                err = e.args[0]
                if err != errno.EEXIST:
                    raise

        # Add several info in env to be used by the runner
        command.environment["PULI_COMMAND_ID"] = command.id
        command.environment["PULI_RUNNER_PACKAGES"] = command.runnerPackages
        command.environment["PULI_TASK_NAME"] = command.taskName
        command.environment["PULI_TASK_ID"] = command.relativePathToLogDir
        command.environment["PULI_LOG"] = outputFile

        # LOGGER.debug("command.runnerPackages = %s" % command.runnerPackages)
        # LOGGER.debug("command.watcherPackages = %s" % command.watcherPackages)
        if 'REZ_USED_RESOLVE' in os.environ:
            pythonExec = "python"
            runnerPackages = command.runnerPackages if command.runnerPackages != '' else 'undefined'
        else:
            pythonExec = sys.executable
            runnerPackages = "undefined"

        args = [
            pythonExec,
            "-u",
            scriptFile,
            commandWatcherLogFile,
            str(settings.DISPATCHER_ADDRESS + ":" + str(settings.DISPATCHER_PORT)),
            str(workerPort),
            str(command.id),
            command.runner,
            command.validationExpression,
            runnerPackages,
            json.dumps(command.arguments)
        ]

        try:
            # Starts a new process (via CommandWatcher script) with current command info and environment.
            # The command environment is derived from the current os.env
            if 'REZ_USED_RESOLVE' in os.environ:
                LOGGER.warning("Current worker managed with rez, command watcher packages are: %s" % command.watcherPackages)
                watcherProcess = spawnRezManagedCommandWatcher(pidFile, logFile, args, command.watcherPackages, command.environment)
            else:
                LOGGER.warning("Current worker is not rez-managed (undefined REZ_USED_RESOLVE in env)")
                watcherProcess = spawnCommandWatcher(pidFile, logFile, args, command.environment)

            newCommandWatcher.processObj = watcherProcess
            newCommandWatcher.startTime = time.time()
            newCommandWatcher.timeOut = None
            newCommandWatcher.command = command
            newCommandWatcher.processId = watcherProcess.pid

            self.commandWatchers[command.id] = newCommandWatcher
            self.status = rendernode.RN_WORKING

            LOGGER.info("Started command %d", command.id)
        except Exception, e:
            LOGGER.error("Error spawning command watcher %r", e)
            raise e

    def reloadConfig(self):
        reload(config)
        pass
