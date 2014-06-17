#!/usr/bin/env python
####################################################################################################
# @file commandwatcher.py
# @package
# @author Arnaud Chassagne, Olivier Derpierre, Jean-Baptiste Spieser
# @date 2009/01/12
# @version 0.1
#
# @mainpage
#
####################################################################################################

from threading import Thread
import logging
import sys
import inspect
import os
import socket
import time
from datetime import timedelta
import traceback
import requests

import httplib as http
try:
    import simplejson as json
except ImportError:
    import json

from tornado.web import HTTPError

from puliclient.jobs import CommandError
from octopus.core.http import Request
from octopus.core.communication.requestmanager import RequestManager
from octopus.core.enums.command import *
from octopus.worker import settings

EXEC, POSTEXEC = "execute", "postExecute"

COMMAND_RUNNING = 0
COMMAND_STOPPED = 1
COMMAND_CRASHED = 2
COMMAND_FAILED = 3


logger = logging.getLogger('puli.commandwatcher')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stderr)

FORMAT = '# [%(levelname)s] %(asctime)s - %(message)s'
DATE_FORMAT = '%b %d %H:%M:%S'

handler.setFormatter( logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT) )
logger.addHandler(handler)


## This class is used to thread a command.
#
class CmdThreader(Thread):
    # boolean flag describing whether the thread is stopped or not
    stopped = COMMAND_RUNNING

    ## Creates a new CmdThreader.
    # @param cmd name of the jobtype script
    # @param methodName name of the jobtype method
    #
    def __init__(self, cmd, methodName, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        Thread.__init__(self)

        self.logger = logging.getLogger()
        self.logger.debug("cmd = %s" % cmd)
        self.logger.debug("methodName = %s" % methodName)
        self.cmd = cmd
        self.methodName = methodName
        self.errorInfo = None
        self.arguments = arguments
        self.updateCompletion = updateCompletion
        self.updateMessage = updateMessage
        self.updateStats = updateStats
        self.updateLicense = updateLicense

    ## Runs the specified method of the command.
    #
    def run(self):
        try:
            self.stopped = COMMAND_RUNNING

            # 
            # HACK: inspect method args to see if updateStats must be passed as argument or not
            #
            constructArgs = [ self.arguments, self.updateCompletion, self.updateMessage ]
            if 'updateStats' in inspect.getargspec( getattr(self.cmd, self.methodName) ).args:
                constructArgs.append( self.updateStats )
            if 'updateLicense' in inspect.getargspec( getattr(self.cmd, self.methodName) ).args:
                constructArgs.append( self.updateLicense )


            getattr(self.cmd, self.methodName)( *constructArgs )

            self.stopped = COMMAND_STOPPED
        except CommandError, e:
            self.errorInfo = str(e)
            self.stopped = COMMAND_FAILED
        except Exception, e:
            self.errorInfo = (str(e) + "\n" + str(traceback.format_exc()))
            self.stopped = COMMAND_CRASHED

    ## Stops the thread.
    #
    def stop(self):
        self.stopped = COMMAND_STOPPED
        self.logger.warning("Abrupt termination for thread \"%s\"" % self.methodName)
        Thread.__stop(self)


## This class is used to ensure the good execution of the CmdThreader's process.
#
class CommandWatcher(object):

    intervalTimeExec = 1
    maxRefreshDataDelay = 30
    intervalTimePostExec = 3
    threadList = {}
    lastUpdateDate = time.time()

    ## Creates a new CmdWatcher.
    #
    # @param id the id of the command
    # @param runner the runner type name
    # @param arguments the arguments of the command
    #
    def __init__(self, serverFullName, workerPort, id, runner, validationExpression, arguments):

        self.id = id
        self.requestManager = RequestManager("127.0.0.1", workerPort)
        self.workerPort = workerPort
        self.workerFullName = socket.gethostname()+":"+self.workerPort
        self.serverFullName = serverFullName
        self.arguments = arguments
        self.runner = runner

        self.completion = 0.0
        self.message = "loading command script"
        self.stats = {}

        self.completionHasChanged = True
        self.messageHasChanged = True
        self.statsHasChanged = True

        self.finalState = CMD_DONE

        self.runnerErrorInExec = None
        self.runnerErrorInPostExec = None

        # check that the job type is a registered one
        runnerLabel = runner.rsplit('.', 1)[1:][0]
        logger.info("Loading class: \"%s\"" % runnerLabel)

        from puliclient.jobs import loadCommandRunner, JobTypeImportError
        try:
            runnerClass = loadCommandRunner(runner)
        except JobTypeImportError, e:
            logger.error("Command runner loading failed: %r" % e )
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)
        except ImportError, e:
            logger.exception("Command runner loading failed: %r" % e )
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)
        except Exception, e:
            logger.exception("Unexpected error in loaded runner: %r" % e )
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)

        self.commandValidationExpression = validationExpression

        # instanciation of the jobtype script
        try:
            self.job = runnerClass()
            self.job.associatedWatcher = self
            self.mainActions()
        except Exception:
            self.updateCommandStatus(CMD_ERROR)
            logger.exception("CommandWatcher failed. This is a bug, please report it.")
            sys.exit(1)

    ## The main actions.
    #
    def mainActions(self):
        startDate = time.time()
        try:
            self.job.validate(self.arguments)
        except Exception,e:
            logger.warning("Caught exception (%r) while starting command %d." % (e, self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return self.finalState

        try:
            logger.info("Starting command: %r" % self.id)
            self.executeScript()
        except Exception, e:
            logger.warning("Caught exception (%r) while starting command %d." % (e, self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return self.finalState

        try:
            self.execScriptChecker()
        except Exception, e:
            logger.warning("Caught exception (%r) while executing command %d." % (e, self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return self.finalState

        elapsedTime = time.time() - startDate
        logger.info("Finished command %r (status %r), elapsed time: %s " % (self.id, CMD_STATUS_NAME[self.finalState], timedelta(seconds=int(elapsedTime))))


    ## Creates a thread for the script corresponding to the provided action name.
    # @param action the name of the action to thread (jobtype script method)
    #
    def threadAction(self, action):
        tmpThread = CmdThreader(self.job, action, self.arguments, self.updateCompletionCallback, self.updateMessageCallback, self.updateCustomStatsCallback, self.updateLicenseCallback)
        tmpThread.setName('jobMain')
        # add this thread to the list
        self.threadList[action] = tmpThread
        # launch it
        tmpThread.start()

    ## Updates the status of the command.
    # @param status
    #
    def updateCommandStatus(self, status):

        if self.workerPort is "0":
            return

        dct = json.dumps({"id": self.id, "status": status})
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            result = self.requestManager.put("/commands/%d/" % self.id, dct, headers)
            logger.error('Update command request failed: result=%r' % result)


        except http.BadStatusLine:
            logger.debug('Updating status has failed with a BadStatusLine error')

    def updateValidatorResult(self, msg, errorInfos):
        """
        FIXME: NEVER CALLED ??! WTF
        """

        if self.workerPort is "0":
            return

        # logger.debug('Updating msg and errorInfos : %s,%s' % (msg, str(errorInfos)))
        dct = json.dumps({"id": self.id, "validatorMessage": msg, "errorInfos": errorInfos})
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            self.requestManager.put("/commands/%d/" % self.id, dct, headers)
        except http.BadStatusLine:
            logger.debug('Updating  msg and errorInfos has failed with a BadStatusLine error')

    def updateCommandStatusAndCompletion(self, status, retry=False):

        if self.workerPort is "0":
            return

        body = json.dumps({"id": self.id, "status": status, "completion": self.completion, "message": self.message, "stats" : self.stats})

        headers = {}
        headers['Content-Length'] = len(body)
        headers['Content-Type'] = 'application/json'

        import httplib

        def onResponse(request, response):
            request.done = True
            if response.status == 202:
                return
            elif response.status == 404:
                logger.debug("Command is not registered anymore on the worker")
            else:
                logger("Unexpected response to status update request: %d %s" % (response.status, response.reason))

        def onError(request, error):
            logger.debug("Update request failed: %s", error)

        delay = 0.5
        request = Request('PUT', '/commands/%d/' % self.id, headers, body)
        request.done = False
        conn = httplib.HTTPConnection('127.0.0.1', self.workerPort)
        while retry and not request.done:
            request.call(conn, onResponse, onError)
            conn.close()
            time.sleep(delay)
            delay = max(2.0 * delay, 30.0)

    ## Updates the completion of the command.
    #
    def updateCommandCompletion(self):
        """
        Sends info to the server every intervalTimeExec (several seconds).
        Info sent is a dict with: commandid, completion, message and custom stats dict

        Update checks if the data has change between last update to avoid sending same value to frequently.
        A maxRefreshDataDelay will force a resend even if data is identicial to ensure server consistency.
        """
        if self.workerPort is "0":
            return

        elapsedTimeSinceLastUpdate = time.time() - self.lastUpdateDate
        
        # If no change since last update AND the last update is not too old --> exit without sending update
        if elapsedTimeSinceLastUpdate < self.maxRefreshDataDelay:
            if not self.messageHasChanged and not self.completionHasChanged and not self.statsHasChanged :
                logger.debug('Nothing changed, no need to update')
                return
        else:
            logger.debug('Maximum refresh delay reached (%rs), force refresh even if nothing has changed' % self.maxRefreshDataDelay)


        data = {}
        if self.completionHasChanged:
            data["completion"] = self.completion
        if self.messageHasChanged:
            data["message"] = self.message
        if self.statsHasChanged and self.stats is not {}:
            data["stats"] = self.stats

        dct = json.dumps( data )
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            self.requestManager.put("/commands/%d/" % self.id, dct, headers)
        except http.BadStatusLine:
            logger.debug('Updating completion has failed with a BadStatusLine error')

        # Reset update flags
        self.messageHasChanged = False
        self.completionHasChanged = False
        self.statsHasChanged = False
        self.lastUpdateDate = time.time()


    def releaseLicense(self, licenseName):
        """
        | Sends a request to release a license for the current node
        | A request releases only one token, i.e. a tuple (RN, license)
        | The command watcher will make several attempts (with 200ms delay btw each)
        | Request detail:
        |   -url: http://server:port/licenses/<licenseName>
        |   -method: DELETE
        |   -body: a dict containing the worker address: { "rns":"workerAddress:port" }

        :param licenseName: the license name, usally "shave", "mtoa", "nuke"
        :type licenseName: string
        """
        if self.workerPort is "0":
            return

        try:
            body = json.dumps({"rns":self.workerFullName})
            url = "http://%s/licenses/%s" % (self.serverFullName, licenseName)

            logger.info("Releasing license: %s - %s", body, url)
            for i in range(10):
                r=requests.delete(url, data=body)
                if r.status_code in [200,202]: 
                    logger.info("License released successfully, response = %s" % r.text)
                    res = True
                    break
                else:
                    logger.error("Error releasing license, response = %s" % r.text)
                    res = False

                logger.warning("Impossible to release license (attempt %d/10)" % (i+1) )
                time.sleep(.2)

        except (HTTPError, ConnectionError) as e:
            print "Network error:", e
            res = False
        except Exception as e:
            print "Unknow error:", e
            res = False


        return res



    ## Threads the post execution of the corresponding runner.
    #
    def executeScript(self):
        logger.debug("Starting execution...")
        self.threadAction(EXEC)

    ## Controls the execution of the main command.
    #
    def execScriptChecker(self):
        logger.debug("Checking Execution...")

        timeOut = self.job.scriptTimeOut

        while not(self.threadList[EXEC].stopped):
            tmpTime = time.time()

            if self.workerPort is not 0:
                self.updateCommandCompletion()

            if timeOut is not None:
                if timeOut < 0:
                    logger.error("execute Script timeout reached !")
                    self.finalState = CMD_ERROR

            if self.finalState == CMD_ERROR or self.finalState == CMD_CANCELED:
                self.killCommand()
                break

            time.sleep(self.intervalTimeExec)

            if timeOut is not None:
                timeOut -= time.time() - tmpTime

        if self.threadList[EXEC].stopped == COMMAND_FAILED:
            self.finalState = CMD_ERROR
            logger.error("Error: %s", self.threadList[EXEC].errorInfo)
            self.runnerErrorInExec = str(self.threadList[EXEC].errorInfo)
        elif self.threadList[EXEC].stopped == COMMAND_CRASHED:
            logger.error("Job script raised some unexpected exception :")
            error = str(self.threadList[EXEC].errorInfo) or ("None")
            for line in error.strip().split("\n"):
                logger.error(line)
            self.finalState = CMD_ERROR
            self.runnerErrorInExec = str(self.threadList[EXEC].errorInfo)
        else:
            logger.debug("No more threads to check")

        if self.workerPort is not 0:
            self.updateCommandStatusAndCompletion(self.finalState, True)

    ## Kills all processes launched by the command.
    #
    def killCommand(self):
        # FIXME: maybe we ought to provide a way to ask the command to stop itself
        self.threadList[EXEC].stop()

    def updateCompletionCallback(self, completion):

        if completion != self.completion:
            self.completion = completion
            self.completionHasChanged = True
        # else:
        #     logger.debug( "Completion updated from runner but value is identical as previous update." )

    def updateMessageCallback(self, message):
        if message != self.message:
            self.message = message
            self.messageHasChanged = True
        # else:
        #     logger.debug( "Message updated from runner but value is identical as previous update." )

    def updateCustomStatsCallback(self, pStats):
        """
        IPC between the runner thread and the commandwatcher.
        It is used to report custom data from the runner class (generally datas extracted from the process log) to the dispatcher.
        As the data can be large, a flag is maintained indicating if a change occured and if the data has already been updated on the server.
        """
        # TODO
        #  - evaluate diff betwenn the new val and previous val
        #    if value is updated: change flag is set to True
        #  - check if data size when dumped to text is less than 64k i.e. if it can fit in DB backend, otherwise reject update

        if type(pStats) is dict:
            self.stats = pStats
            self.statsHasChanged = True
        else:
            logger.warning("Impossible to update stats: dictionnary expected but \"%r\" was received" % type(pStats) )


    def updateLicenseCallback(self, pLicenseInfo):
        """
        Specific callback used to reserve/release a license on the server.
        Note, it is ONLY POSSIBLE TO RELEASE a license.
        Remote reservation from the worker is not implemented yet.

        :param licenseInfo: Information about the license and action to perform.
        :type licenseInfo: dict {'action':'release', 'licenseName':'shave'}
        """
        logger.debug("Updating license: %r" % type(pLicenseInfo) )

        if type(pLicenseInfo) is not dict:
            logger.warning("Impossible to update license: dictionnary expected but \"%r\" was received" % type(pLicenseInfo) )
            return False

        if "action" not in pLicenseInfo or "licenseName" not in pLicenseInfo:
            logger.warning("Impossible to update license: missing action or license name in given dict: \"%r\"" % pLicenseInfo )
            return False
        
        if pLicenseInfo["licenseName"].strip()=='':
            logger.warning("Impossible to update license: license name is empty: \"%r\"" % pLicenseInfo )
            return False
        

        if pLicenseInfo["action"] == 'reserve':
            # result = self.reserveLicense( pLicenseInfo["licenseName"].strip() )
            logger.warning("Currently not implemented.")
            result = False

        elif pLicenseInfo["action"] == 'release':
            result = self.releaseLicense( pLicenseInfo["licenseName"].strip() )
        else:
            logger.warning("Impossible to update license: invalid action specified, got \"%s\", 'reserve' or 'release' expected" % pLicenseInfo["action"] )
            return False

        return result


def closeFileDescriptors():
    '''Close all the file descriptors inherited from the parent except for stdin, stdout and stderr.'''
    if os.name == 'posix':
        import resource
        MAXFILENO = resource.getrlimit(resource.RLIMIT_NOFILE)
        for fd in xrange(3, MAXFILENO[0]):
            try:
                os.close(fd)
            except OSError:
                pass
    else:
        pass


if __name__ == "__main__":

    try:
        logFile = sys.argv[1]
        serverFullName = sys.argv[2]
        workerPort = sys.argv[3]
        id = int(sys.argv[4])
        runner = sys.argv[5]
        validationExpression = sys.argv[6]
        rawArguments = sys.argv[7:]
        
        # ARGH !
        # Receiveing arguments as string and loosing type info...
        # Change this to receive a serialized dict and use ast.literal_eval to load it properly
        argumentsDict = {}
        for argument in rawArguments:
            arglist = argument.split("=")
            key = arglist[0]
            value = '='.join(arglist[1:])
            argumentsDict[key] = value

        # import ast
        # argumentsDict = {}
        # argumentsDict = ast.literal_eval(rawArguments[0])

    except Exception, e:
        # print "Usage : commandwatcher.py /path/to/the/log/file workerPort id runnerscript argument1=value1,argument2=value2...",
        print "Usage : commandwatcher.py /path/to/the/log/file workerPort id runnerscript \"{'argument1':'strvalue1', 'argument2'=intvalue2... }\"",
        print ""
        print "Executes and monitor a given script in a separate thread. Completion and message updates are periodically sent back"
        print "to the workerd process to notify puliserver."
        print ""
        print "NOTE: If worker port is \"0\", it means the commandwatcher is intended to be used locally only."
        print "      In this case completion and message updates are ignore as everything is logged."
        logger.warning("Invalid call to the CommandWatcher script: %r" % e)
        closeFileDescriptors()
        sys.exit(1)

    closeFileDescriptors()

    # logger = logging.getLogger()
    # logger.setLevel(logging.INFO)

    # handler = logging.StreamHandler(sys.stderr)
    
    # FORMAT = '# [%(levelname)s] %(asctime)s - %(message)s'
    # DATE_FORMAT = '%b %d %H:%M:%S'

    # handler.setFormatter( logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT) )
    # logger.addHandler(handler)

    try:
        CommandWatcher(serverFullName, workerPort, id, runner, validationExpression, argumentsDict)
    except KeyboardInterrupt, e:
        print("\n")
        logger.warning("Exit event caught: exiting CommandWatcher...\n")
        sys.exit(0)
    except Exception, e:
        logger.warning("Exception raised during commandwatcher init: %r" % e)
        sys.exit(1)
