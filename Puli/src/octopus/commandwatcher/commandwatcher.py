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
from puliclient.jobs import CommandError
import logging
import sys
import os
import time
from datetime import timedelta
import traceback
import httplib as http
try:
    import simplejson as json
except ImportError:
    import json

from octopus.core.http import Request
from octopus.core.communication.requestmanager import RequestManager

from octopus.core.enums.command import *

EXEC, POSTEXEC = "execute", "postExecute"

COMMAND_RUNNING = 0
COMMAND_STOPPED = 1
COMMAND_CRASHED = 2
COMMAND_FAILED = 3


## This class is used to thread a command.
#
class CmdThreader(Thread):
    # boolean flag describing whether the thread is stopped or not
    stopped = COMMAND_RUNNING

    ## Creates a new CmdThreader.
    # @param cmd name of the jobtype script
    # @param methodName name of the jobtype method
    #
    def __init__(self, cmd, methodName, arguments, updateCompletion, updateMessage):
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

    ## Runs the specified method of the command.
    #
    def run(self):
        try:
            self.stopped = COMMAND_RUNNING
            getattr(self.cmd, self.methodName)(self.arguments, self.updateCompletion, self.updateMessage)
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

    intervalTimeExec = 3
    intervalTimePostExec = 3
    threadList = {}

    ## Creates a new CmdWatcher.
    #
    # @param id the id of the command
    # @param runner the runner type name
    # @param arguments the arguments of the command
    #
    def __init__(self, workerPort, id, runner, validationExpression, arguments):

        self.id = id
        self.requestManager = RequestManager("127.0.0.1", workerPort)
        self.workerPort = workerPort
        self.completion = 0.0
        self.message = "loading command script"
        self.arguments = arguments
        self.runner = runner

        self.finalState = CMD_DONE
        self.logger = logging.getLogger()

        self.runnerErrorInExec = None
        self.runnerErrorInPostExec = None

        # check that the job type is a registered one
        self.logger.info("Loading runner: %s" % runner)

        from puliclient.jobs import loadCommandRunner, JobTypeImportError
        try:
            runnerClass = loadCommandRunner(runner)
        except JobTypeImportError, e:
            self.logger.error("Command runner loading failed: %r" % e )
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)
        except ImportError, e:
            self.logger.exception("Command runner loading failed: %r" % e )
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)
        except Exception, e:
            self.logger.exception("Unexpected error in loaded runner: %r" % e )
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
            self.logger.exception("CommandWatcher failed. This is a bug, please report it.")
            sys.exit(1)

    ## The main actions.
    #
    def mainActions(self):
        startDate = time.time()
        try:
            self.job.validate(self.arguments)
        except Exception:
            self.logger.exception("Caught some unexpected exception while validating command %d." % (self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return

        try:
            self.logger.info("Starting command: %r" % self.id)
            self.executeScript()
        except Exception:
            self.logger.exception("Caught some unexpected exception (%s) while starting command %d." % (self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return

        try:
            self.execScriptChecker()
        except Exception:
            self.logger.exception("Caught some unexpected exception (%s) while executing command %d." % (self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState, True)
            return

        elapsedTime = time.time() - startDate
        self.logger.info("Finished command %r, elapsed time: %s " % (self.id, timedelta(seconds=int(elapsedTime))))


    ## Creates a thread for the script corresponding to the provided action name.
    # @param action the name of the action to thread (jobtype script method)
    #
    def threadAction(self, action):
        tmpThread = CmdThreader(self.job, action, self.arguments, self.updateCompletionCallback, self.updateMessageCallback)
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

        self.logger.debug('Updating status: %s' % status)
        dct = json.dumps({"id": self.id, "status": status})
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            self.requestManager.put("/commands/%d/" % self.id, dct, headers)
        except http.BadStatusLine:
            self.logger.debug('Updating status has failed with a BadStatusLine error')

    def updateValidatorResult(self, msg, errorInfos):

        if self.workerPort is "0":
            return

        self.logger.debug('Updating msg and errorInfos : %s,%s' % (msg, str(errorInfos)))
        dct = json.dumps({"id": self.id, "validatorMessage": msg, "errorInfos": errorInfos})
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            self.requestManager.put("/commands/%d/" % self.id, dct, headers)
        except http.BadStatusLine:
            self.logger.debug('Updating  msg and errorInfos has failed with a BadStatusLine error')

    def updateCommandStatusAndCompletion(self, status, retry=False):

        if self.workerPort is "0":
            return

        self.logger.debug('Updating status: %s' % status)
        completion = self.completion
        self.logger.debug('Updating completion: %s' % completion)

        body = json.dumps({"id": self.id, "status": status, "completion": completion, "message": self.message})
        headers = {}
        headers['Content-Length'] = len(body)
        headers['Content-Type'] = 'application/json'

        import httplib

        def onResponse(request, response):
            request.done = True
            if response.status == 202:
                return
            elif response.status == 404:
                self.logger.debug("Command is not registered anymore on the worker")
            else:
                self.logger("Unexpected response to status update request: %d %s" % (response.status, response.reason))

        def onError(request, error):
            self.logger.debug("Update request failed: %s", error)

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
        if self.workerPort is "0":
            return

        completion = self.completion
        self.logger.debug('Updating completion: %s' % completion)
        dct = json.dumps({"id": self.id,
                          "completion": completion,
                          "message": self.message})
        headers = {}
        headers['Content-Length'] = len(dct)
        try:
            self.requestManager.put("/commands/%d/" % self.id, dct, headers)
        except http.BadStatusLine:
            self.logger.debug('Updating completion has failed with a BadStatusLine error')

    ## Threads the post execution of the corresponding runner.
    #
    def executeScript(self):
        logger.debug("Starting execution...")
        self.threadAction(EXEC)

    ## Controls the execution of the main command.
    #
    def execScriptChecker(self):
        self.logger.debug("Checking Execution...")

        timeOut = self.job.scriptTimeOut

        while not(self.threadList[EXEC].stopped):
            tmpTime = time.time()

            if self.workerPort is not 0:
                self.updateCommandCompletion()

            if timeOut is not None:
                if timeOut < 0:
                    self.logger.error("execute Script timeout reached !")
                    self.finalState = CMD_ERROR

            if self.finalState == CMD_ERROR or self.finalState == CMD_CANCELED:
                self.killCommand()
                break

            time.sleep(self.intervalTimeExec)

            if timeOut is not None:
                timeOut -= time.time() - tmpTime

        if self.threadList[EXEC].stopped == COMMAND_FAILED:
            self.finalState = CMD_ERROR
            self.logger.error("Error: %s", self.threadList[EXEC].errorInfo)
            self.runnerErrorInExec = str(self.threadList[EXEC].errorInfo)
        elif self.threadList[EXEC].stopped == COMMAND_CRASHED:
            self.logger.error("Job script raised some unexpected exception :")
            error = str(self.threadList[EXEC].errorInfo) or ("None")
            for line in error.strip().split("\n"):
                self.logger.error(line)
            self.finalState = CMD_ERROR
            self.runnerErrorInExec = str(self.threadList[EXEC].errorInfo)
        else:
            self.logger.debug("No more threads to check")

        if self.workerPort is not 0:
            self.updateCommandStatusAndCompletion(self.finalState, True)

    ## Kills all processes launched by the command.
    #
    def killCommand(self):
        # FIXME: maybe we ought to provide a way to ask the command to stop itself
        self.threadList[EXEC].stop()

    def updateCompletionCallback(self, completion):
        self.completion = completion

    def updateMessageCallback(self, message):
        self.message = message


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
        workerPort = sys.argv[2]
        id = int(sys.argv[3])
        runner = sys.argv[4]
        validationExpression = sys.argv[5]
        rawArguments = sys.argv[6:]
        argumentsDict = {}
        for argument in rawArguments:
            arglist = argument.split("=")
            key = arglist[0]
            value = '='.join(arglist[1:])
            argumentsDict[key] = value
    except:
        print "Usage : commandwatcher.py /path/to/the/log/file workerPort id runnerscript argument1=value1,argument2=value2...",
        print ""
        print "Executes and monitor a given script in a separate thread. Completion and message updates are periodically sent back"
        print "to the workerd process to notify puliserver."
        print ""
        print "NOTE: If worker port is \"0\", it means the commandwatcher is intended to be used locally only."
        print "      In this case completion and message updates are ignore as everything is logged."
        raise

    closeFileDescriptors()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # handler = logging.StreamHandler(sys.stdout)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("# [%(levelname)s] %(asctime)s - %(message)s"))
    logger.addHandler(handler)

    CommandWatcher(workerPort, id, runner, validationExpression, argumentsDict)
