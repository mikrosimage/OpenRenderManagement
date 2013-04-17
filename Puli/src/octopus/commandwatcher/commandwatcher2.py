from threading import Thread
from puliclient.jobs import CommandError
import logging
import sys
import os
import time
import traceback
try:
    import simplejson as json
except ImportError:
    import json
import requests

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
        self.logger = logging.getLogger("worker.CmdThreader")
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
        self.logger.debug("abrupt termination for thread \"%s\"" % self.methodName)
        Thread.__stop(self)


class CommandWatcher2(object):
    intervalTimeExec = 10
    threadList = {}

    def __init__(self, workerPort, cmdid, runner, validationExpression, arguments):
        self.id = cmdid
        self.workerPort = workerPort
        self.completion = 0.0
        self.message = "loading command script"
        self.arguments = arguments

        self.finalState = CMD_DONE
        self.logger = logging.getLogger("cmdwatcher")

        self.runnerErrorInExec = None
        self.runnerErrorInPostExec = None

        # check that the job type is a registered one
        from puliclient.jobs import loadCommandRunner
        try:
            runnerClass = loadCommandRunner(runner)
        except ImportError:
            self.logger.exception("Command runner loading failed.")
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)

        self.commandValidationExpression = validationExpression

        # instanciation of the jobtype script
        try:
            self.job = runnerClass()
            self.job.associatedWatcher = self
            self.mainActions()
        except:
            self.logger.exception("CommandWatcher failed. This is a bug, please report it.")
            self.updateCommandStatus(CMD_ERROR)
            sys.exit(1)

    def mainActions(self):
        # validation
        try:
            self.job.validate(self.arguments)
        except Exception:
            self.logger.exception("Caught some unexpected exception while validating command %d." % (self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState)
            return
        # execution
        try:
            self.executeScript()
        except Exception, e:
            self.logger.exception("Caught some unexpected exception (%s) while executing command %d." % (e, self.id))
            self.finalState = CMD_ERROR
            self.updateCommandStatusAndCompletion(self.finalState)
            return
        # check
        self.execScriptChecker()

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
        self.logger.debug('Updating: %s' % status)
        r = requests.put("http://127.0.0.1:%s/commands/%d/" % (self.workerPort, self.id),
                         data=json.dumps({"id": self.id, "status": status}), stream=False)
        if r.status_code != 202:
            self.logger.error('Updating status failed. (%d)' % r.status_code)

    def updateValidatorResult(self, msg, errorInfos):
        self.logger.debug('Updating : %s, %s' % (msg, str(errorInfos)))
        dct = json.dumps({"id": self.id, "validatorMessage": msg, "errorInfos": errorInfos})
        r = requests.put("http://127.0.0.1:%s/commands/%d/" % (self.workerPort, self.id),
                         data=dct, stream=False)
        if r.status_code != 202:
            self.logger.error('Updating msg and errorInfos failed. (%d)' % r.status_code)

    def updateCommandStatusAndCompletion(self, status):
        self.logger.debug('Updating: %s (%s \%)' % (status, str(self.completion*100)))
        dct = json.dumps({"id": self.id, "status": status, "completion": self.completion, "message": self.message})
        reqdone = False
        delay = 0.5
        while not reqdone:
            try:
                r = requests.put("http://127.0.0.1:%s/commands/%d/" % (self.workerPort, self.id),
                                 data=dct, stream=False)
                reqdone = True
                if r.status_code == 202:
                    return
                elif r.status_code == 404:
                    self.logger.warning("Command is not registered anymore on the worker")
                else:
                    self.logger.error("Unexpected response to status update request: %d %s" % (r.status_code, r.text))
            except requests.ConnectionError:
                self.logger.error("Update request failed.")
                time.sleep(delay)
                delay = max(2.0 * delay, 30.0)

    ## Updates the completion of the command.
    #
    def updateCommandCompletion(self):
        self.logger.debug('Updating completion: %s' % self.completion)
        dct = json.dumps({"id": self.id,
                          "completion": self.completion,
                          "message": self.message})
        r = requests.put("http://127.0.0.1:%s/commands/%d/" % (self.workerPort, self.id),
                         data=dct, stream=False)
        if r.status_code != 202:
            self.logger.error('Updating cmd completion failed. (%d)' % r.status_code)

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

        self.updateCommandStatusAndCompletion(self.finalState)

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
        raise

    closeFileDescriptors()

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.setFormatter(logging.Formatter("# [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    CommandWatcher2(workerPort, id, runner, validationExpression, argumentsDict)
