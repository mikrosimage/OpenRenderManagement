'''
Created on Jan 11, 2010

@author: Olivier Derpierre
'''

import sys
import site
import traceback
import logging
import subprocess

try:
    import simplejson as json
except ImportError:
    import json


#
# Errors specific to command execution
#
class TimeoutError (Exception):
    ''' Raised when helper execution is too long. '''


class CommandDone(Exception):
    '''Raised to manually end a command execution.'''


class CommandError(Exception):
    '''Raised to signal failure of a CommandRunner execution.'''


class ValidationError(CommandError):
    '''Raised on a validation error'''


class RangeError(CommandError):
    '''Raised on a validation error where a value given is out of authorized range'''


class JobTypeImportError(ImportError):
    '''Raised when an error occurs while loading a job type through the load function'''


class CommandRunnerParameter(object):
    '''Base class for formal command runner parameter.'''

    name = None
    isMandatory = False

    def __init__(self, default=None, mandatory=None, **kwargs):
        if default is not None:
            self.hasDefault = True
            self.defaultValue = default
        else:
            self.hasDefault = False
            self.defaultValue = None

        if mandatory is not None:
            self.isMandatory = True

        # Set range values if given in args, still need to be validated against value
        # TODO specialize it in typed parameter classes
        if 'min' in kwargs:
            self.minValue = kwargs['min']
        if 'max' in kwargs:
            self.maxValue = kwargs['max']

    def validate(self, arguments):
        if not self.name in arguments and self.isMandatory:
            raise ValidationError("Mandatory argument \"%s\" is not defined in command arguments" % self.name)

        if not self.name in arguments and self.hasDefault:
            arguments[self.name] = self.defaultValue

    def __repr__(self):
        return "CommandRunnerParameter(name=%r, default=%r, mandatory=%r)" % (self.name, self.defaultValue, self.isMandatory)

    def __str__(self):
        return "%r (default=%r, mandatory=%r)" % (self.name, self.defaultValue, self.isMandatory)


class StringParameter(CommandRunnerParameter):
    '''A command runner parameter class that converts the argument value to a string.'''

    def validate(self, arguments):
        try:
            super(StringParameter, self).validate(arguments)
            if self.name in arguments:
                arguments[self.name] = str(arguments[self.name])
        except Exception, e:
            raise e


class StringListParameter(CommandRunnerParameter):

    def validate(self, arguments):
        try:
            super(StringListParameter, self).validate(arguments)

            arguments[self.name] = [str(v) for v in arguments[self.name]]
        except Exception, e:
            raise e


class BooleanParameter(CommandRunnerParameter):

    def validate(self, arguments):
        try:
            super(BooleanParameter, self).validate(arguments)
            arguments[self.name] = bool(arguments[self.name])
        except Exception, e:
            raise e


class IntegerParameter(CommandRunnerParameter):
    '''A command runner parameter class that converts the argument value to an integer value.'''

    minValue = None
    maxValue = None

    def validate(self, arguments):
        try:
            # Base class will check if argument is present or ensure it has its default value
            super(IntegerParameter, self).validate(arguments)

            if arguments[self.name]:
                newVal = int(arguments[self.name])
                # Validate range if defined
                if self.minValue is not None and newVal < self.minValue:
                    raise RangeError("Argument \"%s\"=%d is less than minimum: %d" % (
                        self.name,
                        newVal,
                        self.minValue))

                if self.maxValue is not None and self.maxValue < newVal:
                    raise RangeError("Argument \"%s\"=%d is more than maximum: %d" % (
                        self.name,
                        self.maxValue,
                        newVal))

                arguments[self.name] = newVal
        except RangeError, e:
            raise e
        except ValidationError, e:
            raise e

    def __repr__(self):
        return "IntParameter(name=%r, default=%r, mandatory=%r, minValue=%r, maxValue=%r )" % (
            self.name,
            self.defaultValue,
            self.isMandatory,
            self.minValue,
            self.maxValue,
        )

    def __str__(self):
        return "%r (default=%r, mandatory=%r, range=[%r,%r])" % (self.name, self.defaultValue, self.isMandatory, self.minValue, self.maxValue)


class FloatParameter(CommandRunnerParameter):
    '''A command runner parameter class that converts the argument value to an float value.'''

    minValue = None
    maxValue = None

    def validate(self, arguments):
        try:
            super(FloatParameter, self).validate(arguments)

            if arguments[self.name]:
                newVal = float(arguments[self.name])
                # Validate range if defined
                if self.minValue is not None and newVal < self.minValue:
                    raise RangeError("Argument \"%s\"=%d is less than minimum: %d" % (
                        self.name,
                        newVal,
                        self.minValue))

                if self.maxValue is not None and self.maxValue < newVal:
                    raise RangeError("Argument \"%s\"=%d is more than maximum: %d" % (
                        self.name,
                        self.maxValue,
                        newVal))

                arguments[self.name] = newVal

        except Exception, e:
            raise e

    def __repr__(self):
        return "FloatParameter(name=%r, default=%r, mandatory=%r, minValue=%r, maxValue=%r )" % (
            self.name,
            self.defaultValue,
            self.isMandatory,
            self.minValue,
            self.maxValue,
        )

    def __str__(self):
        return "%r (default=%r, mandatory=%r, range=[%r,%r])" % (self.name, self.defaultValue, self.isMandatory, self.minValue, self.maxValue)


class CommandRunnerMetaclass(type):

    def __init__(self, name, bases, attributes):
        type.__init__(self, name, bases, attributes)
        parameters = attributes.get('parameters', [])
        for base in bases:
            if isinstance(base, CommandRunnerMetaclass):
                parameters += base.parameters
        for (name, arg) in attributes.iteritems():
            if isinstance(arg, CommandRunnerParameter):
                arg.name = name
                parameters.append(arg)
        self.parameters = parameters

        # logging.getLogger('puli.commandwatcher').info('init CommandRunnerMetaclass')


class CommandRunner(object):

    __metaclass__ = CommandRunnerMetaclass

    log = logging.getLogger('puli.runner')
    scriptTimeOut = None
    parameters = []

    def execute(self, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        raise NotImplementedError

    def validate(self, arguments):
        logger = logging.getLogger('puli.commandwatcher')

        if len(self.parameters) > 0:
            logger.info("Validating %d parameter(s):" % len(self.parameters))

        for parameter in self.parameters:
            logger.info("  - %s" % parameter)
            parameter.validate(arguments)

        # TOFIX no need for scripttimeOut, impossible to kill a thread manually, timeout handling
        # should be done outside the command runner or around subprocess call in the runner

        # # Checking global argument scriptTimeOut:
        # try:
        #     self.scriptTimeOut = int(arguments['scriptTimeOut'])
        #     logger.info("Defining time out limit: scriptTimeout=%d" % self.scriptTimeOut)
        # except KeyError, e:
        #     logger.info("No scriptTimeout in arguments. Command will never be interrupted (msg: %s)" % e)
        # except TypeError, e:
        #     logger.info("Invalid scriptTimeout value given (integer expected) (msg: %s)" % e)


class DefaultCommandRunner(CommandRunner):

    cmd = StringParameter(mandatory=True)
    start = IntegerParameter(default=1)
    end = IntegerParameter(default=1)
    timeout = IntegerParameter(default=0, min=0)

    def execute(self, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        '''
        | Simple execution using the helper. Default argument "cmd" is expected (mandatory)
        | to start the execution with the current env.
        |
        | If a command is defined on a range, the cmd will be executed several time using start/end arguments.
        | The command can use several standard replacement values:
        | %%MI_FRAME%% -> replaced by the current frame value
        | %%MI_START%% -> replaced by the index of the first frame of the range
        | %%MI_END%% -> replaced by the index of the last frame of the range
        |
        | For instance if a command is defined like this:
        |   - start = "10"
        |   - end = "15"
        |   - cmd = "nuke -x -F %%MI_FRAME%% ma_comp.nk"
        |     or cmd = "nuke -x -F %%MI_START%%-%%MI_END%% ma_comp.nk"
        |
        | The runner will produce the following execution:
        | nuke -x -F 10 ma_comp.nk
        | nuke -x -F 11 ma_comp.nk
        | nuke -x -F 12 ma_comp.nk
        | nuke -x -F 13 ma_comp.nk
        | nuke -x -F 14 ma_comp.nk
        | nuke -x -F 15 ma_comp.nk
        '''

        cmd = arguments.get('cmd')
        start = arguments.get('start')
        end = arguments.get('end')
        timeout = arguments.get('timeout', None)

        updateCompletion(0)

        completion = 0.0
        completionIncrement = 1.0 / float((int(end) + 1) - int(start))

        for frame in range(start, end + 1):
            self.log.info("==== Frame %d ====" % frame)

            currCommand = cmd.replace("%%MI_FRAME%%", str(frame))
            currCommand = currCommand.replace("%%MI_START%%", str(start))
            currCommand = currCommand.replace("%%MI_END%%", str(end))

            self.log.info("Command: %s" % currCommand)
            subprocess.check_call(currCommand, close_fds=True, shell=True)

            completion += completionIncrement
            updateCompletion(completion)
            self.log.info("Updating completion %f " % completion)

        updateCompletion(1)


class TaskExpander(object):

    def __init__(self, taskGroup):
        pass


class TaskDecomposer(object):
    """
    | Base class for Decomposer hierarchy.
    | Implements a minimalist "addCommand" method.
    """

    def __init__(self, task):
        self.task = task

    def addCommand(self, name, args, runnerPackages=None, watcherPackages=None):
        self.task.addCommand(name, args, runnerPackages, watcherPackages)


class DefaultTaskDecomposer(TaskDecomposer):
    """
    | Default decomposesr called when no decomposer given for a task. It will use the PuliActionHelper to create one
    | or several commands on a task. PuliActionHelper's decompose method will have the following behaviour:
    |   - if "framesList" is defined:
    |         create a command for each frame indicated (frameList is a string with frame numbers separated by spaces)
    |   - else:
    |         try to use start/end/packetSize attributes to create several commands (frames grouped by packetSize)
    |
    | If no "arguments" dict is given, print a warning and create a single command with empty arguments.
    """

    # DEFAULT FIELDS USED TO DECOMPOSE A TASK
    START_LABEL = "start"
    END_LABEL = "end"
    PACKETSIZE_LABEL = "packetSize"
    FRAMESLIST_LABEL = "framesList"

    def __init__(self, task):
        """

        :type task: object
        """
        super(DefaultTaskDecomposer, self).__init__(task)

        if task.arguments is None:
            # Create an empty command anyway --> probably unecessary
            print "WARNING: No arguments given for the task \"%s\", it is necessary to do this ? (we are creating an empty command anyway..." % task.name
            self.task.addCommand(task.name + "_1_1", {})

        elif all(key in task.arguments for key in (self.START_LABEL, self.END_LABEL)) \
                or self.FRAMESLIST_LABEL in task.arguments:
            # if standard attributes exist in arguments, use the PuliHelper to decompose accordingly

            start = task.arguments.get(self.START_LABEL, 1)
            end = task.arguments.get(self.END_LABEL, 1)
            packetSize = task.arguments.get(self.PACKETSIZE_LABEL, 1)
            framesList = task.arguments.get(self.FRAMESLIST_LABEL, "")

            self.decompose(start=start, end=end, packetSize=packetSize, callback=self, framesList=framesList)

        else:
            # If arguments given but no standard behaviour, simply transmit task arguments to single command
            self.task.addCommand(task.name, task.arguments)

    def addCommand(self, packetStart, packetEnd):
        '''
        Default method to add a command with DefaultTaskDecomposer.

        :param packetStart: Integer representing the first frame
        :param packetEnd: Integer representing the last frame
        '''
        cmdArgs = self.task.arguments.copy()
        if packetStart is not None:
            cmdArgs[self.START_LABEL] = packetStart
        if packetEnd is not None:
            cmdArgs[self.END_LABEL] = packetEnd

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)

    def decompose(self, start, end, packetSize, callback, framesList=""):
        ''' Method extracted from PuliActionHelper. Default behaviour for decompozing a task.

        :param start: Integer representing the first frame
        :param end: Integer representing the last frame
        :param packetSize: The number of frames to process in each command
        :param callback: A specific callback given to replace default's "addCommand" if necessary
        :param framesList: A string representing a list of frames
        '''
        packetSize = int(packetSize)
        if len(framesList) != 0:
            frames = framesList.split(",")
            for frame in frames:
                if "-" in frame:
                    frameList = frame.split("-")
                    start = int(frameList[0])
                    end = int(frameList[1])

                    length = end - start + 1
                    fullPacketCount, lastPacketCount = divmod(length, packetSize)

                    if length < packetSize:
                        callback.addCommand(start, end)
                    else:
                        for i in range(fullPacketCount):
                            packetStart = start + i * packetSize
                            packetEnd = packetStart + packetSize - 1
                            callback.addCommand(packetStart, packetEnd)
                        if lastPacketCount:
                            packetStart = start + (i + 1) * packetSize
                            callback.addCommand(packetStart, end)
                else:
                    callback.addCommand(int(frame), int(frame))
        else:
            start = int(start)
            end = int(end)

            length = end - start + 1
            fullPacketCount, lastPacketCount = divmod(length, packetSize)

            if length < packetSize:
                callback.addCommand(start, end)
            else:
                for i in range(fullPacketCount):
                    packetStart = start + i * packetSize
                    packetEnd = packetStart + packetSize - 1
                    callback.addCommand(packetStart, packetEnd)
                if lastPacketCount:
                    packetStart = start + (i + 1) * packetSize
                    callback.addCommand(packetStart, end)


def _load(name, motherClass):
    try:
        moduleName, cls = name.rsplit(".", 1)
    except ValueError:
        raise JobTypeImportError("Invalid job type name '%s'. It should be like 'some.module.JobClassName'." % name)

    try:
        module = __import__(moduleName, fromlist=[cls])
    except ImportError, error:
        traceback.print_exc()
        raise JobTypeImportError("No module '%s' on PYTHONPATH:\n%s. (%s)" % (moduleName, "\n".join(sys.path), error))

    try:
        jobtype = getattr(module, cls)
    except AttributeError:
        raise JobTypeImportError("No such job type '%s' defined in module '%s'." % (cls, name))

    if not issubclass(jobtype, motherClass):
        motherClassName = "%s.%s" % (motherClass.__module__, motherClass.__name__)
        raise JobTypeImportError("%s (loaded as '%s') is not a valid %s." % (jobtype, name, motherClassName))

    return jobtype


def loadTaskExpander(name):
    return _load(name, TaskExpander)


def loadTaskDecomposer(name):
    return _load(name, TaskDecomposer)


def loadCommandRunner(name):
    return _load(name, CommandRunner)
