'''
Created on Jan 11, 2010

@author: Olivier Derpierre
'''

import sys
import traceback
import logging

class CommandError(Exception):
    '''Raised to signal failure of a CommandRunner execution.'''


class ValidationError(CommandError):
    '''Raised on a validation error'''

class RangeError(CommandError):
    '''Raised on a validation error where a value given is out of authorized range'''


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
            if arguments[self.name]:
                # try:
                arguments[self.name] = str(arguments[self.name])
                # except Exception, e:
                #     print "Error when parameter conversion %s: %r" % (self.name, e)
                #     raise ValidationError("StringParameter cannot be converted to str")
        except Exception, e:
            raise e

    # def __repr__(self):
    #     return "StringParameter(name=%r, default=%r, mandatory=%r)" % (self.name, self.defaultValue, self.isMandatory)

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
                                        self.minValue) )

                if self.maxValue is not None and self.maxValue < newVal:
                    raise RangeError("Argument \"%s\"=%d is more than maximum: %d" % (
                                        self.name, 
                                        self.maxValue,
                                        newVal ) )

                arguments[self.name] = newVal
        except Exception, e:
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
                                        self.minValue) )

                if self.maxValue is not None and self.maxValue < newVal:
                    raise RangeError("Argument \"%s\"=%d is more than maximum: %d" % (
                                        self.name, 
                                        self.maxValue,
                                        newVal ) )

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


class CommandRunner(object):

    __metaclass__ = CommandRunnerMetaclass

    scriptTimeOut = None
    parameters = []


    def execute(self, arguments, updateCompletion, updateMessage):
        raise NotImplementedError


    def validate(self, arguments):
        # print "validating against: %r" % self.parameters
        if len(self.parameters) > 0:
            logging.getLogger().info("Validating %d parameter(s):" % len(self.parameters))

        for parameter in self.parameters:
            logging.getLogger().info("  - %s" % parameter)
            parameter.validate(arguments)




class TaskExpander(object):

    def __init__(self, taskGroup):
        pass


class TaskDecomposer(object):

    def __init__(self, task):
        self.task = task

    def addCommand(self, name, args):
        self.task.addCommand(name, args)


class DefaultTaskDecomposer(TaskDecomposer):

    def __init__(self, task):
        super(DefaultTaskDecomposer, self).__init__(task)
        # self.addCommand(task.name, {})

        print "No decomposer given for a task \"%s\", using DefaultTaskDecomposer to create default command." % task.name
        if hasattr(task, 'arguments') and task.arguments is not None:
            # If exists we retrieve task's arguments to use them on the command
            cmdArgs = task.arguments.copy()
            if 'start' in cmdArgs and 'end' in cmdArgs:
                cmdName = "%s_%s_%s" % ( task.name, str(cmdArgs['start']), str(cmdArgs['end']) )
                self.addCommand(cmdName, cmdArgs)
            else:
                self.addCommand(task.name+"_1_1", cmdArgs)
        else:
            # Create an empty command anyway --> probably unecessary
            print "WARNING: No arguments given for the task \"%s\", it is necessary to do this ? (we are creating an empty command anyway..." % task.name
            self.addCommand(task.name+"_1_1", {})

class JobTypeImportError(ImportError):
    """Raised when an error occurs while loading a job type through the load function."""
    pass


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

