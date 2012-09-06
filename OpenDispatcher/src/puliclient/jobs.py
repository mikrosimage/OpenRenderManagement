'''
Created on Jan 11, 2010

@author: Olivier Derpierre
'''

import sys


class CommandError(Exception):
    '''Raised to signal failure of a CommandRunner execution.'''


class ValidationError(CommandError):
    '''Raised on a validation error'''


class CommandRunnerParameter(object):
    '''Base class for formal command runner parameter.'''

    name = None

    def __init__(self, **kwargs):
        if 'default' in kwargs:
            self.hasDefault = True
            self.defaultValue = kwargs['default']
        else:
            self.hasDefault = False
            self.defaultValue = None

    def validate(self, arguments):
        if not self.name in arguments and self.hasDefault:
            arguments[self.name] = self.defaultValue


class StringParameter(CommandRunnerParameter):
    '''A command runner parameter class that converts the argument value to a string.'''

    def validate(self, arguments):
        super(StringParameter, self).validate(arguments)
        if arguments[self.name]:
            arguments[self.name] = str(arguments[self.name])


class StringListParameter(CommandRunnerParameter):

    def validate(self, arguments):
        super(StringListParameter, self).validate(arguments)
        arguments[self.name] = [str(v) for v in arguments[self.name]]


class BooleanParameter(CommandRunnerParameter):

    def validate(self, arguments):
        super(BooleanParameter, self).validate(arguments)
        arguments[self.name] = bool(arguments[self.name])


class IntegerParameter(CommandRunnerParameter):
    '''A command runner parameter class that converts the argument value to an integer value.'''

    def validate(self, arguments):
        super(IntegerParameter, self).validate(arguments)
        if arguments[self.name]:
            arguments[self.name] = int(arguments[self.name])


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
        for parameter in self.parameters:
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
        self.addCommand(task.name, {})


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
    except ImportError:
        raise JobTypeImportError("No module '%s' on PYTHONPATH:\n%s." % (moduleName, "\n".join(sys.path)))

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

