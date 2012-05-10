from .models import (Model, StringField, ModelField, DictField, IntegerField, FloatField,
                     ModelListField, ModelDictField)
from .enums import NODE_BLOCKED, NODE_CANCELED, NODE_DONE, NODE_ERROR, NODE_PAUSED, NODE_READY, NODE_RUNNING, NODE_READY
from collections import defaultdict

class TaskGroup(Model):

    name = StringField()
    parent = ModelField(allow_null=True)
    user = StringField()
    arguments = DictField()
    environment = DictField()
    maxRN = IntegerField()
    priority = IntegerField()
    dispatchKey = FloatField()
    nodes = ModelDictField()
    tasks = ModelListField()
    tags = DictField()
    status = IntegerField()
    completion = FloatField()
    creationTime = FloatField()
    startTime = FloatField(allow_null=True)
    updateTime = FloatField(allow_null=True)
    endTime = FloatField(allow_null=True)

    def __init__(self, id, name, parent, user, arguments, environment, requirements,
                 maxRN, priority, dispatchKey, strategy, nodes={}, tags={}):
        Model.__init__(self)
        self.id = int(id) if id else None
        self.name = str(name)
        self.parent = parent
        self.user = str(user)
        self.arguments = arguments
        self.environment = environment
        self.requirements = requirements
        self.maxRN = int(maxRN)
        self.priority = int(priority)
        self.dispatchKey = float(dispatchKey)
        self.strategy = strategy
        self.nodes = nodes.copy()
        self.tasks = []
        self.tags = tags.copy()
        self.completion = 0
        self.status = 0
        self._modified = True
        self.creationTime = None
        self.startTime = None
        self.updateTime = None
        self.endTime = None


    def addTask(self, task):
        assert isinstance(task, Task) or isinstance(task, TaskGroup)
        self.tasks.append(task)


    def removeTask(self, task):
        self.tasks.remove(task)


    def archive(self):
        self.fireDestructionEvent(self)


    def updateStatusAndCompletion(self):
        if  not self.tasks:
            self.completion = 1.0
            self.status = NODE_DONE
        else:
            completion = 0.0
            status = defaultdict(int)
            for child in self.tasks:
                if isinstance(child, TaskGroup):
                    child.updateStatusAndCompletion()
                completion += child.completion
                status[child.status] += 1
            self.completion = completion / len(self.tasks)

            if NODE_PAUSED in status:
                self.status = NODE_PAUSED
            elif NODE_RUNNING in status:
                self.status = NODE_RUNNING
            elif NODE_ERROR in  status:
                self.status = NODE_ERROR
            elif NODE_CANCELED in status:
                self.status = NODE_CANCELED
            elif NODE_READY in status or self.completion != 1.0:
                self.status = NODE_READY
            elif NODE_BLOCKED  in status:
                self.status = NODE_BLOCKED
            else:
                self.status = NODE_DONE


    def __iter__(self):
        return iter(self.tasks)


class Task(Model):
    changeEventSourceFields = ['name', 'parent', 'priority', 'dispatchKey']
    name = StringField()
    parent = ModelField(allow_null=True)
    user = StringField()
    maxRN = IntegerField()
    priority = IntegerField()
    dispatchKey = FloatField()
    runner = StringField()
    arguments = DictField()
    validationExpression = StringField()
    commands = ModelListField()
    requirements = DictField()
    environment = DictField()
    minNbCores = IntegerField()
    maxNbCores = IntegerField()
    ramUse = IntegerField()
    nodes = ModelDictField()
    tags = DictField()
    status = IntegerField()
    completion = FloatField()
    creationTime = FloatField(allow_null=True)
    startTime = FloatField(allow_null=True)
    updateTime = FloatField(allow_null=True)
    endTime = FloatField(allow_null=True)
    lic = StringField()

    def __init__(self, id, name, parent, user, maxRN, priority, dispatchKey, runner, arguments, validationExpression, commands, requirements=[], minNbCores=1, maxNbCores=0, ramUse=0, environment={}, nodes={}, lic="", tags={}):
        assert parent is None or isinstance(parent, TaskGroup)
        Model.__init__(self)
        self.id = int(id) if id else None
        self.name = str(name)
        self.parent = parent
        self.user = user
        self.maxRN = int(maxRN)
        self.priority = int(priority)
        self.dispatchKey = int(dispatchKey)
        self.runner = str(runner)
        self.arguments = arguments
        self.validationExpression = str(validationExpression)
        self.commands = commands
        self.requirements = requirements
        self.environment = environment
        self.minNbCores = int(minNbCores)
        self.maxNbCores = int(maxNbCores)
        self.ramUse = int(ramUse)
        self.nodes = nodes.copy()
        self.lic = lic
        self.tags = tags.copy()
        self.completion = 0
        self.status = 0
        self.creationTime = None
        self.startTime = None
        self.updateTime = None
        self.endTime = None


    def addValidationExpression(self, validationExpression):
        self.validationExpression = "&".join(self.validationExpression,
                                             validationExpression)


    def archive(self):
        self.fireDestructionEvent(self)


    def repr(self):
        return "Task(%r, %r, %r, %r, %r, %r, %r, %r, %r, %r)" % (self.id,
                                                             self.name,
                                                             self.priority,
                                                             self.dispatchKey,
                                                             self.runner,
                                                             self.arguments,
                                                             self.validationExpression,
                                                             self.commands,
                                                             self.requirements,
                                                             self.lic)

class TaskListener(object):

    def __init__(self):
        self.created = False
        self.modified = False

    def onCreationEvent(self, *args):
        pass

    def onDestructionEvent(self, *args):
        pass

    def onChangeEvent(self, obj, name, oldvalue, newvalue):
        if obj.parent is not None:
            obj.parent.modified = True
