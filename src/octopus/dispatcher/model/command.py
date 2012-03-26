####################################################################################################
# @file command.py
# @package 
# @author 
# @date 2008/10/29
# @version 0.1
#
# @mainpage
# 
####################################################################################################

import time

from octopus.core.enums.command import *
from . import models
from octopus.core.enums.rendernode import RN_IDLE

class Command(models.Model):

    description = models.StringField()
    task = models.ModelField()
    arguments = models.DictField()
    status = models.IntegerField()
    message = models.StringField()
    completion = models.FloatField()
    renderNode = models.ModelField(allow_null=True)
    creationTime = models.FloatField()
    startTime = models.FloatField(allow_null=True)
    updateTime = models.FloatField(allow_null=True)
    endTime = models.FloatField(allow_null=True)

    def __init__(self, id, description, task, arguments, status=CMD_READY, completion=None, renderNode=None, creationTime=None, startTime=None, updateTime=None, endTime=None, message=""):
        from octopus.dispatcher.model import Task
        models.Model.__init__(self)
        if task:
            assert isinstance(task, Task)
        if id is None:
            self.id = None
            self.description = str(description)
            self.task = task
            self.arguments = arguments
            self.status = int(status)
            self.completion = 0.
            self.renderNode = None
            self.creationTime = time.time()
            self.startTime = None
            self.updateTime = None
            self.endTime = None
        else:
            # id exists, so we'll assume that status, completion and creationTime exist as well
            assert status != None
            assert completion != None
            assert creationTime != None
            self.id = int(id)
            self.description = str(description)
            self.task = task
            self.arguments = arguments
            self.status = int(status)
            self.completion = float(completion)
            self.renderNode = renderNode
            self.creationTime = creationTime
            self.updateTime = updateTime
            self.startTime = startTime
            self.endTime = endTime
        self.message = str(message)


    def __repr__(self):
        return "Command(id=%r, status=%s)" % (self.id, CMD_STATUS_NAME[self.status])


    def clearAssignment(self):
        self.renderNode = None
        self.startTime = None
        self.updateTime = None
        self.endTime = None
#        self.completion = 0
        self.status = CMD_READY


    def assign(self, renderNode):
        self.renderNode = renderNode
        self.startTime = time.time()
        self.status = CMD_ASSIGNED


    def cancel(self):
        if not isRunningStatus(self.status):
            self.status = CMD_CANCELED
        elif isRunningStatus(self.status):
            self.renderNode.request("DELETE", "/commands/" + str(self.id) + "/")

    def setReadyAndKill(self):
        if self.renderNode is not None:
            self.renderNode.request("DELETE", "/commands/" + str(self.id) + "/")
            self.renderNode.release()
        self.setReadyStatusAndClear()

    def setReadyStatus(self):
        if isRunningStatus(self.status):
            raise RuntimeError, "Cannot reset a running command."
        self.setReadyStatusAndClear()
        
    def setReadyStatusAndClear(self):
        self.status = CMD_READY
        # added by acs
        self.clearAssignment()
        self.completion = 0.0
        self.message = ""

    def finish(self):
        "Called on a finished command, it sets the endTime for this command and releases the resources on its associated render node"
        if not isFinalStatus(self.status):
            raise ValueError("cannot finish a command that is not completed")
        if self.renderNode is None:
            return
        self.renderNode.unassign(self)


    def to_json(self):
        jsonRepr = super(Command, self).to_json()
        if self.renderNode is not None:
            renderNodeName = self.renderNode.name
            jsonRepr['renderNode'] = renderNodeName
        else:
            jsonRepr['renderNode'] = None
        return jsonRepr


class CommandDatesUpdater(object):

    def onCreationEvent(self, obj):
        pass

    def onDestructionEvent(self, obj):
        pass

    def onChangeEvent(self, obj, field, oldvalue, newvalue):
        if field == 'completion':
            self.onCompletionUpdate(obj)
        elif field == 'status':
            self.onStatusUpdate(obj)

    def onCompletionUpdate(self, cmd):
        cmd.updateTime = time.time()

    def onStatusUpdate(self, cmd):
        cmd.updateTime = time.time()
        if isFinalStatus(cmd.status):
            cmd.endTime = cmd.updateTime
        else:
            cmd.endTime = None
        if cmd.status is CMD_ASSIGNED:
            cmd.startTime = cmd.updateTime
        elif cmd.status < CMD_ASSIGNED:
            cmd.startTime = None


Command.changeListeners.append(CommandDatesUpdater())
