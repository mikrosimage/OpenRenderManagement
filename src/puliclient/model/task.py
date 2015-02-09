#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2015, Mikros Image"

try:
    import simplejson as json
except ImportError:
    import json

from puliclient.model.command import Command

class Task(object):

    def __init__(self, taskDict=None):

        # Core infos
        self.id = 0
        self.name = ""
        self.user = ""
        self.status = 0
        self.creationTime = 0
        self.startTime = 0
        self.endTime = 0
        self.updateTime = 0
        self.tags = {}

        # Hierarchy
        # self.parent = None
        # self.nodes = []
        self.commands = []
        # self.dependencies = []

        # Assignment
        # self.maxRN = 0
        # self.dispatchKey = 0
        # self.timer = 0
        self.lic = ""
        self.ramUse = 0
        # self.minNbCores = 0
        # self.maxNbCores = 0

        # Runtime infos
        self.completion = 0.0
        # self.allocatedRN = 0

        # self.readyCommandCount = 0
        # self.doneCommandCount = 0

        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

        # Execution infos
        self.runner = ""
        self.arguments = {}
        # self.requirements = {}
        self.environment = {}
        self.maxAttempt = 1
        self.paused = False

        if taskDict:
            self._createFromDict(taskDict)

    def __repr__(self):
        return "Task(%s)" % self.name

    def __str__(self):
        return "Task: %d - %s" % (self.id, self.name)

    def encode(self, indent=0):
        res = {}
        for field in self.__dict__:
            if field == 'commands':
                res['commands'] = []
                for cmd in self.commands:
                    res['commands'].append(cmd.encode())
            else:
                res[field] = getattr(self, field)
        return res

    def _createFromDict(self, dict):
        for key, val in dict.iteritems():
            if key == 'commands':
                for cmd in dict['commands']:
                    self.commands.append(Command(cmd))
            elif hasattr(self, key):
                setattr(self, key, val)

    def createFromTaskNode(self, task):
        # Core infos
        self.id = task.id
        self.name = task.name
        self.user = task.user
        self.status = task.status
        self.creationTime = task.creationTime
        self.startTime = task.startTime
        self.endTime = task.endTime
        self.updateTime = task.updateTime
        self.tags = task.tags.copy()

        # # Hierarchy
        for cmd in task.commands:
            newCmd = Command()
            newCmd.createFromCommandNode(cmd)
            self.commands.append(newCmd)

        # self.dependencies =

        # Assignment
        self.lic = task.lic
        self.ramUse = task.ramUse

        # Runtime infos
        self.completion = task.completion
        # self.averageTimeByFrame = task.averageTimeByFrame
        # self.minTimeByFrame = task.minTimeByFrame
        # self.maxTimeByFrame = task.maxTimeByFrame

        # Execution infos
        self.runner = task.runner
        self.arguments = task.arguments.copy()
        self.environment = task.environment.copy()
        self.maxAttempt = task.maxAttempt
        # self.paused = task.paused
