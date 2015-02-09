# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import logging
import copy


try:
    import simplejson as json
except Exception:
    import json


class IJson():
    """
    Add serialization capability to any object
    """
    def to_JSON(self, indent=2):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=indent
        )

    def loadFrom(self, obj):

        try:
            for key, val in obj.iteritems():
                if hasattr(self, key):
                    setattr(self, key, val)
        except Exception as e:
            logging.getLogger('main').error('Impossible to load data from node %s (%s)' % (obj, e))

    def serializePoolShareList(self, poolShareList):
        res = []
        for poolShare in poolShareList:
            res.append({
                'id': poolShare.id,
                'pool': poolShare.pool.name,
                'allocatedRN': poolShare.allocatedRN,
                'maxRN': poolShare.maxRN,
                'userDefinedMaxRN': poolShare.userDefinedMaxRN,
            })
        return res


class JobInfo(object, IJson):

    def __init__(self, id):
        # Core infos
        self.id = id
        self.name = ""
        self.user = ""
        self.status = 0
        self.creationTime = 0
        self.updateTime = 0
        self.startTime = 0
        self.endTime = 0
        self.tags = {}
        self.commandCount = 0

        # Hierarchy
        self.children = []
        self.dependencies = []

        # Assignment
        self.dispatchKey = 0
        self.maxRN = 0
        self.timer = None
        self.poolShares = []
        self.additionnalPoolShares = []

        # Runtime infos
        self.completion = 0.0
        self.allocatedRN = 0
        self.optimalMaxRN = 0

        self.doneCommandCount = 0
        self.readyCommandCount = 0

        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

    def loadFrom(self, node):
        # Core infos
        self.name = node.name
        self.user = node.user
        self.status = node.status
        self.creationTime = node.creationTime
        self.updateTime = node.updateTime
        self.startTime = node.startTime
        self.endTime = node.endTime
        self.tags = copy.copy(node.tags)
        self.commandCount = node.commandCount

        # Hierarchy
        # self.children = node.children
        self.dependencies = copy.copy(node.dependencies)

        # Assignment
        self.dispatchKey = node.dispatchKey
        self.maxRN = node.maxRN
        self.timer = node.timer
        self.poolShares = self.serializePoolShareList(node.poolShares.values())
        self.additionnalPoolShares = self.serializePoolShareList(node.additionnalPoolShares.values())

        # Runtime infos
        self.completion = node.completion
        self.allocatedRN = node.allocatedRN
        self.optimalMaxRN = node.optimalMaxRN

        self.doneCommandCount = node.doneCommandCount
        self.readyCommandCount = node.readyCommandCount

        self.averageTimeByFrame = node.averageTimeByFrame
        self.minTimeByFrame = node.minTimeByFrame
        self.maxTimeByFrame = node.maxTimeByFrame


class TaskgroupInfo(object, IJson):

    def __init__(self, id):
        # // From node
        # poolShares
        # additionnalPoolShares

        # Core infos
        self.id = id
        self.name = ""
        self.user = ""
        self.status = 0
        self.creationTime = 0
        self.startTime = 0
        self.endTime = 0
        self.updateTime = 0
        self.tags = {}
        self.commandCount = 0
        self.environment = {}

        # Hierarchy
        self.children = []
        self.dependencies = []
        # self.parent
        # self.nodes
        # self.tasks

        # Assignment
        self.dispatchKey = 0
        self.maxRN = 0
        self.timer = 0

        # Runtime infos
        self.completion = 0.0
        self.allocatedRN = 0

        self.readyCommandCount = 0
        self.doneCommandCount = 0

        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

    def loadFrom(self, node):
        # Core infos
        self.id = node.id
        self.name = node.name
        self.user = node.user
        self.status = node.status
        self.creationTime = node.creationTime
        self.startTime = node.startTime
        self.endTime = node.endTime
        self.updateTime = node.updateTime
        self.tags = copy.copy(node.tags)
        self.commandCount = node.commandCount
        self.environment = copy.copy(node.environment)

        # Hierarchy
        self.dependencies = copy.copy(node.dependencies)

        # Assignment
        self.dispatchKey = node.dispatchKey
        self.maxRN = node.maxRN
        self.timer = node.timer

        # Runtime infos
        self.completion = node.completion
        self.allocatedRN = node.allocatedRN

        self.readyCommandCount = node.readyCommandCount
        self.doneCommandCount = node.doneCommandCount

        self.averageTimeByFrame = node.averageTimeByFrame
        self.minTimeByFrame = node.minTimeByFrame
        self.maxTimeByFrame = node.maxTimeByFrame


class TaskInfo(object, IJson):

    def __init__(self, id):
        # // From node
        # poolShares
        # additionnalPoolShares

        # Core infos
        self.id = id
        self.name = ""
        self.user = ""
        self.status = 0
        self.creationTime = 0
        self.startTime = 0
        self.endTime = 0
        self.updateTime = 0
        self.commandCount = 0
        self.tags = {}

        # Hierarchy
        # self.parent = None
        # self.nodes = []
        self.commands = []
        self.dependencies = []

        # Assignment
        self.maxRN = 0
        self.dispatchKey = 0
        self.timer = 0
        self.lic = ""
        self.ramUse = 0
        self.minNbCores = 0
        self.maxNbCores = 0

        # Runtime infos
        self.completion = 0.0
        self.allocatedRN = 0

        self.readyCommandCount = 0
        self.doneCommandCount = 0

        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

        # Execution infos
        self.runner = ""
        self.arguments = {}
        self.requirements = {}
        self.environment = {}
        self.maxAttempt = 1
        self.paused = False

    def loadFrom(self, node):
        self.id = node.id
        self.name = node.name
        self.user = node.user
        self.status = node.status
        self.creationTime = node.creationTime
        self.startTime = node.startTime
        self.endTime = node.endTime
        self.updateTime = node.updateTime
        self.commandCount = node.commandCount
        self.tags = copy.copy(node.tags)

        self.dependencies = copy.copy(node.dependencies)

        # Assignment
        self.maxRN = node.maxRN
        self.dispatchKey = node.dispatchKey
        self.timer = node.timer
        self.lic = node.task.lic
        self.ramUse = node.task.ramUse
        self.minNbCores = node.task.minNbCores
        self.maxNbCores = node.task.maxNbCores

        # Runtime infos
        self.completion = node.completion
        self.allocatedRN = node.allocatedRN
        self.readyCommandCount = node.readyCommandCount
        self.doneCommandCount = node.doneCommandCount

        self.averageTimeByFrame = node.averageTimeByFrame
        self.minTimeByFrame = node.minTimeByFrame
        self.maxTimeByFrame = node.maxTimeByFrame

        # Execution infos
        self.runner = node.task.runner
        self.arguments = node.task.arguments
        self.requirements = copy.copy(node.task.requirements)
        self.environment = copy.copy(node.task.environment)
        self.maxAttempt = node.maxAttempt
        self.paused = node.paused


class CommandInfo(object, IJson):

    def __init__(self, id):

        # Core infos
        self.id = id
        self.description = ""
        self.nbFrames = 0

        # Runtime infos
        self.status = 0
        self.avgTimeByFrame = 0
        self.renderNode = ""

        # Execution infos
        self.arguments = {}
        self.attempt = 0

        # Callback infos
        self.completion = 0.0
        self.message = ""
        self.stats = {}

        self.creationTime = 0
        self.startTime = 0
        self.endTime = 0
        self.updateTime = 0

    def loadFrom(self, node):
        # Core infos
        self.id = node.id
        self.description = node.description
        self.nbFrames = node.nbFrames

        # Runtime infos
        self.status = node.status
        self.avgTimeByFrame = node.avgTimeByFrame
        self.renderNode = node.renderNode.name if node.renderNode is not None else ""

        # Execution infos
        self.arguments = copy.copy(node.arguments)
        self.attempt = node.attempt

        # Callback infos
        self.completion = node.completion
        self.message = node.message
        self.stats = copy.copy(node.stats)

        self.creationTime = node.creationTime
        self.startTime = node.startTime
        self.endTime = node.endTime
        self.updateTime = node.updateTime
