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

from puliclient.model.renderNode import RenderNode


class Command(object):

    def __init__(self, cmdDict=None):

        self.id = 0
        self.description = ""
        self.status = 0
        self.completion = 0.0
        self.attempt = 0
        self.message = ""
        self.renderNode = None
        self.nbFrames = 0
        self.arguments = {}
        self.startTime = 0
        self.endTime = 0
        self.updateTime = 0
        self.avgTimeByFrame = 0.0
        self.stats = {}

        if cmdDict:
            self._createFromDict(cmdDict)

    def __repr__(self):
        return "Command(%s)" % self.id

    def __str__(self):
        return "Command: %d" % (self.id)

    def encode(self, indent=0):
        res = {}
        for field in self.__dict__:
            if field == 'renderNode':
                if self.renderNode:
                    res['renderNode'] = self.renderNode.encode()
            else:
                res[field] = getattr(self, field)
        return res

    def _createFromDict(self, dict):

        for key, val in dict.iteritems():
            if key == 'renderNode':
                self.renderNode.append(RenderNode(val))
            elif hasattr(self, key):
                setattr(self, key, val)

    def createFromCommandNode(self, command):
        # Core infos
        self.id = command.id
        self.description = command.description
        self.status = command.status
        self.completion = command.completion
        self.attempt = command.attempt
        self.message = command.message
        # self.renderNode = command.renderNode
        self.nbFrames = command.nbFrames
        self.arguments = command.arguments.copy()
        self.startTime = command.startTime
        self.endTime = command.endTime
        self.updateTime = command.updateTime
        self.avgTimeByFrame = command.avgTimeByFrame
        self.stats = command.stats.copy()

