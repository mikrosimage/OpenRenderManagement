# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2015, Mikros Image"

import logging
from datetime import datetime

try:
    import simplejson as json
except ImportError:
    import json

from puliclient.model.jsonModel import JsonModel
from puliclient.server.server import Server, RequestError, RequestTimeoutError
from puliclient.server.server import request


# class RenderNode(object, JsonModel):
class RenderNode(object):
    '''
    '''
    #
    # Private
    #
    def __init__(self, rnDict=None):

        # Sys infos
        self.id = 0
        self.name = ""
        self.coresNumber = 0
        self.ramSize = ""
        self.speed = 0

        # Dynamic sys infos
        self.systemFreeRam = 0
        self.systemSwapPercentage = 0

        # Worker state
        self.puliversion = ""
        self.commands = {}
        self.status = 0
        self.host = ""
        self.port = 0
        # self.pools = []
        self.caracteristics = {}
        self.performance = 0.0
        self.excluded = False

        # Timers
        self.createDate = 0
        self.registerDate = 0
        self.lastAliveTime = 0

        if rnDict:
            self._createFromDict(rnDict)
        # workerHistory (list state changes and user actions)
        # commandHistory

    def __repr__(self):
        return "RenderNode(%s)" % self.name

    def __str__(self):
        return "%s" % self.name

    def encode(self, indent=0):
        # import pudb; pu.db
        res = {}
        for field in self.__dict__:
            res[field] = getattr(self, field)
        return res

    def _createFromDict(self, rnDict):
        """
        :param rnDict:
        :return boolean: Indicating success
        """
        for key, val in rnDict.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)

            # Specific transformation
            # self.speed = rnDict.get("createDate")

            # self.createDate = datetime.fromtimestamp(rnDict.get("createDate"))
            # self.registerDate = datetime.fromtimestamp(rnDict.get("registerDate"))
            # self.lastAliveTime = datetime.fromtimestamp(rnDict.get("lastAliveTime"))
        return True

    def _refresh(self):
        raise NotImplementedError
        # url = "/rendernodes/%s:%s/" % (self.host, self.port)
        #
        # try:
        #     rnDict = Server.get(url)
        #     for key, val in rnDict.iteritems():
        #         if hasattr(self, key):
        #             setattr(self, key, val)
        # except (RequestTimeoutError, RequestError):
        #     logging.error("Impossible to refresh rendernode with query: \
        #         %s" % url)

    def _sendPauseCommand(self, content):
        '''
        '''
        url = "/pause/"
        body = json.dumps(content)
        try:
            # No data awaited from request, an exception is raised
            # if pause action could not be executed
            request(self.host, self.port, url, "post", data=body)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to send proper pause action to node %s\
                with content: %s" % (url, content))
            return False
        return True

    #
    # User actions
    #
    def resume(self):
        '''
        Send command to render node to exit from pause
        :return: A boolean indicating if the action has been properly executed
        '''
        return self._sendPauseCommand({'content': "0"})

    def pause(self):
        '''
        | Send command to current RN to kill running command and pause
        | NOTE: status will be effective after a short delay (approx. 50ms)
        :return: A boolean indicating if the action succeeded
        '''
        return self._sendPauseCommand({'content': "-1"})

    def killAndRestart(self):
        '''
        | Send command to kill command on a RN and restart it
        | NOTE: status will be effective after a short delay (approx. 50ms)
        :return: A boolean indicating if the action has been properly executed
        '''
        return self._sendPauseCommand({'content': "-3"})

    def restart(self):
        '''
        | Send command to restart current RN
        | NOTE: status will be effective after delay (approx. 50ms)
        :return: A boolean indicating if the action has been properly executed
        '''
        return self._sendPauseCommand({'content': "-2"})

    def getLog(self):
        '''
        Return a string containing the worker log.
        '''
        raise NotImplementedError

    def tailLog(self, length=100):
        '''
        Return a string containing the tail of the worker log.

        :param length: int indicating the number of lines to retrieve
        '''
        raise NotImplementedError

    def setPerformanceIndex(self):
        raise NotImplementedError
