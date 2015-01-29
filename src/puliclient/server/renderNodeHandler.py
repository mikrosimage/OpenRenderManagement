#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import logging
try:
    import simplejson as json
except ImportError:
    import json

from puliclient.server.server import Server
from puliclient.server.server import RequestError
from puliclient.server.server import RequestTimeoutError

from puliclient.model.renderNode import RenderNode


class RenderNodeHandler(object):
    """
    """

    # @classmethod
    # def createRN(cls, rnDict):
    #     rn = RenderNode(rnDict.get("name"))
    #
    #     for key, val in rnDict.iteritems():
    #         if hasattr(rn, key):
    #             setattr(rn, key, val)
    #     return rn

    @classmethod
    def getAllRenderNodes(cls):
        """
        """
        result = []
        url = "rendernodes"

        try:
            response = Server.get(url)
            rnList = response.get("rendernodes", None)
            for rn in rnList:
                result.append(RenderNode(rn))
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve rendernode with query: %s" % url)
        return result

    @classmethod
    def getRenderNode(cls, workerName, workerPort=8000):
        """
        """
        rn = None
        url = "rendernodes/%s:%s/" % (workerName, workerPort)

        try:
            rnDict = Server.get(url)
            rn = RenderNode(rnDict)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve rendernode with query: %s" % url)
        return rn

    # @classmethod
    # def getRenderNodes(cls, queryDict):
    #     result = []
    #     response = {}
    #     url = "query2/rn"
    #
    #     try:
    #         response = Server.post(url, data=json.dumps(queryDict))
    #         for rn in response.get('items', []):
    #             result.append(RenderNode(rn))
    #     except (RequestTimeoutError, RequestError):
    #         logging.error("Impossible to retrieve rendernode with query: %s" % url)
    #
    #     return result, response.get('summary')

    @classmethod
    def getRenderNodesById(cls, idList):
        raise NotImplementedError
    @classmethod
    def getRenderNodesByPool(cls, poolList):
        raise NotImplementedError
    @classmethod
    def getRenderNodesByStatus(cls, statusList):
        raise NotImplementedError
    @classmethod
    def getRenderNodesByName(cls, nameList):
        raise NotImplementedError
    @classmethod
    def getRenderNodesByHost(cls, hostnameList):
        raise NotImplementedError
    @classmethod
    def getRenderNodesByVersion(cls, versionList):
        raise NotImplementedError
