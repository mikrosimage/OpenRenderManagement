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
except Exception:
    import json

from puliclient.server.server import Server
from puliclient.server.server import RequestError
from puliclient.server.server import RequestTimeoutError

from puliclient.model.renderNode import RenderNode


class RenderNodeHandler(object):
    '''
    '''

    @classmethod
    def createRN(cls, rnDict):
        rn = RenderNode(rnDict.get("name"))

        for key, val in rnDict.iteritems():
            if hasattr(rn, key):
                setattr(rn, key, val)
        return rn

    @classmethod
    def getRenderNode(cls, workerName, workerPort=8000):
        '''
        '''
        rn = None
        url = "/rendernodes/%s:%s/" % (workerName, workerPort)

        try:
            rnDict = Server.get(url)
            rn = cls.createRN(rnDict)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve rendernode with query: %s"
                          % url)
        return rn
