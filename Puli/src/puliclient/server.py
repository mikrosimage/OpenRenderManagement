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

import requests


class RequestTimeoutError(Exception):
    ''' Raised when helper execution is too long. '''


class RequestError(Exception):
    ''''''


class IJson():
    """
    Add serialization capability to any object
    """
    def to_JSON(self, indent=0):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True,
                          indent=indent)


def request(host, port, url, method="get", *args, **kwargs):
    '''
    | General wrapper around the "Request" methods
    | Used by Server object when sending request to the main server, can also
    | be used by any worker/specific requests.

    :param host: hostname to reach
    :param port: port to use
    :param url: end part of the url to reach
    :param method: a string indicating wich method to use [get,put,post,delete]

    :return: a json or text data depending of the webservice response
    :raise RequestError: for any error that occured related to the network
    :raise RequestTimeoutError: when a request timeout occur
    '''
    try:
        baseUrl = "http://%s:%d" % (host, port)
        url = baseUrl+url

        if method == "get":
            r = requests.get(url, *args, **kwargs)
        elif method == "post":
            r = requests.post(url, *args, **kwargs)
        elif method == "put":
            r = requests.put(url, *args, **kwargs)
        elif method == "delete":
            r = requests.delete(url, *args, **kwargs)
        else:
            logging.error("Unkown HTTP method called: %s" % method)
            raise RequestError

        if r.status_code in [requests.codes.ok,
                             requests.codes.created,
                             requests.codes.accepted]:
            #
            # Request returned successfully
            #
            try:
                result = r.json()
            except ValueError, e:
                result = r.text
            return result

        elif r.status_code in [requests.codes.bad,
                               requests.codes.unauthorized,
                               requests.codes.forbidden,
                               requests.codes.not_found,
                               requests.codes.not_allowed,
                               requests.codes.not_acceptable,
                               requests.codes.internal_server_error,
                               requests.codes.not_implemented,
                               requests.codes.unavailable,
                               requests.codes.conflict]:
            try:
                msg = r.text
            except:
                msg = ""

            logging.error("Error return code: %s, response message: '%s'" % (
                r.status_code, msg))
            raise RequestError(msg)
        else:
            raise RequestError

    except requests.exceptions.Timeout:
        logging.error("Timeout: %s" % e)
        raise RequestTimeoutError

    except requests.exceptions.ConnectionError, e:
        logging.error("Network problem occured: %s" % e.args[0].reason)
        raise RequestError

    except requests.exceptions.RequestException, e:
        logging.error("Unhandled request exception: %s" % e)
        raise RequestError

    except RequestError, e:
        raise

    except Exception, e:
        logging.error("Unhandled exception: %s" % e)
        raise


class Server(object):
    __host = "vfxpc64"
    __port = 8004

    __baseUrl = "http://%s:%d" % (__host, __port)
    __query = ""

    @classmethod
    def getBaseUrl(cls):
        return cls.__baseUrl

    @classmethod
    def request(cls, url, method, *args, **kwargs):
        return request(cls.__host, cls.__port, url, method, *args, **kwargs)

    @classmethod
    def get(cls, url, *args, **kwargs):
        return cls.request(url, "get", *args, **kwargs)

    @classmethod
    def post(cls, url, *args, **kwargs):
        return cls.request(url, "post", *args, **kwargs)

    @classmethod
    def put(cls, url, *args, **kwargs):
        return cls.request(url, "put", *args, **kwargs)

    @classmethod
    def delete(cls, url, *args, **kwargs):
        return cls.request(url, "delete", *args, **kwargs)


class Job(object, IJson):
    #
    # Private
    #
    def __init__(self, id):
        # Core infos
        self.id = id
        self.name = ""
        self.user = ""
        #self.parent = None
        self.status = 0
        self.creationTime = 0
        self.updateTime = 0
        self.startTime = 0
        self.endTime = 0

        # Additionnal infos
        self.tags = {}
        self.commandCount = 0
        self.doneCommandCount = 0
        self.readyCommandCount = 0

        # Assignment
        self.dispatchKey = 0
        self.maxRN = 0
        self.timer = None

        # Progress and stats
        self.completion = 0.0
        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

        self.dependencies = []

        # Internal infos
        self.task = None
        self.poolShares = []
        self.additionnalPoolShares = []
        #updateTime = models.FloatField(allow_null=True)

    def __repr__(self):
        return "Job(%s)" % self.name

    def __str__(self):
        return "Job: %d - %s" % (self.id, self.name)

    def _refresh(self):
        url = "/nodes/%d/" % (self.id)

        try:
            dataDict = Server.get(url)
            for key, val in dataDict.iteritems():
                if hasattr(self, key):
                    setattr(self, key, val)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to refresh job with query: %s" % url)

    def setDispatchKey(self, prio):
        '''
        | Updates dispatchKey (i.e. prio) of a particular node to the server
        | Internal data is updated on succeed to reflect server change
        :param prio: Integer
        :return: A boolean indicating success or failure
        '''
        url = "/nodes/%d/dispatchKey/" % self.id
        body = json.dumps({'dispatchKey': prio})
        try:
            Server.put(url, data=body)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to update prio with url %s and content: \
                %s" % (url, body))
            return False

        # Update internal value (or refresh)
        self.dispatchKey = prio
        return True

    def setPool(self, pool):
        '''
        | Updates pool name of a particular node
        | Internal data is updated on succeed to reflect server change
        :param pool: String representing a pool name
        :return: A boolean indicating success or failure
        '''
        url = "/poolshares/"
        body = json.dumps({'poolName': pool, 'nodeId': self.id, 'maxRN': -1})
        try:
            Server.post(url, data=body)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to update data with url %s and content: \
                %s" % (url, body))
            return False
        return True

    def setMaxRn(self, maxRn):
        '''
        | Updates maxRn of a particular node i.e. the number of RN to affect
        | to this node. Internal data is updated on succeed to reflect server
        | change.
        :param maxRn: Integer
        :return: A boolean indicating success or failure
        '''
        url = "/nodes/%d/maxRN/" % self.id
        body = json.dumps({'maxRN': maxRn})
        try:
            Server.put(url, data=body)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to update data with url %s and content: \
                %s" % (url, body))
            return False

        # Update internal value (or refresh)
        self.maxRN = maxRn
        return True

    def setProd(self, prod):
        pass

    def setShot(self, shot):
        pass

    def setTags(self, tags):
        pass

    def updateTags(self, tags):
        pass


class RenderNode(object, IJson):
    '''
    '''
    #
    # Private
    #
    def __init__(self, name):

        # Sys infos
        self.name = name
        self.coresNumber = 0
        self.ramSize = ""

        # Dynamic sys infos
        self.freeCoresNumber = 0
        self.freeRam = 0
        self.systemFreeRam = 0
        self.systemSwapPercentage = 0

        # Worker state
        self.puliversion = ""
        self.commands = {}
        self.status = 0
        self.host = ""
        self.port = 0
        self.pools = []
        self.caracteristics = {}
        self.performance = 0.0
        self.excluded = False

        # Timers
        self.createDate = 0
        self.registerDate = 0
        self.lastAliveTime = 0

    def __repr__(self):
        return "RenderNode(%s)" % self.name

    def __str__(self):
        return "%s" % self.name

    def _refresh(self):
        url = "/rendernodes/%s:%s/" % (self.host, self.port)

        try:
            rnDict = Server.get(url)
            for key, val in rnDict.iteritems():
                if hasattr(self, key):
                    setattr(self, key, val)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to refresh rendernode with query: \
                %s" % url)

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


class QueueHandler(object):
    '''
    '''

    @classmethod
    def createJob(cls, jobDict):

        jobId = jobDict.get("id")
        if jobId:
            job = Job(jobId)
        else:
            logging.error("Invalid data: Job ID could not be retrieved")
            raise RequestError

        for key, val in jobDict.iteritems():
            if hasattr(job, key):
                setattr(job, key, val)

        return job

    @classmethod
    def getJob(cls, id):
        job = None
        url = "/nodes/%d/" % id

        try:
            jobDict = Server.get(url)
            job = cls.createJob(jobDict)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve rendernode with query: %s"
                          % url)
        return job

    @classmethod
    def getJobs(cls, idList):

        jobList = []
        for id in idList:
            tmp = cls.getJob(id)
            if tmp:
                jobList.append(tmp)

        return jobList


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
