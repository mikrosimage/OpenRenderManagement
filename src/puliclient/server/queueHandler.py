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

from puliclient.model.job import Job


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
    def getJobList(cls, idList):

        jobList = []
        for id in idList:
            tmp = cls.getJob(id)
            if tmp:
                jobList.append(tmp)

        return jobList
