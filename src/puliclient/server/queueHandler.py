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
    def getJob(cls, id):
        job = None
        url = "/nodes/%d/" % id

        try:
            jobDict = Server.get(url)
            job = cls.createJob(jobDict)
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve rendernode with query: %s" % url)
        return job

    @classmethod
    def getTest(cls):
        result = []
        url = "query/job?id=2"

        try:
            response = Server.post(url)
            jobs = response.get("items")

            for job in jobs:
                # print job
                result.append(Job(job))
        except (RequestTimeoutError, RequestError):
            logging.error("Impossible to retrieve jobs with query: %s" % url)

        return result, response.get("summary")

    @classmethod
    def getJobList(cls, idList):

        jobList = []
        for id in idList:
            tmp = cls.getJob(id)
            if tmp:
                jobList.append(tmp)

        return jobList
