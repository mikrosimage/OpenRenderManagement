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


class InvalidParamError(Exception):
    """ Raised when api method param is invalid. """


class QueueHandler(object):
    '''
    '''



    # @classmethod
    # def getJob(cls, id):
    #     job = None
    #     url = "/nodes/%d/" % id
    #
    #     try:
    #         jobDict = Server.get(url)
    #         job = cls.createJob(jobDict)
    #     except (RequestTimeoutError, RequestError):
    #         logging.error("Impossible to retrieve rendernode with query: %s" % url)
    #     return job

    @classmethod
    def getAllJobs(cls):
        result = []

        url = "query/job"
        body = {}
        try:
            response = Server.post(url, data=json.dumps(body))
            jobs = response.get("items")
            summary = response.get("summary")

            for job in jobs:
                result.append(Job(job))

        except (RequestTimeoutError, RequestError) as err:
            raise err

        return result, summary


    @classmethod
    def getJobsById(cls, idList):
        result = []

        if idList == []:
            raise InvalidParamError("Error: empty idList given for request")

        url = "query/job"
        body = {"id": idList}
        try:
            response = Server.post(url, data=json.dumps(body))
            jobs = response.get("items")
            summary = response.get("summary")

            for job in jobs:
                tmp = Job(job)
                result.append(tmp)
        except (RequestTimeoutError, RequestError) as err:
            # logging.error("Impossible to retrieve jobs with query: %s" % url)
            raise err

        return result, summary
