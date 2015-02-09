#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
Request server to retrieve and interact on job,subjobs and commands.
Each request method will return a result list and a summary dict.
Result list holds one or several Job objects.

Use:
(jobs, summary) = QueueHandler.getAllJobs()
print "Summary:"
for job in jobs:
    print "%s: %s" % (job.id, job.name)

jobs = QueueHandler.getJobsByName(['test*'])
jobs = QueueHandler.getJobsById([2,4,50])

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
    """
    """

    @classmethod
    def getAllJobs(cls, recursive=True):
        result = []

        url = "query/job"
        body = {
            "recursive": recursive
        }
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
    def getJobsById(cls, idList, recursive=True):
        result = []

        if idList == []:
            raise InvalidParamError("Error: empty idList given for request")

        url = "query/job"
        body = {
            "id": idList,
            "recursive": recursive
        }
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

    @classmethod
    def getJobsByName(cls, nameList, recursive=True):
        result = []

        if nameList == []:
            raise InvalidParamError("Error: empty nameList given for request")

        url = "query/job"
        body = {
            "name": nameList,
            "recursive": recursive
        }
        try:
            response = Server.post(url, data=json.dumps(body))
            jobs = response.get("items")
            summary = response.get("summary")

            for job in jobs:
                tmp = Job(job)
                result.append(tmp)
        except (RequestTimeoutError, RequestError) as err:
            raise err

        return result, summary


if __name__ == '__main__':
    pass