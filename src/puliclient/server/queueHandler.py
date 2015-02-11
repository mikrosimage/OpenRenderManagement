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
from octopus.core.enums.node import NODE_STATUS

class InvalidParamError(Exception):
    """ Raised when api method param is invalid. """


class QueueHandler(object):
    """
    """

    def __init__(self, server=None):
        self.__server = Server() if server is None else server

    def processRequest(self, body=None, url="query/job"):
        """
        :param body:
        :return:
        """
        result = []
        try:
            response = self.__server.post(url, data=json.dumps(body))

            jobs = response.get("items")
            summary = response.get("summary")

            for job in jobs:
                result.append(Job(job))

        except (RequestTimeoutError, RequestError) as err:
            raise err

        return result, summary

    def getAllJobs(self, recursive=True):
        """
        Retrieves all jobs on the server
        :param recursive:
        :return:
        """

        body = {
            "recursive": recursive
        }
        return self.processRequest(body)


    def getJobsById(self, idList, recursive=True):
        """
        Retrieves job list on server given a list of ids.
        :param idList: job ids
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if idList == []:
            raise InvalidParamError("Error: empty idList given for request")

        url = "query/job"
        body = {
            "id": idList,
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByName(self, nameList, recursive=True):
        """
        Retrieves job list on server given a list of name regular expresssion.
        :param nameList: job name expresssions
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if nameList == []:
            raise InvalidParamError("Error: empty nameList given for request")

        url = "query/job"
        body = {
            "name": nameList,
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByStatus(self, statusList, recursive=True):
        """
        Retrieves job list on server given a list of job statuses.
        :param statusList: job status (in range [0-6] cf enum NODE_STATUS)
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if statusList == []:
            raise InvalidParamError("Error: empty nameList given for request")

        cleanStatus = []
        for status in statusList:
            try:
                cleanStatus.append(NODE_STATUS[int(status)])
            except IndexError as err:
                logging.error("Status index out of range: %s" % status)
                raise err
            except Exception as err:
                raise err

        logging.debug("clean statuses = %s" % cleanStatus)

        url = "query/job"
        body = {
            "status": statusList,
            "recursive": recursive
        }
        return self.processRequest(body)


if __name__ == '__main__':

    from octopus.core import enums

    qh = QueueHandler(Server("localhost", 8004))
    print qh.getAllJobs()
    print qh.getJobsByStatus([enums.NODE_CANCELED, enums.NODE_RUNNING])
    print qh.getJobsById(range(1, 100))
    print qh.getJobsByName(['.*test'], False)
