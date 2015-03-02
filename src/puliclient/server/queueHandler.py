#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
Request server to retrieve and interact on job, subjobs and commands.
Each request method will return a result list and a summary dict.
Result list holds one or several Job objects.

Use:
qh = QueueHandler()

(jobs, summary) = qh.getAllJobs()
print "Summary:"
for job in jobs:
    print "%s: %s" % (job.id, job.name)

jobs = qh.getJobsByName(['test*'], recursive=False)
jobs = qh.getJobsById([2,4,50])
jobs = qh.getJobsByStatus([2, 3])

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
        :return: a tuple with list of jobs and a summary dict
        """

        body = {
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobs(
            self,
            idList=None,
            nameList=None,
            poolList=None,
            statusList=None,
            userList=None,
            tagsDict=None,
    ):
        """
        Retrieves job list on server given several filter
        :param idList: Job id
        :param nameList: Job name
        :param poolList: Job pool name
        :param statusList: Job statuses see octopus.core.enums.nodes.NODE_STATUS
        :param userList: Job create user
        :param tagsDict: a dict of tags key and possible values

        :return: a tuple with list of results and a summary dict
        :raises InvalidParamError: if filter params are not valid
        :raises RequestTimeoutError: if the request reaches a timeout
        :raises RequestError: if the request response code is an error or if any other network error occur
        """
        if idList == []:
            raise InvalidParamError("Error: empty idList given for request")
        if nameList == []:
            raise InvalidParamError("Error: empty nameList given for request")
        if poolList == []:
            raise InvalidParamError("Error: empty poolList given for request")
        if statusList == []:
            raise InvalidParamError("Error: empty statusList given for request")
        if userList == []:
            raise InvalidParamError("Error: empty userList given for request")
        if tagsDict == []:
            raise InvalidParamError("Error: empty tagsDict given for request")

        body = {}

        if idList is not None: body['id'] = idList
        if nameList is not None: body['name'] = nameList
        if poolList is not None: body['pool'] = poolList
        if statusList is not None: body['status'] = statusList
        if userList is not None: body['user'] = userList
        if tagsDict is not None: body['tags'] = tagsDict

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

        body = {
            "name": nameList,
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByProd(self, prodList, recursive=True):
        """
        Retrieves job list on server given a list of prods.
        :param prodList: job prod, stored under "prod" key in tags dict
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if prodList == []:
            raise InvalidParamError("Error: empty prodList given for request")

        body = {
            "tags": {"prod": prodList},
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByPool(self, poolList, recursive=True):
        """
        Retrieves job list on server given a list of pool names.
        :param poolList: pool name list
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if poolList == []:
            raise InvalidParamError("Error: empty poolList given for request")

        body = {
            "pool": poolList,
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

        body = {
            "status": statusList,
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByTags(self, tagsDict, recursive=True):
        """
        Retrieves job list on server given a list of tags, each tag having one or several possible values.
        Jobs will be selected if any given tag is found.
        :param tagsDict: a tags dict, each key being the tag name and each value being one or several possible values
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if tagsDict == {}:
            raise InvalidParamError("Error: empty tagsDict given for request")

        body = {
            "tags": tagsDict,
            "recursive": recursive
        }
        return self.processRequest(body)

    def getJobsByUser(self, userList, recursive=True):
        """
        Retrieves job list on server given a list of user.
        :param userList: job users
        :param recursive: retrieve a whole hierarchy or only single level top job(s) (default=True)
        :return: a tuple with list of jobs and a summary dict
        """
        if userList == []:
            raise InvalidParamError("Error: empty userList given for request")

        body = {
            "user": userList,
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
    print qh.getJobsByPool(['renderfarm'], False)
    print qh.getJobsByProd(['prod_name'], False)
    print qh.getJobsByTags({'nbFrames': [10], 'prod': ['test']}, False)
    print qh.getJobsByUser(['jsa'], False)

    (results, summary) = qh.getJobs(
        idList=[28, 32],
        nameList=['.*test'],
        poolList=["default", "renderfarm"],
        statusList=[enums.NODE_DONE],
        tagsDict={
            'prod': ['prodA', 'prodB'],
            'nbFrames': [100],
        },
        userList=['user1', 'user2']
    )
    if results:
        for job in results:
            print "id:%s name:%s user:%s" % (job.id, job.name, job.user)
