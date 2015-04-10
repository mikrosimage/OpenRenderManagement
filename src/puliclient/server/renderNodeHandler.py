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
from octopus.core.enums.rendernode import RN_STATUS


class InvalidParamError(Exception):
    """ Raised when api method param is invalid. """


class RenderNodeHandler(object):
    """
    """
    def __init__(self, server=None):
        self.__server = Server() if server is None else server

    def processRequest(self, body=None, url="query/rendernode"):
        """
        :param body:
        :return:
        """
        result = []
        try:
            response = self.__server.post(url, data=json.dumps(body))

            renderNodes = response.get("items")
            summary = response.get("summary")

            for rn in renderNodes:
                result.append(RenderNode(rn))

        except (RequestTimeoutError, RequestError) as err:
            raise err

        return result, summary

    def getAllRenderNodes(self):
        """
        Retrieves all jobs on the server
        :param:
        :return:
        """
        body = {}
        return self.processRequest(body)

    def getRenderNodesByHost(self, hostnameList):
        """
        Retrieves RNs list on server given a list of hostname regular expresssion.
        :param hostList: render node hostname expresssions
        :return: a tuple with list of results and a summary dict
        """
        if hostnameList == [] or hostnameList is None:
            raise InvalidParamError("Error: empty hostnameList given for request")

        body = {
            "host": hostnameList,
        }
        return self.processRequest(body)

    def getRenderNodesById(self, idList):
        """
        Retrieves job list on server given a list of ids.
        :param idList: rn ids
        :return: a tuple with list of results and a summary dict
        """
        if idList == [] or idList is None:
            raise InvalidParamError("Error: empty idList given for request")

        body = {
            "id": idList,
        }
        return self.processRequest(body)

    def getRenderNodesByName(self, nameList):
        """
        Retrieves RNs list on server given a list of name regular expresssion.
        :param nameList: render node name expresssions
        :return: a tuple with list of results and a summary dict
        """
        if nameList == [] or nameList is None:
            raise InvalidParamError("Error: empty nameList given for request")

        body = {
            "name": nameList,
        }
        return self.processRequest(body)

    def getRenderNodesByPool(self, poolList):
        """
        Retrieves RNs on server given a list of exact pool names.
        :param poolList: pool names
        :return: a tuple with list of results and a summary dict
        """
        if poolList == [] or poolList is None:
            raise InvalidParamError("Error: empty idList given for request")

        body = {
            "pool": poolList,
        }
        return self.processRequest(body)

    def getRenderNodesByStatus(self, statusList):
        """
        Retrieves RNs list on server given a list of statuses.
        :param statusList: job status (in range [0-6] cf enum NODE_STATUS)
        :return: a tuple with list of results and a summary dict
        """
        if statusList == [] or statusList is None:
            raise InvalidParamError("Error: empty nameList given for request")

        cleanStatus = []
        for status in statusList:
            try:
                cleanStatus.append(RN_STATUS[int(status)])
            except IndexError as err:
                logging.error("Status index out of range: %s" % status)
                raise err
            except Exception as err:
                raise err

        logging.debug("clean statuses = %s" % cleanStatus)

        body = {
            "status": statusList,
        }
        return self.processRequest(body)

    def getRenderNodesByVersion(self, versionList):
        """
        Retrieves RNs list on server given a list of version exact values.
        :param versionList: render node versions
        :return: a tuple with list of results and a summary dict
        """
        if versionList == [] or versionList is None:
            raise InvalidParamError("Error: empty versionList given for request")

        body = {
            "version": versionList,
        }
        return self.processRequest(body)

    def getRenderNodes(
            self,
            hostList=None,
            idList=None,
            nameList=None,
            poolList=None,
            statusList=None,
            versionList=None
    ):
        """
        Retrieves RNs list on server given several filter
        :param hostList: RN hostname
        :param idList: RN id
        :param nameList: RN name, i.e. "<hostname>:<port>"
        :param poolList: RN pool name
        :param statusList: RN statuses see octopus.core.enums.rendernode.RN_STATUS
        :param versionList: RN puliversion
        :return: a tuple with list of results and a summary dict
        :raises InvalidParamError: if filter params are not valid
        :raises RequestTimeoutError: if the request reaches a timeout
        :raises RequestError: if the request response code is an error or if any other network error occur
        """
        if hostList == []:
            raise InvalidParamError("Error: empty hostList given for request")
        if idList == []:
            raise InvalidParamError("Error: empty idList given for request")
        if nameList == []:
            raise InvalidParamError("Error: empty nameList given for request")
        if poolList == []:
            raise InvalidParamError("Error: empty poolList given for request")
        if statusList == []:
            raise InvalidParamError("Error: empty statusList given for request")
        if versionList == []:
            raise InvalidParamError("Error: empty versionList given for request")

        body = {}

        if hostList is not None: body['host'] = hostList
        if idList is not None: body['id'] = idList
        if nameList is not None: body['name'] = nameList
        if poolList is not None: body['pool'] = poolList
        if statusList is not None: body['status'] = statusList
        if versionList is not None: body['version'] = versionList

        return self.processRequest(body)


if __name__ == '__main__':

    from octopus.core import enums

    rnHandler = RenderNodeHandler(Server("localhost", 8004))
    print rnHandler.getAllRenderNodes()
    print rnHandler.getRenderNodesById(range(1, 100))
    print rnHandler.getRenderNodesByName(['.*vfxpc*'])
    print rnHandler.getRenderNodesByStatus([enums.RN_IDLE, enums.RN_WORKING])
    print rnHandler.getRenderNodesByHost(['vfxpc64'])
    print rnHandler.getRenderNodesByVersion(['1.7.12'])
    print rnHandler.getRenderNodesByPool(None)

    (results, summary) = rnHandler.getRenderNodes(
        idList=[4, 5, 6],
        nameList=["vfxpc.*:8000"],
        hostList=["vfxpc64"],
        versionList=["dev"],
        poolList=["default", "renderfarm"],
        statusList=[enums.RN_IDLE],
    )
    if results:
        for rn in results:
            print "id:%s name:%s hostname:%s version:%s ram:%s/%s" % (rn.id, rn.name, rn.host, rn.puliversion, rn.systemFreeRam, rn.ramSize)
