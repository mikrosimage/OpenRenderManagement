'''
Created on Dec 16, 2009

@author: Arnaud Chassagne
'''
try:
    import simplejson as json
except ImportError:
    import json

from octopus.core.communication.http import Http404, Http400, HttpConflict
from octopus.core.framework import BaseResource, queue
from octopus.dispatcher.model.pool import PoolShare, PoolShareCreationException
import logging

__all__ = []


LOGGER = logging.getLogger('poolshares')

class PoolSharesResource(BaseResource):
    #@queue
    def get(self):
        poolShares = self.getDispatchTree().poolShares.values()
        self.writeCallback({'poolshares': dict(((poolShare.id, poolShare.to_json()) for poolShare in poolShares))})

    #@queue
    def put(self):
        dct = self.getBodyAsJSON()

        if "pslist" in dct:
            pslist = dct['pslist']
            poolShares = []
            for psId in pslist:
                poolShares.append(self.getDispatchTree().poolShares[int(psId)])
            self.writeCallback({'poolshares': dict(((poolShare.node.id, poolShare.to_json()) for poolShare in poolShares))})

    #@queue
    def post(self):
        dct = self.getBodyAsJSON()
        for key in ('poolName', 'nodeId', 'maxRN'):
            if not key in dct:
                return Http400("Missing key %r" % key)
        poolName = str(dct['poolName'])
        nodeId = int(dct['nodeId'])
        maxRN = int(dct['maxRN'])
        # get the pool object
        if not poolName in self.getDispatchTree().pools:
            return HttpConflict("Pool %s is not registered" % poolName)
        pool = self.getDispatchTree().pools[poolName]
        # get the node object
        if not nodeId in self.getDispatchTree().nodes:
            return HttpConflict("No such node %r" % nodeId)
        node = self.getDispatchTree().nodes[nodeId]
        # create the poolShare
        try:
            poolShare = PoolShare(None, pool, node, maxRN)
            self.getDispatchTree().poolShares[poolShare.id] = poolShare
            # return the response
            self.set_header('Location', '/poolshares/%r/' % poolShare.id)
            self.writeCallback(json.dumps(poolShare.to_json()))
        except PoolShareCreationException:
            return HttpConflict("PoolShare of pool for this node already exists, re-assigning...")


class PoolShareResource(BaseResource):
    #@queue
    def get(self, id):
        try:
            poolShare = self.getDispatchTree().poolShares[int(id)]
        except KeyError:
            return Http404("No such poolshare")
        self.writeCallback({
            'poolshare': poolShare.to_json()
        })

    #@queue
    def post(self, id):
        '''
        This request is sent when a user wants to update a poolshare's maxRN.
        We update the poolShare but also report this update on the related node for data consistency (and display).
        '''
        try:
            poolShare = self.getDispatchTree().poolShares[int(id)]
        except KeyError:
            return Http404("No such poolshare")
        dct = self.getBodyAsJSON()
        maxRN = int(dct['maxRN'])
        poolShare.maxRN = maxRN
        poolShare.userDefinedMaxRN = True

        # Mirror this change to the related node of this pool share
        poolShare.node.maxRN = maxRN
        poolShare.userDefinedMaxRN = True if maxRN not in [-1, 0] else False

        self.writeCallback({
            'poolshare': poolShare.to_json()
        })
