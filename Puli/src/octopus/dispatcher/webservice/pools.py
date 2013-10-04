
try:
    import simplejson as json
except ImportError:
    import json
import logging

from octopus.dispatcher.model.pool import Pool
from octopus.core.communication.http import Http404, Http400, HttpConflict

from octopus.core.framework import BaseResource, queue

__all__ = []

logger = logging.getLogger('dispatcher.webservice.PoolController')


class PoolsResource(BaseResource):
    #@queue
    def get(self):
        pools = self.getDispatchTree().pools.values()
        self.writeCallback({
            'pools': dict(((pool.name, pool.to_json()) for pool in pools))
        })


class PoolResource(BaseResource):
    #@queue
    def get(self, poolName):
        try:
            pool = self.getDispatchTree().pools[poolName]
        except KeyError:
            return Http404('No such pool')
        self.writeCallback({
            'pool': pool.to_json()
        })

    #@queue
    def post(self, poolName):
        if poolName in self.getDispatchTree().pools:
            return HttpConflict("Pool already registered")
        else:
            tmpPool = Pool(None, poolName)
            self.getDispatchTree().pools[poolName] = tmpPool
            if self.request.headers('Host') is not None:
                host = self.request.headers('Host')
            else:
                host = "%s:%d" % self.getServerAddress()
            self.set_header('Location', 'http://%s/pools/%s/' % (host, poolName))
            self.writeCallback(json.dumps(tmpPool.to_json()))

    #@queue
    def delete(self, poolName):
        try:
            # remove reference of the pool from all rendernodes
            for rn in self.getDispatchTree().renderNodes.values():
                if self.getDispatchTree().pools[poolName] in rn.pools:
                    rn.pools.remove(self.getDispatchTree().pools[poolName])
            # try to remove the pool from the dispatch tree
            self.getDispatchTree().pools[poolName].archive()
        except KeyError, e:
            print e
            return Http404('No such pool')


class PoolRenderNodesResource(BaseResource):
    #@queue
    def put(self, poolName):
        dct = self.getBodyAsJSON()
        if poolName not in self.getDispatchTree().pools:
            return HttpConflict("Pool %s is not registered" % poolName)
        pool = self.getDispatchTree().pools[poolName]
        if not 'renderNodes' in dct:
            return Http400("Missing renderNodes list in the request body.")
        rns = dct['renderNodes']
        rnList = []
        for rnName in rns:
            if rnName not in self.getDispatchTree().renderNodes:
                return HttpConflict("RenderNode %s is not registered." % rnName)
            rnList.append(self.getDispatchTree().renderNodes[rnName])
        pool.setRenderNodes(rnList)
        self.set_header('Location', '/pools/%s/' % poolName)
        self.writeCallback(json.dumps(pool.to_json()))
