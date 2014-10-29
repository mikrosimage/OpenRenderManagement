
try:
    import simplejson as json
except ImportError:
    import json
import logging

from octopus.dispatcher.model.pool import Pool
from octopus.core.communication.http import Http404, Http400, HttpConflict

from octopus.core.framework import queue
from octopus.dispatcher.webservice import DispatcherBaseResource

__all__ = []

logger = logging.getLogger('main.dispatcher.webservice.PoolController')


class PoolsResource(DispatcherBaseResource):
    #@queue
    def get(self):
        pools = self.getDispatchTree().pools.values()
        self.writeCallback({
            'pools': dict(((pool.name, pool.to_json()) for pool in pools))
        })


class PoolResource(DispatcherBaseResource):
    #@queue
    def get(self, poolName):
        try:
            pool = self.getDispatchTree().pools[poolName]
        except KeyError:
            raise Http404('No such pool')
        self.writeCallback({
            'pool': pool.to_json()
        })

    #@queue
    def post(self, poolName):
        if poolName in self.getDispatchTree().pools:
            raise HttpConflict("Pool already registered")
        else:
            tmpPool = Pool(None, poolName)
            self.getDispatchTree().pools[poolName] = tmpPool

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
            logger.warning("No such pool: %r" % e)
            raise Http404('No such pool')


class PoolRenderNodesResource(DispatcherBaseResource):
    #@queue
    def put(self, poolName):
        dct = self.getBodyAsJSON()
        if poolName not in self.getDispatchTree().pools:
            return HttpConflict("Pool %s is not registered" % poolName)
        pool = self.getDispatchTree().pools[poolName]
        if not 'renderNodes' in dct:
            raise Http400("Missing renderNodes list in the request body.")
        rns = dct['renderNodes']
        rnList = []
        for rnName in rns:
            if rnName not in self.getDispatchTree().renderNodes:
                return HttpConflict("RenderNode %s is not registered." % rnName)
            rnList.append(self.getDispatchTree().renderNodes[rnName])
        pool.setRenderNodes(rnList)
        self.set_header('Location', '/pools/%s/' % poolName)
        self.writeCallback(json.dumps(pool.to_json()))
