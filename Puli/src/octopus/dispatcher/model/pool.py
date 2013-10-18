####################################################################################################
# @file pool.py
# @package
# @author
# @date 2008/10/29
# @version 0.1
#
# @mainpage
#
####################################################################################################

from weakref import WeakKeyDictionary

from . import models


class PoolShareCreationException(Exception):
    '''Raised on a poolshare submission error.'''


## A portion of a pool bound to a dispatchTree node
#
class PoolShare(models.Model):

    pool = models.ModelField(False, 'name')
    node = models.ModelField()
    allocatedRN = models.IntegerField()
    maxRN = models.IntegerField()
    userDefinedMaxRN = models.BooleanField()

    # Use PoolShare.UNBOUND as maxRN value to allow full pool usage
    UNBOUND = -1

    ## Constructs a new pool share.
    #
    # @param id the pool share unique identifier. Use None for auto-allocation by the DispatchTree.
    # @param pool the pool from which to draw render nodes
    # @param node the node where the poolshare is affected
    # @param maxRN the max number of render nodes this share is allowed to draw from the pool. Use PoolShare.UNBOUND for unlimited access to the pool.
    #
    def __init__(self, id, pool, node, maxRN):
        self.id = int(id) if id else None
        self.pool = pool
        self.node = node
        self.allocatedRN = 0
        self.maxRN = int(maxRN)
        # check if we already have a poolShare with this pool and node
        if node in pool.poolShares:
            # reassign to the node if it already exists
            self.node.poolShares = WeakKeyDictionary()
            self.node.poolShares[self.pool] = self.pool.poolShares[self.node]
            raise PoolShareCreationException("PoolShare on node %s already exists for pool %s", node.name, pool.name)
        # registration
        self.pool.poolShares[self.node] = self
        # remove any previous poolshare on this node
        self.node.poolShares = WeakKeyDictionary()
        self.node.poolShares[self.pool] = self
        # the default maxRN at the creation is -1, if it is a different value, it means it's user defined
        if self.maxRN != -1:
            self.userDefinedMaxRN = True
        else:
            self.userDefinedMaxRN = False

    def hasRenderNodesAvailable(self):
        if self.maxRN > 0 and self.allocatedRN >= self.maxRN:
            return False
        return any((rn.isAvailable() for rn in self.pool.renderNodes))

    def __repr__(self):
        return "PoolShare(id=%r, pool.name=%r, node=%r, maxRN=%r, allocatedRN=%r)" % (self.id, self.pool.name if self.pool else None, self.node.name, self.maxRN, self.allocatedRN)


## This class represents a Pool.
#
class Pool(models.Model):

    name = models.StringField()
    renderNodes = models.ModelListField(indexField='name')
    poolShares = models.ModelDictField()

    ## Constructs a new Pool.
    # @param parent the pool's parent
    # @param name the pool's name
    #
    def __init__(self, id, name):
        self.id = int(id) if id else None
        self.name = name if name else ""
        self.renderNodes = []
        self.poolShares = WeakKeyDictionary()

    def archive(self):
        self.fireDestructionEvent(self)

    ## Adds a render node to the pool.
    # @param rendernode the rendernode to add
    #
    def addRenderNode(self, rendernode):
        if self not in rendernode.pools:
            rendernode.pools.append(self)
        if rendernode not in self.renderNodes:
            self.renderNodes.append(rendernode)
        self.fireChangeEvent(self, "renderNodes", [], self.renderNodes)

    ## Removes a render node from the pool.
    # @param rendernode the  rendernode to remove
    #
    def removeRenderNode(self, rendernode):
        if self in rendernode.pools:
            rendernode.pools.remove(self)
        if rendernode in self.renderNodes:
            self.renderNodes.remove(rendernode)
        self.fireChangeEvent(self, "renderNodes", [], self.renderNodes)

    ## Sets the rendernodes associated to this pool to the given list of rendernodes
    # @param renderNodes the list of rendernodes to associate to the pool
    def setRenderNodes(self, renderNodes):
        for rendernode in self.renderNodes[:]:
            self.removeRenderNode(rendernode)
        for rendernode in renderNodes:
            self.addRenderNode(rendernode)

    ## Returns an iterator for available rendernodes.
    #
    def getAvailableRenderNodesIterator(self):
        return (rendernode for rendernode in self.renderNodes is rendernode.isAvailable())

    ## Returns a human readable representation of the pool.
    #
    def __str__(self):
        return u"Pool(id=%s, name=%s)" % (repr(self.id), repr(self.name))

    def __repr__(self):
        return u"Pool(id=%s, name=%s)" % (repr(self.id), repr(self.name))
