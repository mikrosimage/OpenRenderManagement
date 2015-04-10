#!/usr/bin/env python
# coding: utf-8
from __future__ import absolute_import

"""
"""


from puliclient.server.renderNodeHandler import RenderNodeHandler
from octopus.core import enums

rnHandler = RenderNodeHandler()
(allRnList, summary) = rnHandler.getAllRenderNodes()
print "All render nodes"
if allRnList:
    for rn in allRnList:
        print "id:%s name:%s hostname:%s ram:%s/%s" % (rn.id, rn.name, rn.host, rn.systemFreeRam, rn.ramSize)

# print rnHandler.getRenderNodesById([1, 2, 3, 4, 5])
# print rnHandler.getRenderNodesByName(['vfxpc.*:8000'])
# print rnHandler.getRenderNodesByStatus([enums.RN_UNKNOWN])
# print rnHandler.getRenderNodesByHost(['vfxpc64'])
# print rnHandler.getRenderNodesByVersion(['dev'])
# print rnHandler.getRenderNodesByPool(None)

(results, summary) = rnHandler.getRenderNodes(
    idList=[4, 5, 6],
    nameList=["vfxpc.*:8000"],
    hostList=["vfxpc64"],
    versionList=["dev"],
    poolList=["default", "renderfarm"],
    statusList=[enums.RN_IDLE],
)

print ""
print "Query render nodes:"
if results:
    for rn in results:
        print "id:%s name:%s hostname:%s puliversion:%s ram:%s/%s" % (rn.id, rn.name, rn.host, rn.puliversion, rn.systemFreeRam, rn.ramSize)
