#!/usr/bin/env python
# coding: utf-8
from __future__ import absolute_import

"""
"""


from puliclient.server.renderNodeHandler import RenderNodeHandler

allRnList = RenderNodeHandler.getAllRenderNodes()

if allRnList:
    for rn in allRnList:
        print "%s: RAM=%s/%s" % (rn.name, rn.systemFreeRam, rn.ramSize)

singleRn = RenderNodeHandler.getRenderNode("vfxpc64", 9000)
print "----------------------"
print "Render node %s has a total of %d MB RAM but only %d MB are considered free by the system." % (singleRn.name, singleRn.ramSize, singleRn.systemFreeRam)
print "Detailled data: "
print singleRn.toJson(indent=4)
