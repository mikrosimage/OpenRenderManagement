#!/usr/bin/env python
# coding: utf-8
"""
"""

from __future__ import absolute_import

from puliclient.server.server import Server
from puliclient.server.queueHandler import QueueHandler
from octopus.core import enums

qh = QueueHandler(Server("localhost", 8004))
allJobsList, summary = qh.getAllJobs()

for job in allJobsList:
    print "id:%s name:%s user:%s" % (job.id, job.name, job.user)

print qh.getAllJobs()
print qh.getJobsByStatus([enums.NODE_CANCELED, enums.NODE_RUNNING])
print qh.getJobsById(range(1, 100))
print qh.getJobsByName(['.*test'], False)
print qh.getJobsByUser(['jsa'], False)[0]
print qh.getJobsByProd(['prod_name'], False)
print qh.getJobsByTags({'nbFrames': [10]}, False)
print qh.getJobsByTags({'nbFrames': [10], 'prod': ['test']}, False)
print qh.getJobsByPool(['renderfarm'], False)

print qh.getJobs(
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

# print qh.getJobsByCreateDate(['>2015-01-01', '<2015-02-01'], False)[0]
