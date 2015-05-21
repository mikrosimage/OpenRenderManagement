#!/usr/bin/python2.7
#! -*- encoding: utf-8 -*-

import argparse
from puliclient import Task, Graph


# Create Folder Nodes (ie Task Groups)
def createFolderNodes(parent, listGroup, nbTasks, nbCmds):
    name = "FolderNode"
    tags = {"prod": "mikros_test", "nbFrames": 1}

    if len(listGroup) > 0:
        nbGroup = listGroup.pop(0)
        for inbFolderNode in range(nbGroup):
            print "Create %s" % name
            taskGroup = parent.addNewTaskGroup(name=name, tags=tags)
            tmp_listGroup = listGroup[:]
            createFolderNodes(taskGroup, tmp_listGroup, nbTasks, args.nbCmds)
    else:
        for inbTask in range(nbTasks):
            createTaskNode(parent, inbTask, nbCmds)

# Create TaskNode (ie Task)
def createTaskNode(parent, nbTasks, nbCmds):
    name = "TaskNode"
    print "Create %s" % name

    cmd = "echo 'command %%MI_FRAME%%'"
    arguments = {"cmd": cmd, "start": 0, "end": nbCmds - 1}
    tags = {"prod": "mikros_test", "nbFrames": 1}
    task = parent.addNewTask(name=name, arguments=arguments, tags=tags)


# Parse arguments
parser = argparse.ArgumentParser(description='Puli - Test tree depth.')
parser.add_argument('nbGroups', 
                   metavar='NB_GROUP',
                   type=int,
                   nargs='+',
                   help='Number of groups.')
parser.add_argument('nbTasks', 
                   metavar='NB_TASK',
                   type=int,
                   help='Number of task by group.')
parser.add_argument('nbCmds',
                   metavar='NB_CMD',
                   type=int,
                   help='Number of commands by task')
parser.add_argument('server',
                   metavar='SERVER',
                   type=str,
                   help='Name of the server on which to submit job')
args = parser.parse_args()

# Create graph
runner = "puliclient.contrib.generic.GenericRunner"
name = "Job_Test"
tags = {"prod": "mikros_test", "comment": "JOB TEST", "nbFrames": 1}
puliGraph = Graph(name, poolName='mik', tags=tags, maxRN=5)

print "NB_GROUP = " + str(args.nbGroups)
print "NB_TASK = " + str(args.nbTasks)
print "NB_CMD = " + str(args.nbCmds)
print "SERVER = " + args.server

createFolderNodes(puliGraph, args.nbGroups, args.nbTasks, args.nbCmds)

puliGraph.submit(args.server, 8004)

