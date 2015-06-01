#!/usr/bin/python2.7
#! -*- encoding: utf-8 -*-


import simplejson as json
import threading 

from sqlobject import SQLObject, connectionForURI
from sqlobject.sqlbuilder import Insert, Select, Delete

from octopus.dispatcher.model.node import FolderNode, TaskNode
from octopus.dispatcher.model.task import Task, TaskGroup
from octopus.dispatcher.model.command import Command
from octopus.dispatcher.model.rendernode import RenderNode
from octopus.dispatcher.model.pool import Pool, PoolShare
from octopus.dispatcher.strategies import createStrategyInstance
from octopus.dispatcher import settings
from octopus.dispatcher.db.pulidb import FolderNodes, TaskNodes, Dependencies, TaskGroups, Rules, Tasks, Commands, Pools, PoolShares, PuliDB, StatDB 
from octopus.core.tools import elapsedTimeToString
from octopus.core import singletonconfig

BUFFER_SIZE = 10000

def deleteElementFromMainDB(table, elementId):
	mainConn.query(mainConn.sqlrepr(Delete(table.q, where=(table.q.id==elementId))))

def insertElementIntoStatDB(table, values):
	statConn.query(statConn.sqlrepr(Insert(table.q, values=values)))

def archiveTaskNodesDependencies(taskNodeId):
	archiveDependencies(taskNodeId, Dependencies.q.taskNodes)

def archiveFolderNodesDependencies(folderNodeId):
	archiveDependencies(folderNodeId, Dependencies.q.folderNodes)

def archiveTaskNodesRules(taskNodeId):
	archiveDependencies(taskNodeId, Rules.q.taskNodeId)

def archiveFolderNodesRules(folderNodeId):
	archiveRules(folderNodeId, Rules.q.folderNodeId)

def archiveDependencies(nodeId, nodeType):
	Dependencies._connection = mainConn
	dependencies = Dependencies.select(nodeType==nodeId)
	for dependency in dependencies:
		duplicateDependencyIntoStatDB(dependency)
		deleteElementFromMainDB(Dependencies, dependency.id)

def archiveRules(nodeId, nodeType):
	Rules._connection = mainConn
	rules = Rules.select(nodeType==nodeId)
	for rule in rules:
		duplicateRueIntoStatDB(rule)
		deleteElementFromMainDB(Rules, rule.id) 

def archivePoolShares():
	PoolShares._connection = mainConn
	print "Starting to archive PoolShares"
	poolSharestoArchive = PoolShares.select(PoolShares.q.archived==True)
	processedItems = 0
	totalItems = poolSharestoArchive.count()
	print "Found " + str(totalItems) + " PoolShares to archive"
	while totalItems > processedItems:
		for poolShare in poolSharestoArchive.limit(BUFFER_SIZE):
			duplicatePoolSharesIntoStatDB(poolShare)
			deleteElementFromMainDB(PoolShares, poolShare.id)
			processedItems+=1
		print str(totalItems - processedItems) + " PoolShares remaining"
	print "Finished to archive PoolShares"

def archivePools():
	Pools._connection = mainConn
	print "Starting to archive Pools"
	poolstoArchive = Pools.select(Pools.q.archived==True)
	processedItems = 0
	totalItems = poolstoArchive.count()
	print "Found " + str(totalItems) + " Pools to archive"
	while totalItems > processedItems:
		for pool in poolstoArchive.limit(BUFFER_SIZE):
			duplicatePoolsIntoStatDB(pool)
			deleteElementFromMainDB(Pools, pool.id)
			processedItems+=1
		print str(totalItems - processedItems) + " Pools remaining"
	print "Finished to archive Pools"

def archiveFolderNodes():
	FolderNodes._connection = mainConn
	print "Starting to archive FolderNodes"
	folderNodestoArchive = FolderNodes.select(FolderNodes.q.archived==True)
	processedItems = 0
	totalItems = folderNodestoArchive.count()
	print "Found " + str(totalItems) + " FolderNodes to archive"
	while totalItems > processedItems:
		for node in folderNodestoArchive.limit(BUFFER_SIZE):
			duplicateFolderNodesIntoStatDB(node)
			deleteElementFromMainDB(FolderNodes, node.id)
			archiveFolderNodesDependencies(node.id)
			archiveFolderNodesRules(node.id)
			processedItems+=1
		print str(totalItems - processedItems) + " FolderNodes remaining"
	print "Finished to archive FolderNodes"

def archiveTaskNodes():
	TaskNodes._connection = mainConn
	print "Starting to archive TaskNodes"
	taskNodestoArchive = TaskNodes.select(TaskNodes.q.archived==True)
	processedItems = 0
	totalItems = taskNodestoArchive.count()
	print "Found " + str(totalItems) + " TaskNodes to archive"
	while totalItems > processedItems:
		for node in taskNodestoArchive.limit(BUFFER_SIZE):
			duplicateTaskNodesIntoStatDB(node)
			deleteElementFromMainDB(TaskNodes, node.id)
			archiveTaskNodesDependencies(node.id)
			archiveTaskNodesRules(node.id)
			processedItems+=1
		print str(totalItems - processedItems) + " TaskNodes remaining"
	print "Finished to archive TaskNodes"

def archiveCommands():
	Commands._connection = mainConn
	print "Starting to archive Commands"
	commandsToArchive = Commands.select(Commands.q.archived==True)
	processedItems = 0
	totalItems = commandsToArchive.count()
	print "Found " + str(totalItems) + " Commands to archive"
	while totalItems > processedItems:
		for commands in commandsToArchive.limit(BUFFER_SIZE):
			duplicateCommandIntoStatDB(commands)
			deleteElementFromMainDB(Commands, commands.id)
			processedItems+=1
		print str(totalItems - processedItems) + " Commands remaining"
	print "Finished to archive Commands"

def archiveTaskGroups():
	TaskGroups._connection = mainConn
	print "Starting to archive taskGroups"
	tasksGroupsToArchive = TaskGroups.select(TaskGroups.q.archived==True)
	processedItems = 0
	totalItems = tasksGroupsToArchive.count()
	print "Found " + str(totalItems) + " taskGroups to archive"
	while totalItems > processedItems:
		for taskGroup in tasksGroupsToArchive.limit(BUFFER_SIZE):
			duplicateTaskGroupIntoStatDB(taskGroup)
			deleteElementFromMainDB(TaskGroups, taskGroup.id)
			processedItems+=1
		print str(totalItems - processedItems) + " taskGroups remaining"
	print "Finished to archive taskGroups"

def archiveTasks():
	Tasks._connection = mainConn
	print "Starting to archive tasks"
	tasksToArchive = Tasks.select(Tasks.q.archived==True)
	processedItems = 0
	totalItems = tasksToArchive.count()
	print "Found " + str(totalItems) + " tasks to archive"
	while totalItems > processedItems:
		for task in tasksToArchive.limit(BUFFER_SIZE):
			duplicateTaskIntoStatDB(task)
			deleteElementFromMainDB(Tasks, task.id)
			processedItems+=1
		print str(totalItems - processedItems) + " tasks remaining"
	print "Finished to archive tasks"

def duplicateRueIntoStatDB(rule):
	fields = {Rules.q.id.fieldName: rule.id,
			  Rules.q.name.fieldName: rule.name,
			  Rules.q.taskNodeId.fieldName: rule.taskNodeId,
			  Rules.q.folderNodeId.fieldName: rule.folderNodeId}
	insertElementIntoStatDB(Rules, fields)


def duplicateDependencyIntoStatDB(element):
	fields = {Dependencies.q.toNodeId.fieldName: element.toNodeId,
			  Dependencies.q.statusList.fieldName: element.statusList,
			  Dependencies.q.taskNodes.fieldName: element.taskNodesID,
			  Dependencies.q.folderNodes.fieldName: element.folderNodesID,
			  Dependencies.q.archived.fieldName: False}
	insertElementIntoStatDB(Dependencies,fields)

def duplicateRenderNodesIntoStatDB(element):
	fields = {RenderNodes.q.id.fieldName: element.id,
			  RenderNodes.q.name.fieldName: element.name,
			  RenderNodes.q.coresNumber.fieldName: element.coresNumber,
			  RenderNodes.q.speed.fieldName: element.speed,
			  RenderNodes.q.ip.fieldName: element.ip,
			  RenderNodes.q.port.fieldName: element.port,
			  RenderNodes.q.ramSize.fieldName: element.ramSize,
			  RenderNodes.q.caracteristics.fieldName: json.dumps(element.caracteristics),
			  RenderNodes.q.performance.fieldName: element.performance}
	insertElementIntoStatDB(RenderNodes,fields)

def duplicatePoolSharesIntoStatDB(element):
	fields = {PoolShares.q.id.fieldName: element.id,
			  PoolShares.q.poolId.fieldName: element.poolId,
			  PoolShares.q.nodeId.fieldName: element.nodeId,
			  PoolShares.q.maxRN.fieldName: element.maxRN,
			  PoolShares.q.archived.fieldName: True}
	insertElementIntoStatDB(PoolShares,fields)

def duplicatePoolsIntoStatDB(element):
	fields = {Pools.q.id.fieldName: element.id,
			  Pools.q.name.fieldName: element.name,
			  Pools.q.archived.fieldName: True}
	insertElementIntoStatDB(Pools,fields)

def duplicateFolderNodesIntoStatDB(element):
	fields = {FolderNodes.q.id.fieldName: element.id,
			  FolderNodes.q.name.fieldName: element.name,
			  FolderNodes.q.parentId.fieldName: element.parentId,
			  FolderNodes.q.user.fieldName: element.user,
			  FolderNodes.q.priority.fieldName: element.priority,
			  FolderNodes.q.dispatchKey.fieldName: element.dispatchKey,
			  FolderNodes.q.maxRN.fieldName: element.maxRN,
			  FolderNodes.q.taskGroupId.fieldName: element.taskGroupId,
			  FolderNodes.q.strategy.fieldName: element.strategy,
			  FolderNodes.q.creationTime.fieldName: element.creationTime,
			  FolderNodes.q.startTime.fieldName: element.startTime,
			  FolderNodes.q.updateTime.fieldName: element.updateTime,
			  FolderNodes.q.endTime.fieldName: element.endTime,
			  FolderNodes.q.archived.fieldName: True}
	insertElementIntoStatDB(FolderNodes,fields)


def duplicateTaskNodesIntoStatDB(element):
	fields = {TaskNodes.q.id.fieldName: element.id,
			  TaskNodes.q.name.fieldName: element.name,
			  TaskNodes.q.parentId.fieldName: element.parentId,
			  TaskNodes.q.user.fieldName: element.user,
			  TaskNodes.q.priority.fieldName: element.priority,
			  TaskNodes.q.dispatchKey.fieldName: element.dispatchKey,
			  TaskNodes.q.maxRN.fieldName: element.maxRN,
			  TaskNodes.q.taskId.fieldName: element.taskId,
			  TaskNodes.q.creationTime.fieldName: element.creationTime,
			  TaskNodes.q.startTime.fieldName: element.startTime,
			  TaskNodes.q.updateTime.fieldName: element.updateTime,
			  TaskNodes.q.endTime.fieldName: element.endTime,
			  TaskNodes.q.maxAttempt.fieldName: element.maxAttempt,
			  TaskNodes.q.archived.fieldName: True}
	insertElementIntoStatDB(TaskNodes,fields)

def duplicateCommandIntoStatDB(element):
	fields = {Commands.q.id.fieldName: element.id,
			  Commands.q.description.fieldName: element.description,
			  Commands.q.taskId.fieldName: element.taskId,
			  Commands.q.status.fieldName: element.status,
			  Commands.q.completion.fieldName: element.completion,
			  Commands.q.creationTime.fieldName: element.creationTime,
			  Commands.q.startTime.fieldName: element.startTime,
			  Commands.q.updateTime.fieldName: element.updateTime,
			  Commands.q.endTime.fieldName: element.endTime,
			  Commands.q.message.fieldName: element.message,
			  Commands.q.stats.fieldName: str(element.stats),
			  Commands.q.archived.fieldName: True,
			  Commands.q.args.fieldName: str(element.args),
			  Commands.q.attempt.fieldName: str(element.attempt),
			  Commands.q.runnerPackages.fieldName: json.dumps(element.runnerPackages),
			  Commands.q.watcherPackages.fieldName: json.dumps(element.watcherPackages)}
	insertElementIntoStatDB(Commands,fields)

def duplicateTaskGroupIntoStatDB(element):
	fields = {TaskGroups.q.id.fieldName: element.id,
			  TaskGroups.q.name.fieldName: element.name,
			  TaskGroups.q.parentId.fieldName: element.parentId,
			  TaskGroups.q.user.fieldName: element.user,
			  TaskGroups.q.priority.fieldName: element.priority,
			  TaskGroups.q.dispatchKey.fieldName: element.dispatchKey,
			  TaskGroups.q.maxRN.fieldName: element.maxRN,
			  TaskGroups.q.environment.fieldName: json.dumps(element.environment),
			  TaskGroups.q.requirements.fieldName: json.dumps(element.requirements),
			  TaskGroups.q.tags.fieldName: json.dumps(element.tags),
			  TaskGroups.q.strategy.fieldName: element.strategy,
			  TaskGroups.q.archived.fieldName: True,
			  TaskGroups.q.args.fieldName: str(element.args)}
	insertElementIntoStatDB(TaskGroups,fields)

def duplicateTaskIntoStatDB(element):
	fields = {Tasks.q.id.fieldName: element.id,
			  Tasks.q.name.fieldName: element.name,
			  Tasks.q.parentId.fieldName: element.parentId,
			  Tasks.q.user.fieldName: element.user,
			  Tasks.q.priority.fieldName: element.priority,
			  Tasks.q.dispatchKey.fieldName: element.dispatchKey,
			  Tasks.q.maxRN.fieldName: element.maxRN,
			  Tasks.q.runner.fieldName: element.runner,
			  Tasks.q.environment.fieldName: json.dumps(element.environment),
			  Tasks.q.requirements.fieldName: json.dumps(element.requirements),
			  Tasks.q.minNbCores.fieldName: element.minNbCores,
			  Tasks.q.maxNbCores.fieldName: element.maxNbCores,
			  Tasks.q.ramUse.fieldName: element.ramUse,
			  Tasks.q.licence.fieldName: element.licence,
			  Tasks.q.tags.fieldName: json.dumps(element.tags),
			  Tasks.q.validationExpression.fieldName: element.validationExpression,
			  Tasks.q.archived.fieldName: True,
			  Tasks.q.args.fieldName: str(element.args),
			  Tasks.q.maxAttempt.fieldName: element.maxAttempt,
			  Tasks.q.runnerPackages.fieldName: json.dumps(element.runnerPackages),
			  Tasks.q.watcherPackages.fieldName: json.dumps(element.watcherPackages)}
	insertElementIntoStatDB(Tasks,fields)



mainConn = connectionForURI(settings.DB_URL)
statConn = StatDB.createConnection()

threading.Thread(target=archiveTasks).start()
threading.Thread(target=archiveTaskGroups).start() 
threading.Thread(target=archiveCommands).start() 
threading.Thread(target=archiveTaskNodes).start() 
threading.Thread(target=archiveFolderNodes).start() 
threading.Thread(target=archivePools).start() 
threading.Thread(target=archivePoolShares).start() 

