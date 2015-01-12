#!/usr/bin/env python
####################################################################################################
# @file pulidb.py
# @package octopus.dispatcher.db
# @author Arnaud Chassagne
# @date 2010/10/20
# @version 2.0
#
# @mainpage
#
####################################################################################################

from sqlobject import (SQLObject, UnicodeCol, IntCol, FloatCol, DateTimeCol,
                       BoolCol, MultipleJoin, RelatedJoin, connectionForURI,
                       ForeignKey, sqlhub)
# from sqlobject.sqlbuilder import *
from sqlobject.sqlbuilder import Insert, Update, IN, Select, Table, AND, INNERJOINOn, Delete

from collections import defaultdict
import datetime
import logging
import time
try:
    import simplejson as json
except ImportError:
    import json

from octopus.dispatcher.model.node import FolderNode, TaskNode
from octopus.dispatcher.model.task import Task, TaskGroup
from octopus.dispatcher.model.command import Command
from octopus.dispatcher.model.rendernode import RenderNode
from octopus.dispatcher.model.pool import Pool, PoolShare
from octopus.dispatcher.strategies import createStrategyInstance
from octopus.core.tools import elapsedTimeToString

from octopus.core import singletonconfig

LOGGER = logging.getLogger('main.dispatcher')


class FolderNodes(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    parentId = IntCol()
    user = UnicodeCol()
    priority = IntCol()
    dispatchKey = FloatCol()
    maxRN = IntCol()
    taskGroupId = IntCol()
    strategy = UnicodeCol()
    creationTime = DateTimeCol()
    startTime = DateTimeCol()
    updateTime = DateTimeCol()
    endTime = DateTimeCol()
    archived = BoolCol()
    dependencies = MultipleJoin('Dependencies')


class TaskNodes(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    parentId = IntCol()
    user = UnicodeCol()
    priority = IntCol()
    dispatchKey = FloatCol()
    maxRN = IntCol()
    taskId = IntCol()
    creationTime = DateTimeCol()
    startTime = DateTimeCol()
    updateTime = DateTimeCol()
    endTime = DateTimeCol()
    archived = BoolCol()
    dependencies = MultipleJoin('Dependencies')
    # Adding autoretry capability on task
    maxAttempt = IntCol()


class Dependencies(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    toNodeId = IntCol()
    statusList = UnicodeCol()
    taskNodes = ForeignKey('TaskNodes')
    folderNodes = ForeignKey('FolderNodes')
    archived = BoolCol()


class TaskGroups(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    parentId = IntCol()
    user = UnicodeCol()
    priority = IntCol()
    dispatchKey = FloatCol()
    maxRN = IntCol()
    environment = UnicodeCol()
    requirements = UnicodeCol()
    tags = UnicodeCol()
    strategy = UnicodeCol()
    archived = BoolCol()
    args = UnicodeCol()


class Rules(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    taskNodeId = IntCol()
    folderNodeId = IntCol()


class Tasks(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    parentId = IntCol()
    user = UnicodeCol()
    priority = IntCol()
    dispatchKey = FloatCol()
    maxRN = IntCol()
    runner = UnicodeCol()
    environment = UnicodeCol()
    requirements = UnicodeCol()
    minNbCores = IntCol()
    maxNbCores = IntCol()
    ramUse = IntCol()
    licence = UnicodeCol()
    tags = UnicodeCol()
    validationExpression = UnicodeCol()
    archived = BoolCol()
    args = UnicodeCol()

    # REZ env management support
    runnerPackages = UnicodeCol()
    watcherPackages = UnicodeCol()

    # Adding autoretry capability on task
    maxAttempt = IntCol()


class Commands(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    description = UnicodeCol()
    taskId = IntCol()
    status = IntCol()
    completion = FloatCol()
    creationTime = DateTimeCol()
    startTime = DateTimeCol()
    updateTime = DateTimeCol()
    endTime = DateTimeCol()
    assignedRNId = IntCol()
    message = UnicodeCol()
    stats = UnicodeCol()
    archived = BoolCol()
    args = UnicodeCol()

    # REZ env management support
    runnerPackages = UnicodeCol()
    watcherPackages = UnicodeCol()

    # Adding autoretry capability on command
    attempt = IntCol()


class Pools(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    archived = BoolCol()
    renderNodes = RelatedJoin('RenderNodes')


class PoolShares(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    poolId = IntCol()
    nodeId = IntCol()
    maxRN = IntCol()
    archived = BoolCol()


class RenderNodes(SQLObject):
    class sqlmeta:
        lazyUpdate = True
    name = UnicodeCol()
    coresNumber = IntCol()
    speed = FloatCol()
    ip = UnicodeCol()
    port = IntCol()
    ramSize = IntCol()
    caracteristics = UnicodeCol()
    pools = RelatedJoin('Pools')
    performance = FloatCol()


def createTables():
    FolderNodes.createTable(ifNotExists=True)
    TaskNodes.createTable(ifNotExists=True)
    Dependencies.createTable(ifNotExists=True)
    TaskGroups.createTable(ifNotExists=True)
    Rules.createTable(ifNotExists=True)
    Tasks.createTable(ifNotExists=True)
    Commands.createTable(ifNotExists=True)
    Pools.createTable(ifNotExists=True)
    PoolShares.createTable(ifNotExists=True)
    RenderNodes.createTable(ifNotExists=True)


def dropTables():
    Dependencies.dropTable(ifExists=True)
    FolderNodes.dropTable(ifExists=True)
    TaskNodes.dropTable(ifExists=True)
    TaskGroups.dropTable(ifExists=True)
    Rules.dropTable(ifExists=True)
    Tasks.dropTable(ifExists=True)
    Commands.dropTable(ifExists=True)
    Pools.dropTable(ifExists=True)
    PoolShares.dropTable(ifExists=True)
    RenderNodes.dropTable(ifExists=True)


class PuliDB(object):
    def __init__(self, cleanDB, licManager=None):
        from octopus.dispatcher import settings
        # init the connection
        sqlhub.processConnection = connectionForURI(settings.DB_URL)
        # drop all the tables
        if cleanDB:
            LOGGER.warning("dropping database tables")
            dropTables()
        # create the tables, if necessary
        LOGGER.warning("creating database tables")
        createTables()
        self.licenseManager = licManager

    def dropPoolsAndRnsTables(self):
        Pools.dropTable(ifExists=True)
        RenderNodes.dropTable(ifExists=True)
        Pools.createTable(ifNotExists=True)
        RenderNodes.createTable(ifNotExists=True)

    ## Creates the provided elements in the database.
    # @param elements the elements to create
    #
    def createElements(self, elements):
        elements.sort(key=lambda element: element.__class__)
        for element in elements:
            # LOGGER.info("            ----> Creating elem = %s" % element )
            # /////////////// Handling of the TaskNode
            if isinstance(element, TaskNode):
                conn = TaskNodes._connection
                fields = {TaskNodes.q.id.fieldName: element.id,
                          TaskNodes.q.name.fieldName: element.name,
                          TaskNodes.q.parentId.fieldName: element.parent.id,
                          TaskNodes.q.user.fieldName: element.user,
                          TaskNodes.q.priority.fieldName: element.priority,
                          TaskNodes.q.dispatchKey.fieldName: element.dispatchKey,
                          TaskNodes.q.maxRN.fieldName: element.maxRN,
                          TaskNodes.q.taskId.fieldName: element.task.id,
                          TaskNodes.q.creationTime.fieldName: self.getDateFromTimeStamp(element.creationTime),
                          TaskNodes.q.startTime.fieldName: self.getDateFromTimeStamp(element.startTime),
                          TaskNodes.q.updateTime.fieldName: self.getDateFromTimeStamp(element.updateTime),
                          TaskNodes.q.endTime.fieldName: self.getDateFromTimeStamp(element.endTime),
                          TaskNodes.q.maxAttempt.fieldName: element.maxAttempt,
                          TaskNodes.q.archived.fieldName: False}
                conn.query(conn.sqlrepr(Insert(TaskNodes.q, values=fields)))
                conn.cache.clear()
                if element.dependencies:
                    conn = Dependencies._connection
                    for (toNode, statusList) in element.dependencies:
                        statusStringList = [str(i) for i in statusList]
                        fields = {Dependencies.q.toNodeId.fieldName: toNode.id,
                                  Dependencies.q.statusList.fieldName: ','.join(statusStringList),
                                  Dependencies.q.taskNodes.fieldName: element.id,
                                  Dependencies.q.folderNodes.fieldName: None,
                                  Dependencies.q.archived.fieldName: False}
                        conn.query(conn.sqlrepr(Insert(Dependencies.q, values=fields)))
                        conn.cache.clear()

            # /////////////// Handling of the FolderNode
            elif isinstance(element, FolderNode):
                conn = FolderNodes._connection
                fields = {FolderNodes.q.id.fieldName: element.id,
                          FolderNodes.q.name.fieldName: element.name,
                          FolderNodes.q.parentId.fieldName: element.parent.id,
                          FolderNodes.q.user.fieldName: element.user,
                          FolderNodes.q.priority.fieldName: element.priority,
                          FolderNodes.q.dispatchKey.fieldName: element.dispatchKey,
                          FolderNodes.q.maxRN.fieldName: element.maxRN,
                          FolderNodes.q.taskGroupId.fieldName: element.taskGroup.id if element.taskGroup else None,
                          FolderNodes.q.strategy.fieldName: element.strategy.getClassName(),
                          FolderNodes.q.creationTime.fieldName: self.getDateFromTimeStamp(element.creationTime),
                          FolderNodes.q.startTime.fieldName: self.getDateFromTimeStamp(element.startTime),
                          FolderNodes.q.updateTime.fieldName: self.getDateFromTimeStamp(element.updateTime),
                          FolderNodes.q.endTime.fieldName: self.getDateFromTimeStamp(element.endTime),
                          FolderNodes.q.archived.fieldName: False}
                conn.query(conn.sqlrepr(Insert(FolderNodes.q, values=fields)))
                conn.cache.clear()
                if element.dependencies:
                    conn = Dependencies._connection
                    for (toNode, statusList) in element.dependencies:
                        statusStringList = [str(i) for i in statusList]
                        fields = {Dependencies.q.toNodeId.fieldName: toNode.id,
                                  Dependencies.q.statusList.fieldName: ','.join(statusStringList),
                                  Dependencies.q.taskNodes.fieldName: None,
                                  Dependencies.q.folderNodes.fieldName: element.id,
                                  Dependencies.q.archived.fieldName: False}
                        conn.query(conn.sqlrepr(Insert(Dependencies.q, values=fields)))
                        conn.cache.clear()

            # /////////////// Handling of the TaskGroup
            elif isinstance(element, TaskGroup):
                for (rule, node) in element.nodes.iteritems():
                    conn = Rules._connection
                    fields = {Rules.q.name.fieldName: rule,
                              Rules.q.taskNodeId.fieldName: None,
                              Rules.q.folderNodeId.fieldName: node.id}
                    conn.query(conn.sqlrepr(Insert(Rules.q, values=fields)))
                    conn.cache.clear()
                conn = TaskGroups._connection
                fields = {TaskGroups.q.id.fieldName: element.id,
                          TaskGroups.q.name.fieldName: element.name,
                          TaskGroups.q.parentId.fieldName: element.parent.id if element.parent else None,
                          TaskGroups.q.user.fieldName: element.user,
                          TaskGroups.q.priority.fieldName: element.priority,
                          TaskGroups.q.dispatchKey.fieldName: element.dispatchKey,
                          TaskGroups.q.maxRN.fieldName: element.maxRN,
                          TaskGroups.q.environment.fieldName: json.dumps(element.environment),
                          TaskGroups.q.requirements.fieldName: json.dumps(element.requirements),
                          TaskGroups.q.tags.fieldName: json.dumps(element.tags),
                          TaskGroups.q.strategy.fieldName: element.strategy.getClassName(),
                          TaskGroups.q.archived.fieldName: False,
                          TaskGroups.q.args.fieldName: str(element.arguments)}
                conn.query(conn.sqlrepr(Insert(TaskGroups.q, values=fields)))
                conn.cache.clear()

            # /////////////// Handling of the Task
            elif isinstance(element, Task):
                for (rule, node) in element.nodes.iteritems():
                    conn = Rules._connection
                    fields = {Rules.q.name.fieldName: rule,
                              Rules.q.taskNodeId.fieldName: node.id,
                              Rules.q.folderNodeId.fieldName: None}
                    conn.query(conn.sqlrepr(Insert(Rules.q, values=fields)))
                    conn.cache.clear()
                conn = Tasks._connection
                fields = {Tasks.q.id.fieldName: element.id,
                          Tasks.q.name.fieldName: element.name,
                          Tasks.q.parentId.fieldName: element.parent.id if element.parent else None,
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
                          Tasks.q.licence.fieldName: element.lic,
                          Tasks.q.tags.fieldName: json.dumps(element.tags),
                          Tasks.q.validationExpression.fieldName: element.validationExpression,
                          Tasks.q.archived.fieldName: False,
                          Tasks.q.args.fieldName: str(element.arguments),
                          Tasks.q.maxAttempt.fieldName: element.maxAttempt,
                          Tasks.q.runnerPackages.fieldName: json.dumps(element.runnerPackages),
                          Tasks.q.watcherPackages.fieldName: json.dumps(element.watcherPackages)
                          }
                conn.query(conn.sqlrepr(Insert(Tasks.q, values=fields)))
                conn.cache.clear()

            # /////////////// Handling of the Command
            elif isinstance(element, Command):
                conn = Commands._connection
                fields = {Commands.q.id.fieldName: element.id,
                          Commands.q.description.fieldName: element.description,
                          Commands.q.taskId.fieldName: element.task.id,
                          Commands.q.status.fieldName: element.status,
                          Commands.q.completion.fieldName: element.completion,
                          Commands.q.creationTime.fieldName: self.getDateFromTimeStamp(element.creationTime),
                          Commands.q.startTime.fieldName: self.getDateFromTimeStamp(element.startTime),
                          Commands.q.updateTime.fieldName: self.getDateFromTimeStamp(element.updateTime),
                          Commands.q.endTime.fieldName: self.getDateFromTimeStamp(element.endTime),
                          Commands.q.assignedRNId.fieldName: element.renderNode.id if element.renderNode else None,
                          Commands.q.message.fieldName: element.message,
                          Commands.q.stats.fieldName: str(element.stats),
                          Commands.q.archived.fieldName: False,
                          Commands.q.args.fieldName: str(element.arguments),
                          Commands.q.attempt.fieldName: str(element.attempt),
                          Commands.q.runnerPackages.fieldName: json.dumps(element.runnerPackages),
                          Commands.q.watcherPackages.fieldName: json.dumps(element.watcherPackages)
                          }
                conn.query(conn.sqlrepr(Insert(Commands.q, values=fields)))
                conn.cache.clear()

            # /////////////// Handling of the RenderNode
            elif isinstance(element, RenderNode):
                conn = RenderNodes._connection
                fields = {RenderNodes.q.id.fieldName: element.id,
                          RenderNodes.q.name.fieldName: element.name,
                          RenderNodes.q.coresNumber.fieldName: element.coresNumber,
                          RenderNodes.q.speed.fieldName: element.speed,
                          RenderNodes.q.ip.fieldName: element.host,
                          RenderNodes.q.port.fieldName: element.port,
                          RenderNodes.q.ramSize.fieldName: element.ramSize,
                          RenderNodes.q.caracteristics.fieldName: json.dumps(element.caracteristics),
                          RenderNodes.q.performance.fieldName: element.performance}
                conn.query(conn.sqlrepr(Insert(RenderNodes.q, values=fields)))
                conn.cache.clear()

            # /////////////// Handling of the Pool
            elif isinstance(element, Pool):
                conn = Pools._connection
                fields = {Pools.q.id.fieldName: element.id,
                          Pools.q.name.fieldName: element.name,
                          Pools.q.archived.fieldName: False}
                conn.query(conn.sqlrepr(Insert(Pools.q, values=fields)))
                conn.cache.clear()
                for renderNode in element.renderNodes:
                    conn.query(conn.sqlrepr(Insert('pools_render_nodes', values={'pools_id': element.id, 'render_nodes_id': renderNode.id})))
                    conn.cache.clear()

            # /////////////// Handling of the PoolShare
            elif isinstance(element, PoolShare):
                conn = PoolShares._connection
                fields = {PoolShares.q.id.fieldName: element.id,
                          PoolShares.q.poolId.fieldName: element.pool.id,
                          PoolShares.q.nodeId.fieldName: element.node.id,
                          PoolShares.q.maxRN.fieldName: element.maxRN,
                          PoolShares.q.archived.fieldName: False}
                conn.query(conn.sqlrepr(Insert(PoolShares.q, values=fields)))
                conn.cache.clear()

    ## Updates the provided elements to the database.
    # @param elements the elements to update
    #
    def updateElements(self, elements):
        for element in elements:
            # LOGGER.info("            ----> Updating elem = %s" % element )
            if isinstance(element, Command) or isinstance(element, TaskNode) or isinstance(element, FolderNode):
                startTime = self.getDateFromTimeStamp(element.startTime)
                endTime = self.getDateFromTimeStamp(element.endTime)
                updateTime = self.getDateFromTimeStamp(element.updateTime)

            # /////////////// Handling of the Command
            if isinstance(element, Command):
                if element.id:
                    conn = Commands._connection
                    fields = {Commands.q.status.fieldName: element.status,
                              Commands.q.completion.fieldName: element.completion,
                              Commands.q.startTime.fieldName: startTime,
                              Commands.q.updateTime.fieldName: updateTime,
                              Commands.q.stats.fieldName: str(element.stats),
                              Commands.q.attempt.fieldName: str(element.attempt),
                              Commands.q.endTime.fieldName: endTime}
                    if element.renderNode:
                        fields[Commands.q.assignedRNId.fieldName] = element.renderNode.id
                    conn.query(conn.sqlrepr(Update(Commands.q, values=fields, where=(Commands.q.id == element.id))))
                    conn.cache.clear()

            # /////////////// Handling of the TaskNode
            elif isinstance(element, TaskNode):
                if element.id:
                    conn = TaskNodes._connection
                    fields = {TaskNodes.q.startTime.fieldName: startTime,
                              TaskNodes.q.updateTime.fieldName: updateTime,
                              TaskNodes.q.endTime.fieldName: endTime,
                              TaskNodes.q.maxAttempt.fieldName: str(element.maxAttempt)
                              }
                    conn.query(conn.sqlrepr(Update(TaskNodes.q, values=fields, where=(TaskNodes.q.id == element.id))))
                    conn.cache.clear()

            # /////////////// Handling of the FolderNode
            elif isinstance(element, FolderNode):
                if element.id:
                    conn = FolderNodes._connection
                    fields = {FolderNodes.q.startTime.fieldName: startTime,
                              FolderNodes.q.updateTime.fieldName: updateTime,
                              FolderNodes.q.endTime.fieldName: endTime}
                    conn.query(conn.sqlrepr(Update(FolderNodes.q, values=fields, where=(FolderNodes.q.id == element.id))))
                    conn.cache.clear()

            # /////////////// Handling of the Pool
            elif isinstance(element, Pool):
                # TODO use sqlbuilder
                if element.id:
                    dbPool = Pools.get(element.id)
                    oldids = [rn.id for rn in dbPool.renderNodes]
                    newids = [rn.id for rn in element.renderNodes]
                    for rn in dbPool.renderNodes:
                        if rn.id not in newids:
                            dbPool.removeRenderNodes(rn)  # pylint: disable-msg=E1103
                    for rn in element.renderNodes:
                        if rn.id not in oldids:
                            dbPool.addRenderNodes(rn)  # pylint: disable-msg=E1103

            # /////////////// Handling of the RenderNode
            elif isinstance(element, RenderNode):
                if element.id:
                    conn = RenderNodes._connection
                    # fields = {RenderNodes.q.speed.fieldName: element.speed,
                              # RenderNodes.q.coresNumber.fieldName: element.coresNumber,
                              # RenderNodes.q.ramSize.fieldName: element.ramSize}
                              # RenderNodes.q.caracteristics.fieldName: json.dumps(element.caracteristics),
                    fields = {RenderNodes.q.performance.fieldName: element.performance}
                    conn.query(conn.sqlrepr(Update(RenderNodes.q, values=fields, where=(RenderNodes.q.id == element.id))))
                    conn.cache.clear()

            # /////////////// Handling of the Task
            elif isinstance(element, Task):
                # Simply update "tags" field to preserve comments in DB
                # The model listener will only register this elem when "tags" field is updated
                if element.id:
                    conn = Tasks._connection
                    fields = {Tasks.q.tags.fieldName: json.dumps(element.tags),
                              Tasks.q.maxAttempt.fieldName: str(element.maxAttempt)}
                    conn.query(conn.sqlrepr(Update(Tasks.q, values=fields, where=(Tasks.q.id == element.id))))
                    conn.cache.clear()

            # /////////////// Handling of the TaskGroup
            elif isinstance(element, TaskGroup):
                # Simply update "tags" field to preserve comments in DB
                # The model listener will only register this elem when "tags" field is updated
                if element.id:
                    conn = TaskGroups._connection
                    fields = {TaskGroups.q.tags.fieldName: json.dumps(element.tags)}
                    conn.query(conn.sqlrepr(Update(TaskGroups.q, values=fields, where=(TaskGroups.q.id == element.id))))
                    conn.cache.clear()

    ## Mark the provided elements as archived.
    # @param elements the elements to archive
    #
    def archiveElements(self, elements):
        if not len(elements):
            return
        tasksList = []
        taskgroupsList = []
        commandsList = []
        taskNodesList = []
        folderNodesList = []
        poolsList = []
        poolsharesList = []
        rendernodesList = []
        for element in elements:
            if isinstance(element, Task):
                tasksList.append(element.id)
            elif isinstance(element, TaskGroup):
                taskgroupsList.append(element.id)
            elif isinstance(element, Command):
                commandsList.append(element.id)
                # import gc
                # gc.collect()
                # LOGGER.warning("referrers of %s : %s" % (str(type(element)), str(gc.get_referrers(element))))
            elif isinstance(element, TaskNode):
                # LOGGER.info("            ----> Archiving task node = %s" % element )
                taskNodesList.append(element.id)
            elif isinstance(element, FolderNode):
                # LOGGER.info("            ----> Archiving folder node = %s" % element )
                folderNodesList.append(element.id)
            elif isinstance(element, Pool):
                poolsList.append(element.id)
            elif isinstance(element, PoolShare):
                poolsharesList.append(element.id)
            elif isinstance(element, RenderNode):
                rendernodesList.append(element.id)
        # del elements

        # /////////////// Handling of the Tasks
        if len(tasksList):
            conn = Tasks._connection
            sqlrepr = conn.sqlrepr(Update(Tasks.q, values={Tasks.q.archived.fieldName: True}, where=IN(Tasks.q.id, tasksList)))
            conn.query(sqlrepr)
            conn.cache.clear()
        # /////////////// Handling of the TaskGroups
        if len(taskgroupsList):
            conn = TaskGroups._connection
            conn.query(conn.sqlrepr(Update(TaskGroups.q, values={TaskGroups.q.archived.fieldName: True}, where=IN(TaskGroups.q.id, taskgroupsList))))
            conn.cache.clear()
        # /////////////// Handling of the Commands
        if len(commandsList):
            conn = Commands._connection
            conn.query(conn.sqlrepr(Update(Commands.q, values={Commands.q.archived.fieldName: True}, where=IN(Commands.q.id, commandsList))))
            conn.cache.clear()
        # /////////////// Handling of the TaskNodes
        if len(taskNodesList):
            conn = TaskNodes._connection
            sqlrepr = conn.sqlrepr(Update(TaskNodes.q, values={TaskNodes.q.archived.fieldName: True}, where=IN(TaskNodes.q.id, taskNodesList)))
            conn.query(sqlrepr)
            conn.cache.clear()
            conn = PoolShares._connection
            conn.query(conn.sqlrepr(Update(PoolShares.q, values={PoolShares.q.archived.fieldName: True}, where=IN(PoolShares.q.nodeId, taskNodesList))))
            conn.cache.clear()
        # /////////////// Handling of the FolderNodes
        if len(folderNodesList):
            conn = FolderNodes._connection
            conn.query(conn.sqlrepr(Update(FolderNodes.q, values={FolderNodes.q.archived.fieldName: True}, where=IN(FolderNodes.q.id, folderNodesList))))
            conn.cache.clear()
            conn = PoolShares._connection
            conn.query(conn.sqlrepr(Update(PoolShares.q, values={PoolShares.q.archived.fieldName: True}, where=IN(PoolShares.q.nodeId, folderNodesList))))
            conn.cache.clear()
        # /////////////// Handling of the Pools
        if len(poolsList):
            conn = Pools._connection
            conn.query(conn.sqlrepr(Update(Pools.q, values={Pools.q.archived.fieldName: True}, where=IN(Pools.q.id, poolsList))))
            conn.cache.clear()
            conn = PoolShares._connection
            conn.query(conn.sqlrepr(Update(PoolShares.q, values={PoolShares.q.archived.fieldName: True}, where=IN(PoolShares.q.poolId, poolsList))))
            conn.cache.clear()
        # /////////////// Handling of the PoolShares
        if len(poolsharesList):
            conn = PoolShares._connection
            sqlrepr = conn.sqlrepr(Update(PoolShares.q, values={PoolShares.q.archived.fieldName: True}, where=IN(PoolShares.q.id, poolsharesList)))
            conn.query(sqlrepr)
            conn.cache.clear()
        # /////////////// Handling of the RenderNodes
        if len(rendernodesList):
            conn = RenderNodes._connection
            conn.query(conn.sqlrepr(Delete(RenderNodes.q, where=IN(RenderNodes.q.id, rendernodesList))))
            conn.cache.clear()

    def getDateFromTimeStamp(self, timeStamp):
        return datetime.datetime.fromtimestamp(timeStamp) if timeStamp else None

    def getTimeStampFromDate(self, date):
        return time.mktime(date.timetuple()) if date else None

    ## Restores the state of the dispatcher from the database.
    # @var tree the DispatchTree instance.
    #
    def restoreStateFromDb(self, tree, rnsAlreadyLoaded):
        begintime = time.time()
        refreshDelay = singletonconfig.get('DB', 'REFRESH_DELAY', default=5)

        # reload the pools and rns from the database
        LOGGER.warning("1/9 Reloading pools")
        prevTimer = time.time()
        tmpTimer = prevTimer

        if not rnsAlreadyLoaded:
            ### recreate the pools
            poolsById = {}
            poolConn = Pools._connection
            poolFields = [Pools.q.id,
                          Pools.q.name]
            pools = poolConn.queryAll(poolConn.sqlrepr(Select(poolFields, where=(Pools.q.archived == False))))
            for num, dbPool in enumerate(pools):
                id, name = dbPool
                realPool = Pool(id=id,
                                name=name)
                poolsById[id] = realPool

            ### recreate the rendernodes
            rnById = {}
            conn = RenderNodes._connection
            fields = [RenderNodes.q.id,
                      RenderNodes.q.name,
                      RenderNodes.q.coresNumber,
                      RenderNodes.q.speed,
                      RenderNodes.q.ip,
                      RenderNodes.q.port,
                      RenderNodes.q.ramSize,
                      RenderNodes.q.caracteristics,
                      RenderNodes.q.performance]
            renderNodes = conn.queryAll(conn.sqlrepr(Select(fields)))

            LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
            nbElems = len(renderNodes)
            LOGGER.warning("  - creating %d elems" % nbElems)

            for num, dbRenderNode in enumerate(renderNodes):
                id, name, coresNumber, speed, ip, port, ramSize, caracteristics, performance = dbRenderNode
                realRenderNode = RenderNode(id,
                                            name,
                                            coresNumber,
                                            speed,
                                            ip,
                                            port,
                                            ramSize,
                                            json.loads(caracteristics),
                                            performance)
                # get the pools of the rendernode
                prn = Table('pools_render_nodes')
                join = INNERJOINOn(None, Pools, Pools.q.id == prn.pools_id)
                dbPools = poolConn.queryAll(poolConn.sqlrepr(Select(poolFields, join=join, where=AND(prn.render_nodes_id == id, Pools.q.archived == False))))
                for idx, dbPool in enumerate(dbPools):
                    id, name = dbPool
                    poolsById[id].renderNodes.append(realRenderNode)
                    realRenderNode.pools.append(poolsById[id])
                tree.renderNodes[str(realRenderNode.name)] = realRenderNode
                rnById[realRenderNode.id] = realRenderNode

                if (time.time() - tmpTimer) > refreshDelay:
                    tmpTimer = time.time()
                    LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))

            # add the pools to the dispatch tree
            for pool in poolsById.values():
                tree.pools[pool.name] = pool
        else:
            # pools and rns have already been processed, either from a file or a webservice
            poolsById = {}
            rnById = {}
            for pool in tree.pools.values():
                poolsById[pool.id] = pool
            for rn in tree.renderNodes.values():
                rnById[rn.id] = rn

        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        # print "%s -- rendernodes complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))

        ####### recreate the folder nodes with the correct ids
        LOGGER.warning("2/9 Reloading folder nodes")
        prevTimer = time.time()
        tmpTimer = prevTimer

        nodesById = {}
        conn = FolderNodes._connection
        fields = [FolderNodes.q.id,
                  FolderNodes.q.name,
                  FolderNodes.q.parentId,
                  FolderNodes.q.user,
                  FolderNodes.q.priority,
                  FolderNodes.q.dispatchKey,
                  FolderNodes.q.maxRN,
                  FolderNodes.q.taskGroupId,
                  FolderNodes.q.strategy,
                  FolderNodes.q.creationTime,
                  FolderNodes.q.startTime,
                  FolderNodes.q.updateTime,
                  FolderNodes.q.endTime,
                  FolderNodes.q.archived]
        folderNodes = conn.queryAll(conn.sqlrepr(Select(fields, where=(FolderNodes.q.archived == False))))

        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(folderNodes)
        LOGGER.warning("  - creating %d elems" % nbElems)
        for num, dbFolderNode in enumerate(folderNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskGroupId, strategy, creationTime, startTime, updateTime, endTime, archived = dbFolderNode
            realFolder = FolderNode(id,
                                    name,
                                    None,
                                    user,
                                    priority,
                                    dispatchKey,
                                    maxRN,
                                    createStrategyInstance(strategy),
                                    self.getTimeStampFromDate(creationTime),
                                    self.getTimeStampFromDate(startTime),
                                    self.getTimeStampFromDate(updateTime),
                                    self.getTimeStampFromDate(endTime))
            nodesById[realFolder.id] = realFolder

            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        # print "%s -- foldernodes complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))

        ### recreate the task nodes with the correct ids
        LOGGER.warning("3/9 Reloading task nodes")
        prevTimer = time.time()
        tmpTimer = prevTimer

        conn = TaskNodes._connection
        fields = [TaskNodes.q.id,
                  TaskNodes.q.name,
                  TaskNodes.q.parentId,
                  TaskNodes.q.user,
                  TaskNodes.q.priority,
                  TaskNodes.q.dispatchKey,
                  TaskNodes.q.maxRN,
                  TaskNodes.q.taskId,
                  TaskNodes.q.creationTime,
                  TaskNodes.q.startTime,
                  TaskNodes.q.updateTime,
                  TaskNodes.q.endTime,
                  TaskNodes.q.archived,
                  TaskNodes.q.maxAttempt]
        taskNodes = conn.queryAll(conn.sqlrepr(Select(fields, where=(TaskNodes.q.archived == False))))

        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(taskNodes)
        LOGGER.warning("  - creating %d elems" % nbElems)
        for num, dbTaskNode in enumerate(taskNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskId, creationTime, startTime, updateTime, endTime, archived, maxAttempt = dbTaskNode
            realTaskNode = TaskNode(id,
                                    name,
                                    None,
                                    user,
                                    priority,
                                    dispatchKey,
                                    maxRN,
                                    None,
                                    self.getTimeStampFromDate(creationTime),
                                    self.getTimeStampFromDate(startTime),
                                    self.getTimeStampFromDate(updateTime),
                                    self.getTimeStampFromDate(endTime),
                                    maxAttempt=maxAttempt)
            nodesById[realTaskNode.id] = realTaskNode
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        ###### additional loops for nodes
        LOGGER.warning("4/9 Reparenting folder and task nodes")

        nbElems = len(folderNodes)
        prevTimer = time.time()
        tmpTimer = prevTimer

        LOGGER.warning("  - parsing %d folder nodes" % nbElems)
        for num, dbFolderNode in enumerate(folderNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskGroupId, strategy, creationTime, startTime, updateTime, endTime, archived = dbFolderNode
            # recreate the parents of the folder nodes
            if parentId == 0:
                nodesById[id].setParentValue(tree.nodes[0])
            elif parentId:
                nodesById[id].setParentValue(nodesById[parentId])
            ### add the dependencies between the nodes
            # TODO to be tested
            conn = Dependencies._connection
            fields = [Dependencies.q.toNodeId,
                      Dependencies.q.statusList]
            dependencies = conn.queryAll(conn.sqlrepr(Select(fields, where=(Dependencies.q.folderNodes == id))))
            for idx, dbDependency in enumerate(dependencies):
                toNodeId, statusList = dbDependency
                statusIntList = [int(i) for i in statusList.split(",")]
                #FIXME temp
                # nodesById[id].addDependency(nodesById[toNodeId], statusIntList)
                if toNodeId in nodesById.keys():
                    nodesById[id].addDependency(nodesById[toNodeId], statusIntList)

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100 * num / float(nbElems - 1)))

        nbElems = len(taskNodes)
        LOGGER.warning("  - parsing %d task nodes" % nbElems)
        for num, dbTaskNode in enumerate(taskNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskId, creationTime, startTime, updateTime, endTime, archived, maxAttempt = dbTaskNode
            # recreate the parents of the task nodes
            if parentId == 0:
                nodesById[id].setParentValue(tree.nodes[0])
            elif parentId in nodesById:
                nodesById[id].setParentValue(nodesById[parentId])
                ### add the dependencies between the nodes
                conn = Dependencies._connection
                fields = [Dependencies.q.toNodeId,
                          Dependencies.q.statusList]
                dependencies = conn.queryAll(conn.sqlrepr(Select(fields, where=(Dependencies.q.taskNodes == id))))
                for idx, dbDependency in enumerate(dependencies):
                    toNodeId, statusList = dbDependency
                    statusIntList = [int(i) for i in statusList.split(",")]
                    #FIXME temp
                    if toNodeId in nodesById.keys():
                        nodesById[id].addDependency(nodesById[toNodeId], statusIntList)

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        # print "%s -- add loop complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))

        ### recreate the poolShares
        LOGGER.warning("5/9 Reloading pool shares")
        prevTimer = time.time()
        tmpTimer = prevTimer

        conn = PoolShares._connection
        fields = [PoolShares.q.id,
                  PoolShares.q.poolId,
                  PoolShares.q.nodeId,
                  PoolShares.q.maxRN,
                  PoolShares.q.archived]
        poolShares = conn.queryAll(conn.sqlrepr(Select(fields, where=(PoolShares.q.archived == False))))

        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(poolShares)
        LOGGER.warning("  - creating %d elems" % nbElems)

        for num, dbPoolShare in enumerate(poolShares):
            id, poolId, nodeId, maxRN, archived = dbPoolShare
            #FIXME temp
            if nodeId in nodesById.keys():
                realPoolShare = PoolShare(id,
                                          poolsById[poolId],
                                          nodesById[nodeId],
                                          maxRN)
            tree.poolShares[realPoolShare.id] = realPoolShare
            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        ### recreate the commands
        LOGGER.warning("6/9 Reloading commands (use 'mysqladmin -u root processlist' to check mysql workload)")
        prevTimer = time.time()
        tmpTimer = prevTimer

        cmdTaskIdList = defaultdict(list)
        cmdDict = {}
        conn = Commands._connection
        fields = [Commands.q.id,
                  Commands.q.description,
                  Commands.q.taskId,
                  Commands.q.status,
                  Commands.q.completion,
                  Commands.q.creationTime,
                  Commands.q.startTime,
                  Commands.q.updateTime,
                  Commands.q.endTime,
                  Commands.q.assignedRNId,
                  Commands.q.message,
                  Commands.q.stats,
                  Commands.q.archived,
                  Commands.q.args,
                  Commands.q.attempt,
                  Commands.q.runnerPackages,
                  Commands.q.watcherPackages
                  ]
        commands = conn.queryAll(conn.sqlrepr(Select(fields, where=(Commands.q.archived == False))))

        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(commands)
        LOGGER.warning("  - creating %d elems" % nbElems)

        for num, dbCmd in enumerate(commands):
            id, description, taskId, status, completion, creationTime, startTime, updateTime, endTime, assignedRNId, message, stats, archived, args, attempt, runnerPackages, watcherPackages = dbCmd
            if args is None:
                args = "{}"
            if stats is None:
                stats = "{}"

            realCmd = Command(id,
                              description,
                              None,
                              eval(args),
                              status,
                              completion,
                              rnById.get(assignedRNId, None),
                              self.getTimeStampFromDate(creationTime),
                              self.getTimeStampFromDate(startTime),
                              self.getTimeStampFromDate(updateTime),
                              self.getTimeStampFromDate(endTime),
                              attempt=attempt,
                              stats=eval(stats),
                              message=message,
                              runnerPackages=json.loads(runnerPackages),
                              watcherPackages=json.loads(watcherPackages))
            if status in [2, 3, 4] and realCmd.renderNode is None:
                print "%s -- invalid status for command %d, setting to READY" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)), realCmd.id)
                realCmd.status = 1
                status = 1

            assert not(status in [2, 3, 4] and realCmd.renderNode is None)
            cmdTaskIdList[taskId].append(realCmd)
            cmdDict[realCmd.id] = realCmd

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        ### recreate the tasks
        LOGGER.warning("7/9 Reloading tasks")
        prevTimer = time.time()
        tmpTimer = prevTimer

        realTasksList = {}
        conn = Tasks._connection
        fields = [Tasks.q.id,
                  Tasks.q.name,
                  Tasks.q.parentId,
                  Tasks.q.user,
                  Tasks.q.priority,
                  Tasks.q.dispatchKey,
                  Tasks.q.maxRN,
                  Tasks.q.runner,
                  Tasks.q.environment,
                  Tasks.q.requirements,
                  Tasks.q.minNbCores,
                  Tasks.q.maxNbCores,
                  Tasks.q.ramUse,
                  Tasks.q.licence,
                  Tasks.q.tags,
                  Tasks.q.validationExpression,
                  Tasks.q.archived,
                  Tasks.q.args,
                  Tasks.q.maxAttempt,
                  Tasks.q.runnerPackages,
                  Tasks.q.watcherPackages
                  ]

        tasks = conn.queryAll(conn.sqlrepr(Select(
                  fields, where=(
                      IN(Tasks.q.id, Select( TaskNodes.q.taskId, where=(TaskNodes.q.archived == False)))
                    )
                )))

        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(tasks)
        LOGGER.warning("  - creating %d elems" % nbElems)

        for num, dbTask in enumerate(tasks):
            id, name, parentId, user, priority, dispatchKey, maxRN, runner, environment, requirements, minNbCores, maxNbCores, ramUse, licence, tags, validationExpression, archived, args, maxAttempt, runnerPackages, watcherPackages = dbTask
            taskCmds = []
            if args is None:
                args = '{}'
            # get the commands associated to this task
            taskCmds = cmdTaskIdList[id]
            realTask = Task(id,
                            name,
                            None,
                            user,
                            maxRN,
                            priority,
                            dispatchKey,
                            runner,
                            eval(args),
                            validationExpression,
                            taskCmds,
                            json.loads(requirements),
                            minNbCores,
                            maxNbCores,
                            ramUse,
                            json.loads(environment),
                            {},
                            licence,
                            json.loads(tags),
                            maxAttempt=maxAttempt,
                            runnerPackages=json.loads(runnerPackages),
                            watcherPackages=json.loads(watcherPackages))
            tree.tasks[realTask.id] = realTask
            realTasksList[realTask.id] = realTask
            # set the task on the appropriate commands
            for cmd in taskCmds:
                cmd.task = realTask
                #cmd.computeAvgTimeByFrame()
                # update the command in the dispatch tree
                tree.commands[cmd.id] = cmd
                # if the command was last reported as running, reassign the rendernode in the model
                if cmd.status == 3:
                    cmd.renderNode.commands[cmd.id] = cmd
                    cmd.renderNode.reserveLicense(cmd, self.licenseManager)
                    cmd.renderNode.reserveRessources(cmd)

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        # print "%s -- tasks complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))

        ### recreate the taskGroups
        LOGGER.warning("8/9 Reloading task groups")
        prevTimer = time.time()
        tmpTimer = prevTimer

        realTaskGroupsList = {}
        conn = TaskGroups._connection
        fields = [TaskGroups.q.id,
                  TaskGroups.q.name,
                  TaskGroups.q.parentId,
                  TaskGroups.q.user,
                  TaskGroups.q.priority,
                  TaskGroups.q.dispatchKey,
                  TaskGroups.q.maxRN,
                  TaskGroups.q.environment,
                  TaskGroups.q.requirements,
                  TaskGroups.q.tags,
                  TaskGroups.q.strategy,
                  TaskGroups.q.archived,
                  TaskGroups.q.args]
        taskGroups = conn.queryAll(conn.sqlrepr(Select(fields, where=(
                      IN( TaskGroups.q.id, Select( FolderNodes.q.taskGroupId, where=(FolderNodes.q.archived == False) ) )
                      ))))
        LOGGER.warning("  - sql query executed in %.3f s " % ((time.time()-prevTimer)))
        nbElems = len(taskGroups)
        LOGGER.warning("  - creating %d elems" % nbElems)

        for num, dbTaskGroup in enumerate(taskGroups):
            id, name, parentId, user, priority, dispatcherKey, maxRN, environment, requirements, tags, strategy, archived, args = dbTaskGroup
            if args is None:
                args = '{}'
            realTaskGroup = TaskGroup(id,
                                      name,
                                      None,
                                      user,
                                      eval(args),
                                      json.loads(environment),
                                      json.loads(requirements),
                                      maxRN,
                                      priority,
                                      dispatchKey,
                                      createStrategyInstance(str(strategy)),
                                      {},
                                      json.loads(tags))
            realTaskGroupsList[realTaskGroup.id] = realTaskGroup

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))

        # set the parents of the taskGroups
        nbElems = len(taskGroups)
        LOGGER.warning("  - parsing %d task groups" % nbElems)

        for num, dbTaskGroup in enumerate(taskGroups):
            id, name, parentId, user, priority, dispatcherKey, maxRN, environment, requirements, tags, strategy, archived, args = dbTaskGroup
            if parentId:
                #FIXME: try to avoid pb when reloading DB with inconsistencies
                if int(parentId) in realTaskGroupsList.keys():
                    realTaskGroupsList[int(parentId)].addTask(realTaskGroupsList[int(id)])
                    realTaskGroupsList[int(id)].parent = realTaskGroupsList[int(parentId)]

            tree.tasks[int(id)] = realTaskGroupsList[int(id)]

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))

        # set the parents of the tasks
        nbElems = len(tasks)
        LOGGER.warning("  - parsing %d tasks" % nbElems)
        for num, dbTask in enumerate(tasks):
            id, name, parentId, user, priority, dispatchKey, maxRN, runner, environment, requirements, minNbCores, maxNbCores, ramUse, licence, tags, validationExpression, archived, args, maxAttempt, runnerPackages, watcherPackages = dbTask
            if parentId:
                #FIXME temp
                if int(parentId) in realTaskGroupsList.keys():
                    realTaskGroupsList[int(parentId)].addTask(realTasksList[int(id)])
                    realTasksList[int(id)].parent = realTaskGroupsList[int(parentId)]
            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))

        # print "%s -- taskgroups complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))

        ### affect the task objects to the corresponding TaskNodes
        nbElems = len(taskNodes)
        LOGGER.warning("  - affecting task to %d task nodes" % nbElems)
        for num, dbTaskNode in enumerate(taskNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskId, creationTime, startTime, updateTime, endTime, archived, maxAttempt = dbTaskNode
            # set the real task
            dbTaskNodeId = int(id)
            if dbTaskNodeId in nodesById.keys() and int(taskId) in realTasksList.keys():
                nodesById[dbTaskNodeId].task = realTasksList[int(taskId)]
                # get the correct task in the dispatchtree and append the node to the dict of nodes
                tree.tasks[nodesById[dbTaskNodeId].task.id].nodes["graph_rule"] = nodesById[dbTaskNodeId]
                tree.nodes[dbTaskNodeId] = nodesById[dbTaskNodeId]

            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))

        # print "%s -- affect task complete --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)))
        ### affect the taskGroup objects to the corresponding FolderNodes
        nbElems = len(folderNodes)
        LOGGER.warning("  - affecting task groups to %d folder nodes" % nbElems)
        for num, dbFolderNode in enumerate(folderNodes):
            id, name, parentId, user, priority, dispatchKey, maxRN, taskGroupId, strategy, creationTime, startTime, updateTime, endTime, archived = dbFolderNode
            if taskGroupId:
                tgId = int(taskGroupId)
                #FIXME temp
                if tgId in tree.tasks.keys():
                    nodesById[int(id)].taskGroup = tree.tasks[tgId]
                    tree.tasks[tgId].nodes["graph_rule"] = nodesById[int(id)]
            tree.nodes[int(id)] = nodesById[int(id)]
            # Log progress info
            if (time.time() - tmpTimer) > refreshDelay:
                tmpTimer = time.time()
                LOGGER.warning("    - progress: %d%%" % (100*num/float(nbElems-1)))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(prevTimer))

        # calculate the average time by frame
        LOGGER.warning("9/9 Computing avg time and max ids")
        startTimer = time.time()
        prevTimer = time.time()

        for cmd in tree.commands.values():
            cmd.computeAvgTimeByFrame()

        LOGGER.warning("  - Average time by frame recomputed in %.3f s" % (time.time()-prevTimer))

        ### calculate the correct max ids for all elements, get them from db in case of archived elements that would not appear in the dispatchtree
        prevTimer = time.time()
        try:
            tree.nodeMaxId = int(max([FolderNodes.select().max(FolderNodes.q.id), TaskNodes.select().max(TaskNodes.q.id)]))
        except:
            tree.nodeMaxId = 0
        LOGGER.warning("  - Set max id for nodes in %.3f s" % (time.time()-prevTimer))

        prevTimer = time.time()
        try:
            tree.poolMaxId = int(Pools.select().max(Pools.q.id))
        except:
            tree.poolMaxId = 0
        LOGGER.warning("  - Set max id for pools in %.3f s" % (time.time()-prevTimer))

        prevTimer = time.time()
        try:
            tree.renderNodeMaxId = int(RenderNodes.select().max(RenderNodes.q.id))
        except:
            tree.renderNodeMaxId = 0
        LOGGER.warning("  - Set max id for render nodes in %.3f s" % (time.time()-prevTimer))

        prevTimer = time.time()
        try:
            tree.taskMaxId = int(Tasks.select().max(Tasks.q.id))
        except:
            tree.taskMaxId = 0
        LOGGER.warning("  - Set max id for tasks in %.3f s" % (time.time()-prevTimer))

        prevTimer = time.time()
        try:
            tree.commandMaxId = int(Commands.select().max(Commands.q.id))
        except:
            tree.commandMaxId = 0
        LOGGER.warning("  - Set max id for commands in %.3f s" % (time.time()-prevTimer))

        prevTimer = time.time()
        try:
            tree.poolShareMaxId = int(PoolShares.select().max(PoolShares.q.id))
        except:
            tree.poolShareMaxId = 0
        LOGGER.warning("  - Set max id for pool shares in %.3f s" % (time.time()-prevTimer))
        LOGGER.warning("  - elapsed time %s" % elapsedTimeToString(startTimer))

        tree.toCreateElements = []
