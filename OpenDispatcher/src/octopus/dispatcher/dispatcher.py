
from __future__ import with_statement

import logging
import socket
import time
from Queue import Queue
from itertools import groupby
import collections
import json

from octopus.core.threadpool import ThreadPool, makeRequests, NoResultsPending
from octopus.dispatcher.model import (DispatchTree, FolderNode, RenderNode,
                                      Pool, PoolShare, enums)
from octopus.dispatcher.strategies import FifoStrategy
from octopus.core.framework import MainLoopApplication
from octopus.dispatcher.db.pulidb import PuliDB
from octopus.dispatcher import settings
from octopus.dispatcher.poolman.filepoolman import FilePoolManager
from octopus.dispatcher.poolman.wspoolman import WebServicePoolManager
from octopus.dispatcher.licenses.licensemanager import LicenseManager
from octopus.dispatcher.model.enums import *

LOGGER = logging.getLogger('dispatcher')
ASSIGNMENT_COMPUTATION_TIMEOUT = 0.5


class Dispatcher(MainLoopApplication):
    '''The Dispatcher class is the core of the dispatcher application.

    It computes the assignments of commands to workers according to a
    DispatchTree and handles all the communications with the workers and
    clients.
    '''

    instance = None
    init = False

    def __new__(cls, framework):
        if cls.instance is None:
            cls.instance = super(Dispatcher, cls).__new__(cls, framework)
        return cls.instance

    def __init__(self, framework):
        if self.init:
            return
        self.init = True
        self.nextCycle = time.time()

        MainLoopApplication.__init__(self, framework)

        self.threadPool = ThreadPool(16, 0, 0, None)

        LOGGER.info('settings.DEBUG = %s', settings.DEBUG)
        LOGGER.info('settings.ADDRESS = %s', settings.ADDRESS)
        LOGGER.info('settings.PORT = %s', settings.PORT)

        self.cycle = 1
        self.dispatchTree = DispatchTree()
        self.licenseManager = LicenseManager()
        self.enablePuliDB = settings.DB_ENABLE
        self.cleanDB = settings.DB_CLEAN_DATA

        self.pulidb = None
        if self.enablePuliDB:
            self.pulidb = PuliDB(self.cleanDB, self.licenseManager)
        self.dispatchTree.registerModelListeners()
        rnsAlreadyInitialized = self.initPoolsDataFromBackend()
        if self.enablePuliDB and not self.cleanDB:
            LOGGER.info("reloading jobs from database")
            beginTime = time.time()
            self.pulidb.restoreStateFromDb(self.dispatchTree, rnsAlreadyInitialized)
            LOGGER.info("reloading took %s" % str(time.time() - beginTime))
            LOGGER.info("done reloading jobs from database")
            LOGGER.info("reloaded %d tasks" % len(self.dispatchTree.tasks))
        LOGGER.info("checking dispatcher state")
        self.dispatchTree.updateCompletionAndStatus()
        self.updateRenderNodes()
        self.dispatchTree.validateDependencies()
        if self.enablePuliDB and not self.cleanDB:
            self.dispatchTree.toModifyElements = []
        self.defaultPool = self.dispatchTree.pools['default']
        LOGGER.info("loading dispatch rules")
        self.loadRules()
        # it should be better to have a maxsize
        self.queue = Queue(maxsize=10000)
        if settings.DUMP_HTML_DATA:
            self.dumpToHTML()

    def initPoolsDataFromBackend(self):
        '''Loads pools and workers from appropriate backend.
        '''
        try:
            if settings.POOLS_BACKEND_TYPE == "file":
                manager = FilePoolManager()
            elif settings.POOLS_BACKEND_TYPE == "ws":
                manager = WebServicePoolManager()
            elif settings.POOLS_BACKEND_TYPE == "db":
                return False
        except Exception:
            return False

        computers = manager.listComputers()

        ### recreate the pools
        poolsList = manager.listPools()
        poolsById = {}
        for poolDesc in poolsList:
            pool = Pool(id=int(poolDesc.id), name=str(poolDesc.name))
            self.dispatchTree.toCreateElements.append(pool)
            poolsById[pool.id] = pool

        ### recreate the rendernodes
        rnById = {}
        for computerDesc in computers:
            try:
                computerDesc.name = socket.getfqdn(computerDesc.name)
                ip = socket.gethostbyname(computerDesc.name)
            except socket.gaierror:
                continue
            renderNode = RenderNode(computerDesc.id, computerDesc.name + ":" + str(computerDesc.port), computerDesc.cpucount * computerDesc.cpucores, computerDesc.cpufreq, ip, computerDesc.port, computerDesc.ramsize, json.loads(computerDesc.properties))
            self.dispatchTree.toCreateElements.append(renderNode)
            ## add the rendernodes to the pools
            for pool in computerDesc.pools:
                poolsById[pool.id].renderNodes.append(renderNode)
                renderNode.pools.append(poolsById[pool.id])
            self.dispatchTree.renderNodes[str(renderNode.name)] = renderNode
            rnById[renderNode.id] = renderNode

        # add the pools to the dispatch tree
        for pool in poolsById.values():
            self.dispatchTree.pools[pool.name] = pool
        if self.cleanDB or not self.enablePuliDB:
            graphs = FolderNode(1, "graphs", self.dispatchTree.root, "root", 0, 0, 0, FifoStrategy())
            self.dispatchTree.toCreateElements.append(graphs)
            self.dispatchTree.nodes[graphs.id] = graphs
            ps = PoolShare(1, self.dispatchTree.pools["default"], graphs, PoolShare.UNBOUND)
            self.dispatchTree.toCreateElements.append(ps)
        if self.enablePuliDB:
            # clean the tables pools and rendernodes (overwrite)
            self.pulidb.dropPoolsAndRnsTables()
            self.pulidb.createElements(self.dispatchTree.toCreateElements)
            self.dispatchTree.resetDbElements()

        return True

    def loadRules(self):
        from .rules.graphview import GraphViewBuilder
        graphs = self.dispatchTree.findNodeByPath("/graphs", None)
        if graphs is None:
            LOGGER.fatal("No /graphs node, impossible to load rule for /graphs.")
            self.stop()
        self.dispatchTree.rules.append(GraphViewBuilder(self.dispatchTree, graphs))

        from .rules.userview import UserView
        if self.cleanDB or not self.enablePuliDB:
            userview = UserView.register(self.dispatchTree, "root", "users")
#            self.dispatchTree.toCreateElements.append(userview.root)
            self.dispatchTree.nodes[userview.root.id] = userview.root
        else:
            for node in self.dispatchTree.root.children:
                if node.name == "users":
                    root = node
                    break
            else:
                raise RuntimeError("missing root node for UserView")
            userview = UserView(self.dispatchTree, root)

    def prepare(self):
        pass

    def stop(self):
        '''Stops the application part of the dispatcher.'''
        #self.httpRequester.stopAll()
        pass

    @property
    def modified(self):
        return bool(self.dispatchTree.toArchiveElements or
                    self.dispatchTree.toCreateElements or
                    self.dispatchTree.toModifyElements)

    def mainLoop(self):
        '''Dispatcher main loop iteration.'''
        try:
            self.threadPool.poll()
        except NoResultsPending:
            pass
        else:
            LOGGER.info("finished some network requests")

        self.cycle += 1
        self.dispatchTree.updateCompletionAndStatus()
        self.updateRenderNodes()

        self.dispatchTree.validateDependencies()

        executedRequests = []
        first = True
        while first or not self.queue.empty():
            workload = self.queue.get()
            workload()
            executedRequests.append(workload)
            first = False

        # update db
        self.updateDB()

        # compute and send command assignments to rendernodes
        assignments = self.computeAssignments()
        self.sendAssignments(assignments)

        # call the release finishing status on all rendernodes
        for renderNode in self.dispatchTree.renderNodes.values():
            renderNode.releaseFinishingStatus()

        for workload in executedRequests:
            workload.submit()

    def updateDB(self):
        if settings.DB_ENABLE:
            self.pulidb.createElements(self.dispatchTree.toCreateElements)
            self.pulidb.updateElements(self.dispatchTree.toModifyElements)
            self.pulidb.archiveElements(self.dispatchTree.toArchiveElements)
        self.dispatchTree.resetDbElements()

    def computeAssignments(self):
        '''Computes and returns a list of (rendernode, command) assignments.'''
        from .model.node import NoRenderNodeAvailable
        # if no rendernodes available, return
        if not any(rn.isAvailable() for rn in self.dispatchTree.renderNodes.values()):
            return []
        assignments = []
        # first create a set of entrypoints that are not done nor cancelled nor blocked nor paused and that have at least one command ready
        entryPoints = set([poolShare.node for poolShare in self.dispatchTree.poolShares.values() if poolShare.node.status not in [NODE_BLOCKED, NODE_DONE, NODE_CANCELED, NODE_PAUSED] and poolShare.node.readyCommandCount > 0])
        # sort by pool for the groupby
        entryPoints = sorted(entryPoints, key=lambda node: node.poolShares.values()[0].pool)
        # don't proceed to the calculation if no rns availables in the requested pools
        rnsBool = False
        for pool, nodesiterator in groupby(entryPoints, lambda x: x.poolShares.values()[0].pool):
            rnsAvailables = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED, RN_WORKING]])
            if len(rnsAvailables):
                rnsBool = True
        if not rnsBool:
            return []

        # update the value of the maxrn for the poolshares (parallel dispatching)
        for pool, nodesiterator in groupby(entryPoints, lambda x: x.poolShares.values()[0].pool):
            # we are treating every active node of the pool
            nodesList = [node for node in nodesiterator]
            # the new maxRN value is calculated based on the number of active jobs of the pool, and the number of online rendernodes of the pool
            rnsNotOffline = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED]])
            rnsSize = len(rnsNotOffline)
            # if we have a userdefined maxRN for some nodes, remove them from the list and substracts their maxRN from the pool's size
            l = nodesList[:]  # duplicate the list to be safe when removing elements
            for node in l:
                if node.poolShares.values()[0].userDefinedMaxRN:
                    nodesList.remove(node)
                    rnsSize -= node.poolShares.values()[0].maxRN
            #LOGGER.warning("Pool %s has a size of %s rns and %s nodes" % (pool.name, str(rnsSize), str(len(nodesList))))
            if len(nodesList) == 0:
                break
            updatedmaxRN = rnsSize // len(nodesList)
            remainingRN = rnsSize % len(nodesList)
            # sort by id (fifo)
            nodesList = sorted(nodesList, key=lambda x: x.id)
            # then sort by dispatchKey (priority)
            nodesList = sorted(nodesList, key=lambda x: x.dispatchKey, reverse=True)
            for node in nodesList:
                if node.dispatchKey != 0:
                    node.poolShares.values()[0].maxRN = -1
                    continue
                node.poolShares.values()[0].maxRN = updatedmaxRN
                if remainingRN > 0:
                    node.poolShares.values()[0].maxRN += 1
                    remainingRN -= 1
                #LOGGER.warning("   Node %s has a maxrn of %s" % (node.name, str(node.poolShares.values()[0].maxRN)))

        # now, we are treating every nodes
        # sort by id (fifo)
        entryPoints = sorted(entryPoints, key=lambda node: node.id)
        # then sort by dispatchKey (priority)
        entryPoints = sorted(entryPoints, key=lambda node: node.dispatchKey, reverse=True)

        ###
        for entryPoint in entryPoints:
            if any([poolShare.hasRenderNodesAvailable() for poolShare in entryPoint.poolShares.values()]):
                try:
                    for (rn, com) in entryPoint.dispatchIterator(lambda: self.queue.qsize() > 0):
                        assignments.append((rn, com))
                        # increment the allocatedRN for the poolshare
                        poolShare.allocatedRN += 1
                        # save the active poolshare of the rendernode
                        rn.currentpoolshare = poolShare
                except NoRenderNodeAvailable:
                    pass
        assignmentDict = collections.defaultdict(list)
        for (rn, com) in assignments:
            assignmentDict[rn].append(com)
        return assignmentDict.items()

    def updateRenderNodes(self):
        for rendernode in self.dispatchTree.renderNodes.values():
            rendernode.updateStatus()

    def sendAssignments(self, assignmentList):
        '''Processes a list of (rendernode, command) assignments.'''

        def sendAssignment(args):
            rendernode, commands = args
            failures = []
            for command in commands:
                headers = {}
                if not rendernode.idInformed:
                    headers["rnId"] = rendernode.id
                root = command.task
                ancestors = [root]
                while root.parent:
                    root = root.parent
                    ancestors.append(root)
                arguments = {}
                environment = {
                    'PULI_USER': command.task.user,
                    'PULI_ALLOCATED_MEMORY': unicode(rendernode.usedRam[command.id]),
                    'PULI_ALLOCATED_CORES': unicode(rendernode.usedCoresNumber[command.id]),
                }
                for ancestor in ancestors:
                    arguments.update(ancestor.arguments)
                    environment.update(ancestor.environment)
                arguments.update(command.arguments)
                commandDict = {
                    "id": command.id,
                    "runner": str(command.task.runner),
                    "arguments": arguments,
                    "validationExpression": command.task.validationExpression,
                    "taskName": command.task.name,
                    "relativePathToLogDir": "%d" % command.task.id,
                    "environment": environment,
                }
                body = json.dumps(commandDict)
                headers["Content-Length"] = len(body)
                headers["Content-Type"] = "application/json"

                try:
                    resp, data = rendernode.request("POST", "/commands/", body, headers)
                    if not resp.status == 202:
                        LOGGER.error("Assignment request failed: command %d on worker %s", command.id, rendernode.name)
                        failures.append((rendernode, command))
                    else:
                        LOGGER.info("Sent assignment of command %d to worker %s", command.id, rendernode.name)
                except rendernode.RequestFailed, e:
                    LOGGER.exception("Assignment of command %d to worker %s failed: %r", command.id, rendernode.name, e)
                    failures.append((rendernode, command))
            return failures

        requests = makeRequests(sendAssignment, [[a, b] for (a, b) in assignmentList], self._assignmentFailed)
        for request in requests:
            self.threadPool.putRequest(request)

    def _assignmentFailed(self, request, failures):
        for assignment in failures:
            rendernode, command = assignment
            rendernode.clearAssignment(command)
            command.clearAssignment()

    def handleNewGraphRequestApply(self, graph):
        '''Handles a graph submission request and closes the given ticket
        according to the result of the process.
        '''
        nodes = self.dispatchTree.registerNewGraph(graph)
        # handles the case of post job with paused status
        for node in nodes:
            try:
                if node.tags['paused'] == 'true':
                    node.setPaused(True)
            except KeyError:
                continue
        LOGGER.info('Added graph "%s" to the model.' % graph['name'])
        return nodes

    def updateCommandApply(self, dct):
        commandId = dct['id']
        renderNodeName = dct['renderNodeId']

        try:
            command = self.dispatchTree.commands[commandId]
        except KeyError:
            raise KeyError("Command not found: %d" % commandId)

        if not command.renderNode or command.renderNode.name != renderNodeName:
            raise KeyError("Command %d is not running on rendernode %s" % (commandId, renderNodeName))

        rn = command.renderNode
        rn.lastAliveTime = max(time.time(), rn.lastAliveTime)

        if "status" in dct:
            command.status = int(dct['status'])

        if "completion" in dct and command.status == enums.CMD_RUNNING:
            command.completion = float(dct['completion'])

        command.message = dct['message']

        if "validatorMessage" in dct:
            command.validatorMessage = dct['validatorMessage']
            command.errorInfos = dct['errorInfos']
            if command.validatorMessage:
                command.status = enums.CMD_ERROR

    def queueWorkload(self, workload):
        self.queue.put(workload)
        #self.event.set()

    ## Dumps all informations of the dispatcher to an HTML file.
    #
    def dumpToHTML(self):
        """
        # for license testing
        import random
        try:
            if not random.randint(0, 10):
                self.licenseManager.setRenderNodeMode("maya", random.randint(0, 1))
                self.licenseManager.setMaxLicensesNumber("maya", random.randint(1, 20))
        except:
            pass
        """
        from octopus.dispatcher.model import TaskNode
        import octopus.core.enums.rendernode as rendernodeStatusModule
        rnStatus = {}
        for status in [x for x in dir(rendernodeStatusModule) if x.startswith('RN_')]:
            rnStatus[str(getattr(rendernodeStatusModule, status))] = status

        javascript = """
        <script language="JavaScript" type="text/javascript">
        /* URL /commands/{cmdId}/
         * content {"status": 0} --> WAITING
         * content {"status": 2} --> FINISHED
         * content {"status": 4} --> ERROR
         * content {"status": 5} --> CANCELLED
         */
        function stopCommand(commandId) {

            xhr = new XMLHttpRequest();
            xhr.open("PUT", "http://localhost:8004/commands/" + commandId + "/", true);
            xhr.send("{\\"status\\":5}");



        }

        function restartCommand(commandId) {

                    xhr = new XMLHttpRequest();
            xhr.open("PUT", "http://localhost:8004/commands/" + commandId + "/", true);
           xhr.send("{\\"status\\":0}");


        }

        </script>
        <style type="text/css">
body {
    background-color: #EEEEEE;
    font-family: arial;
    font-size: 10px;
    margin: 20px;
}

.largeTable {
    width: 100%;
    font-size: 10px;
    border-collapse: collapse;
}

.largeTable td {
    border: thin solid black;
}

.zone {
    background-color: #AAAAAA;
    position: relative;
}

.headers {
    background-color: #CCCCCC;
    text-align: center;
    font-size: 12;
    font-weight: bold;
}

.zone th {
    background-color: #EEEEEE;
    font-size: 14px;
    height: 25px;
}

.poolName {
    background-color: #F0F0F0;
    font-size: 12px;
    font-style: italic;
    font-weight: bold;
    padding-left: 15px;
    width: 5% ;
}

.item {
    background-color: #CCCCCC;
    padding-left: 15px;

}
.depth0 {
 background-color: #777;
}
.depth1 {
 background-color: #999;
}
.depth2 {
 background-color: #BBB;
}
.depth3 {
 background-color: #DDD;
}
.depth4 {
 background-color: #FFF;
}


.item td {
    padding-left: 0px;
}

.todo {
    background-color: #EEEEEE;
    font-size: 1px;
    height: 10px;
}

.done {
    background-color: #66FF66;
    font-size: 1px;
    height: 10px;
}

.progress {
    padding: 0px :
 margin : 0px;
    text-align: center;
    font-size: 10px;
}

.progressT {
    padding: 0px;
    margin : 0px;
    width: 100%;
    font-size: 10px;
    text-align: center;
    height: 10px;
}

.aliasName {
    font-size: 12px;
}

.tinyCol {
    width: 5%;
    text-align: center;
    padding: 0px :
    margin : 0px;
}

.largeTable .dep0 {
    border: 0px;
}

.largeTable > tbody > tr:hover {
    background-color: #88DD22;
}

.largeTable .dep1 {
  border: 0px;
  background-image: url('IMAGE_BASE_URL/vertical.png');
  background-repeat: repeat-y;
  background-position: center;
}

.largeTable .dep2 {
  border: 0px;
  background-image: url('IMAGE_BASE_URL/end.png');
  background-repeat: no-repeat;
  background-position: bottom center;
}

.largeTable .dep3 {
  border: 0px;
  background-image: url('IMAGE_BASE_URL/start.png');
  background-position: top center;
  background-repeat: no-repeat;
}
.commandsTableGreen {
    background-color: #66FF66;
    text-align: center;
    border: 1px solid black;
}

.commandsTableYellow {
    background-color: #FFFF66;
    text-align: center;
    border: 1px solid black;
}

.commandsTable {
    text-align: center;
    border: 1px solid black;
}

.control {
border:1px solid black;
font-size:14px;
min-width:50px;
padding:3px;
background-color:#ddd;
font-weight: bold;
cursor:pointer ;
}

.stop {
background-color:#000000;
margin:5px;
padding-left:2px;
padding-right:4px;
font-size:10px;
}
        </style>
        """.replace("IMAGE_BASE_URL", "http://bud-filaire/~bud/images/")

        header = """<html>\n
        <head>\n
        <title>Dispatcher : cycle %d</title>\n
        <link rel=\"stylesheet\" type=\"text/css\" href=\"dumpDispatch.css\">\n%s
        <meta http-equiv="refresh" content="5">
        </head>\n<body>\n
        """ % (self.cycle, javascript)
        corpus = ""
        corpus += "<a href ='dumpDispatch_%03d.html'>Previous Cycle</a> - <a href ='dumpDispatch_%03d.html'>Next Cycle</a><br>" % (self.cycle - 1, self.cycle + 1)

        corpus += "<div class =\"zone\">\n<table class=\"largeTable\">"
        corpus += "<tr><th colspan=\"4\">Licences</th></tr>\n"
        """
                    self.name = name
            self.maximum = int(maximum)
            self.used = 0
            self.renderNodeMode = renderNodeMode
            self.currentUsingRenderNodes = []
        """
        corpus += "<tr class = \"headers\"><td>name</td><td>Used</td><td>RN mode</td><td>currently used by</td></tr>\n"
        licences = self.licenseManager.licenses
        licencesNames = licences.keys()
        licencesNames.sort()
        for licenceName in licencesNames:
            linesNumber = max(1, len(set(licences[licenceName].currentUsingRenderNodes)))
            corpus += "<tr><td rowspan=\"%d\" align=\"center\" class = \"item\">%s</td>\n" % (linesNumber, licenceName)
            corpus += "<td rowspan=\"%d\" align=\"center\" class = \"item\">%d/%d</td>" % (linesNumber, licences[licenceName].used, licences[licenceName].maximum)
            #corpus += "<td rowspan=\"%d\" align=\"center\" class = \"item\">%r</td>" % (linesNumber, licences[licenceName].renderNodeMode)
            currentUsing = ""
            rns = [x for x in set(licences[licenceName].currentUsingRenderNodes)]
            rns.sort(lambda a, b: cmp(a.name, b.name))

            if not rns:
                currentUsing = "<td class = \"item\">&nbsp;</td>"
            else:
                c = 0
                for rn in rns:
                    num = len([x for x in licences[licenceName].currentUsingRenderNodes if x == rn])
                    if num == 1:
                        add = ""
                    else:
                        #if licences[licenceName].renderNodeMode:
                        #    add = " (shared for %d commands)" % num
                        #else:
                        add = " (for %d commands)" % num
                    currentUsing += "<td class = \"item\">%s%s</td>" % (rn.name, add)
                    if c == 0 and len(rns):
                        currentUsing += "</tr><tr>"
                        c = 1
            currentUsing += "</tr>"
            corpus += currentUsing
        corpus += "</table>\n</div></br>\n"

        corpus += "<div class =\"zone\">\n<table class=\"largeTable\">"
        corpus += "<tr><th colspan=\"10\">RenderNodes</th></tr>\n"
        corpus += "<tr class = \"headers\"><td>name</td><td>IP</td><td>port</td><td>status</td><td>free Cores</td><td>free RAM</td><td>command</td></tr>\n"
        for pool in self.dispatchTree.pools:
            corpus += "<tr><td class = \"poolName\">%s</td></tr>\n" % self.dispatchTree.pools[pool].name
            for rn in self.dispatchTree.pools[pool].renderNodes:
                #@todo: move that instruction somewhere else
                rn.updateStatus()
                commandTxt = ""
                cols = len(rn.commands)
                if rn.commands:
                    for command in rn.commands.values():
                        if commandTxt:
                            commandTxt += "</tr><tr class = \"item\">"
                        commandTxt += "<td align=\"center\">&nbsp;&nbsp;" + str(command) + "&nbsp;&nbsp;(" + str(command.task.minNbCores) + "/" + str(command.task.maxNbCores) + "/" + str(command.task.ramUse) + ") - " + str(int(command.completion * 100.)) + " %</td>"
                    if cols > 1:
                        commandTxt += "</tr>"
                else:
                    commandTxt = "<td align=\"center\"><i>None</i></td>"

                corpus += ("<tr class = \"item\" ><td rowspan=\"" + str(cols) + "\">&#149;&nbsp;%s</td><td align=\"center\" rowspan=\"" + str(cols) + "\">%s</td><td align=\"center\" rowspan=\"" + str(cols) + "\">%s</td><td align=\"center\" rowspan=\"" + str(cols) + "\">%s</td><td align=\"center\" rowspan=\"" + str(cols) + "\">%s</td><td align=\"center\" rowspan=\"" + str(cols) + "\">%s</td>%s</tr>\n") % (rn.name, rn.host, rn.port, rnStatus[str(rn.status)] + " " + str(rn.status), str(rn.freeCoresNumber) + "/" + str(rn.coresNumber), str(rn.freeRam) + "/" + str(rn.ramSize), commandTxt)

        corpus += "</table>\n</div>\n"
        dependencies = []
        views = self.dispatchTree.root.children

        for view in views:

            corpus += "<br>\n<div class =\"zone\">\n"
            corpus += "<table class=\"largeTable\">\n"
            corpus += "<tr><th colspan=\"10\">View : %s</th></tr>\n" % view.name

            i = 0
            depth = 0
            aliasToProceed = [(view, depth)]
            allNodesIds = []
            while len(aliasToProceed) > i:
                alias, depth = aliasToProceed[i]
                allNodesIds.append(alias.id)
                for dep in alias.dependencies:
                    dependencies.append((alias.id, dep[0], dep[1]))
                if not alias:
                    continue
                if isinstance(alias, TaskNode):
                    children = []
                else:
                    alias.children.sort()
                    children = [child for child in alias.children]
                    children.reverse()
                for child in children:
                    if not child in aliasToProceed:
                        aliasToProceed.insert(i + 1, (child, depth + 1))
                    else:
                        raise RuntimeError("Alias cycling reference")
                i += 1

            nbDependencies = len(dependencies)
            corpus += "<tr class = \"headers\"><td class = \"poolName\" width=\"300px\">name</td><td >pools</td><td class = \"tinyCol\">id</td><td class = \"tinyCol\">priority</td><td class = \"tinyCol\">dispatch key</td><td>completion</td><td colspan='%d'>dependencies</td></tr>\n" % nbDependencies
            rowTemplate = "<td class='dep%d'>&nbsp;</td>" * nbDependencies
            depStyle = {}
            EMPTY, VERTI, START, END = range(4)

#            import random
#            random.shuffle(dependencies)

            for alias, depth in aliasToProceed:
                tmp = []
                currentId = alias.id

                #currentPos = allNodes.getItem(currentId)

                for dep in dependencies:
                    try:
                        aPos = allNodesIds.index(dep[0])
                        bPos = allNodesIds.index(dep[1])
                    except ValueError:
                        continue
                    startPos = min(aPos, bPos)
                    endPos = max(aPos, bPos)
                    if currentId in allNodesIds[startPos + 1:endPos]:
                        tmp.append(VERTI)
                    elif currentId == dep[0]:
                        tmp.append(START)
                    elif currentId == dep[1]:
                        tmp.append(END)
                    else:
                        tmp.append(EMPTY)
                depStyle[currentId] = tmp

            for alias, depth in aliasToProceed:
                pools = "<table class = \"largeTable\">"

                if len(alias.poolShares):
                    ps = []
                    for pName in alias.poolShares:
                        p = alias.poolShares[pName]
                        ps.append([p.pool.name, p.maxRN])
                    ps.sort()
                    for p in ps:
                        pools += "<tr><td>%d %s</td></tr>" % (p[1], p[0])
                pools += "</table>"
                commands = "<table class = \"largeTable\">"
                if isinstance(alias, TaskNode):
                    zeroLine = "<tr class = \"commandsTable\">"
                    firstLine = "<tr class = \"commandsTable\">"
                    secondLine = "<tr class = \"commandsTable\">"
                    thirdLine = "<tr class = \"commandsTable\">"
                    fourthLine = "<tr class = \"commandsTable\">"
                    aliases = []
                    if isinstance(alias, TaskNode):
                        aliases = alias.task.commands
                    for command in aliases:
                        add = ""
                        if int(command.completion) == 100:
                            add = "Green"
                        elif command.completion:
                            add = "Yellow"
                        firstLine += "<td width=\"%d%%\" class = \"commandsTable%s\">%d %%</td>" % (100. / float(len(alias.task.commands)), add, command.completion)
                        name = "-"
                        if command.renderNode:
                            name = command.renderNode.name
                        secondLine += "<td class = \"commandsTable\">%s</td>" % name
                        #if command.status == COMMAND.RUNNING or command.status == COMMAND.FINISHING:

                        # everything is available
                        zeroLine += "<td class = \"commandsTable\"><span onClick='restartCommand(%d)' class ='control'>&laquo;</span>&nbsp;<span class ='control'><span  onClick='stopCommand(%d)' class ='stop'>&nbsp;</span></span></td>" % (command.id, command.id)
                        #STATUS_NAMES = ( 'WAITING', 'RUNNING', 'FINISHED', 'FINISHING','ERROR' ,'CANCELED')
                        status = " 'style=\"background-color:white\"'>WAITING"
                        if command.status == enums.CMD_CANCELED:
                            status = " 'style=\"background-color:white\"'>CANCELED"
                        elif command.status == enums.CMD_RUNNING:
                            status = " 'style=\"background-color:yellow\"'>RUNNING"
                        elif command.status == enums.CMD_DONE:
                            status = " 'style=\"background-color:green\"'>FINISHED"
                        elif command.status == enums.CMD_ERROR:
                            status = " 'style=\"background-color:red\"'>ERROR"
                        elif command.status == enums.CMD_FINISHING:
                            status = " 'style=\"background-color:green\"'>FINISHING"
                        thirdLine += "<td class = \"commandsTable\" " + status + "</td>"
                        """
                        if command.errorInfos:
                            fourthLine += "<td class = \"commandsTable\" >"+str(command.errorInfos)+"</td>"
                        else:
                            fourthLine += "<td class = \"commandsTable\" ></td>"
                        """
                        """
                        elif command.status == COMMAND.WAITING:
                            # nothing is available
                            zeroLine += "<td class = \"commandsTable\"><span class ='control'>&laquo;</span>&nbsp;<span class ='control'><span class ='stop' style ='background-color:white;'>&nbsp;</span></span></td>"
                        else:
                            # case done
                            zeroLine += "<td class = \"commandsTable\"><span class ='control'>&laquo;</span>&nbsp;<span class ='control'><span class ='stop' style ='background-color:white;'>&nbsp;</span></span></td>"
                        """

                    firstLine += "<tr>"
                    secondLine += "<tr>"
                    zeroLine += "<tr>"
                    thirdLine += "<tr>"
                    fourthLine += "<tr>"
                    commands += zeroLine
                    commands += firstLine
                    commands += secondLine
                    commands += thirdLine
                    commands += fourthLine
                commands += "</table>"
                pctage = "<td class =\"done\" width='%d%%'>&nbsp;</td><td class =\"todo\" width='%d%%'>&nbsp;</td>" % (alias.completion * 100, 100 - alias.completion * 100)
                if not alias.completion:
                    pctage = "<td colspan='2' class =\"todo\" width='100%'>&nbsp;</td>"
                elif alias.completion >= 1:
                    pctage = "<td colspan='2' class =\"done\" width='100%'>&nbsp;</td>"
                arguments = "<table>"
                if isinstance(alias, TaskNode):
                    arguments += "".join(["<tr><td><b>%s</b></td><td>%s</td></tr>" % (str(key), str(val)) for key, val in alias.task.arguments.items()])
                arguments += "</table>"

                deps = ""
                #
                #

                currentId = alias.id
                styles = depStyle[currentId]
                try:
                    deps = rowTemplate % tuple(styles)
                except TypeError:
                    deps = ""
                corpus += """
<tr class = \"item depth%d\">
    <td class=\"aliasName\">%s&nbsp;%s&nbsp;&nbsp;&nbsp;</td>
    <td>%s</td>
    <td class = \"tinyCol\">%r</td>
    <td class = \"tinyCol\">%r</td>
    <td class = \"tinyCol\">%r</td>
    <td class = \"progress\">
        <table class=\"progressT\" >
            <tr>%s
            <tr>
            <td colspan=\"2\">%d&nbsp;%%</td>
            </tr>
        </table>
    </td>
    %s
</tr>
 """ % (depth, "&#149;" * 3 * depth, alias.name, pools, alias.id, alias.priority, alias.dispatchKey, pctage, alias.completion * 100, deps)

            corpus += "</table>\n"
            corpus += "</div>\n"

        bottom = "</body>\n</html>"

        self.htmlData = "".join([header, corpus, bottom])

        if settings.DUMP_HTML_DATA:
            srcFile = "/tmp/dumpDispatch_%03d.html" % self.cycle
            destFile = "/tmp/dumpDispatch.html"

            fileOut = open(srcFile, "w")
            fileOut.write(self.htmlData)
            fileOut.close()

            try:
                fileOut = open(destFile, "w")
                fileOut.write(self.htmlData)
                fileOut.close()
            except IOError:
                import shutil
                shutil.copy(srcFile, destFile)
