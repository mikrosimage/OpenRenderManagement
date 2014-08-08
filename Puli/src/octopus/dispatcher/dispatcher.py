"""
.. module:: Dispatcher
   :platform: Unix
   :synopsis: A useful module indeed.
"""

from __future__ import with_statement

import logging
import socket
import time
from Queue import Queue
from itertools import groupby, ifilter, chain
import collections
try:
    import simplejson as json
except ImportError:
    import json

# from octopus.core import tools
from octopus.core import singletonconfig, singletonstats

from octopus.core.threadpool import ThreadPool, makeRequests, NoResultsPending
from octopus.core.framework import MainLoopApplication

from octopus.dispatcher.model import (DispatchTree, FolderNode, RenderNode,
                                      Pool, PoolShare, enums)
from octopus.dispatcher.strategies import FifoStrategy

from octopus.dispatcher import settings
from octopus.dispatcher.db.pulidb import PuliDB
from octopus.dispatcher.model.enums import *
from octopus.dispatcher.poolman.filepoolman import FilePoolManager
from octopus.dispatcher.poolman.wspoolman import WebServicePoolManager
from octopus.dispatcher.licenses.licensemanager import LicenseManager


LOGGER = logging.getLogger('dispatcher')


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
            # Disable passing framework to super__new__ call. It is automatically avaible via super class hierarchy
            # This removes a deprecation warning when launching dispatcher
            cls.instance = super(Dispatcher, cls).__new__(cls)
        return cls.instance

    def __init__(self, framework):
        if self.init:
            return
        self.init = True
        self.nextCycle = time.time()

        MainLoopApplication.__init__(self, framework)

        self.threadPool = ThreadPool(16, 0, 0, None)

        #
        # Class holding custom infos on the dispatcher.
        # This data can be periodically flushed in a specific log file for later use
        #
        

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
            LOGGER.warning("reloading jobs from database")
            beginTime = time.time()
            self.pulidb.restoreStateFromDb(self.dispatchTree, rnsAlreadyInitialized)
            LOGGER.warning("reloading took %.2fs" % (time.time() - beginTime))
            LOGGER.warning("done reloading jobs from database")
            LOGGER.warning("reloaded %d tasks" % len(self.dispatchTree.tasks))
        LOGGER.warning("checking dispatcher state")

        self.dispatchTree.updateCompletionAndStatus()
        self.updateRenderNodes()
        self.dispatchTree.validateDependencies()
        if self.enablePuliDB and not self.cleanDB:
            self.dispatchTree.toModifyElements = []
        self.defaultPool = self.dispatchTree.pools['default']

        LOGGER.warning("loading dispatch rules")
        self.loadRules()
        # it should be better to have a maxsize
        self.queue = Queue(maxsize=10000)

    def initPoolsDataFromBackend(self):
        '''
        Loads pools and workers from appropriate backend.
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
        '''
        | Dispatcher main loop iteration.
        | Periodically called with tornado'sinternal callback mecanism, the frequency is defined by config: CORE.MASTER_UPDATE_INTERVAL
        | During this process, the dispatcher will:
        |   - update completion and status for all jobs in dispatchTree
        |   - update status of renderNodes
        |   - validate inter tasks dependencies
        |   - update the DB with recorded changes in the model
        |   - compute new assignments and send them to the proper rendernodes
        |   - release all finished jobs/rns
        '''
        
        # JSA DEBUG: timer pour profiler les etapes       

        loopStartTime = time.time()
        prevTimer = loopStartTime

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleDate = loopStartTime

        LOGGER.info("")
        LOGGER.info("-----------------------------------------------------")
        LOGGER.info(" Start dispatcher process cycle.")
        LOGGER.info("-----------------------------------------------------")


        # JSA: Check if requests are finished (necessaire ?)
        try:
            self.threadPool.poll()
        except NoResultsPending:
            pass
        else:
            LOGGER.info("finished some network requests")

        self.cycle += 1

        # Update of allocation is done when parsing the tree for completion and status update (done partially for invalidated node only i.e. when needed)
        self.dispatchTree.updateCompletionAndStatus()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['update_tree'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> update completion status" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        # Update render nodes
        self.updateRenderNodes()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['update_rn'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> update render node" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        # Validate dependencies
        self.dispatchTree.validateDependencies()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['update_dependencies'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> validate dependencies" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()


        # update db
        self.updateDB()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['update_db'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> update DB" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        # compute and send command assignments to rendernodes
        assignments = self.computeAssignments()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['compute_assignment'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> compute assignments." % ( (time.time() - prevTimer)*1000)  )
        prevTimer = time.time()

        self.sendAssignments(assignments)
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['send_assignment'] = time.time() - prevTimer
            singletonstats.theStats.cycleCounts['num_assignments'] = len(assignments)
        LOGGER.info("%8.2f ms --> send %r assignments." % ( (time.time() - prevTimer)*1000, len(assignments) )  )
        prevTimer = time.time()

        # call the release finishing status on all rendernodes
        for renderNode in self.dispatchTree.renderNodes.values():
            renderNode.releaseFinishingStatus()
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['release_finishing'] = time.time() - prevTimer
        LOGGER.info("%8.2f ms --> releaseFinishingStatus" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        loopDuration = (time.time() - loopStartTime)*1000
        LOGGER.info( "%8.2f ms --> cycle ended. " % loopDuration )
        LOGGER.info("-----------------------------------------------------")

        # TODO: process average and sums of datas in stats, if flush time, send it to disk
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleTimers['time_elapsed'] = time.time() - loopStartTime
            singletonstats.theStats.aggregate()



    def updateDB(self):

        # TODO: Study how to change the DB subsystem to a simple file dump (json or pickle)

        # data1 = {'a': [1, 2.0, 3, 4],
        #          'b': ('string', u'Unicode string'),
        #          'c': None}
        # with open('/datas/puli/Puli/data.json', 'wb') as fp:
        #     json.dump(self.dispatchTree, fp)

        # import shelve

        # d = shelve.open('/datas/puli/Puli/data.pkl')
        # d['test'] = self.dispatchTree
        # d.close()

        if settings.DB_ENABLE:
            self.pulidb.createElements(self.dispatchTree.toCreateElements)
            self.pulidb.updateElements(self.dispatchTree.toModifyElements)
            self.pulidb.archiveElements(self.dispatchTree.toArchiveElements)
            # LOGGER.info("                UpdateDB: create=%d update=%d delete=%d" % (len(self.dispatchTree.toCreateElements), len(self.dispatchTree.toModifyElements), len(self.dispatchTree.toArchiveElements)) )
        self.dispatchTree.resetDbElements()

    def computeAssignments(self):
        '''Computes and returns a list of (rendernode, command) assignments.'''

        from .model.node import NoRenderNodeAvailable, NoLicenseAvailableForTask
        # if no rendernodes available, return
        if not any(rn.isAvailable() for rn in self.dispatchTree.renderNodes.values()):
            return []

        assignments = []

        # first create a set of entrypoints that are not done nor cancelled nor blocked nor paused and that have at least one command ready
        # FIXME: hack to avoid getting the 'graphs' poolShare node in entryPoints, need to avoid it more nicely...
        entryPoints = set([poolShare.node for poolShare in self.dispatchTree.poolShares.values() if poolShare.node.status not in [NODE_BLOCKED, NODE_DONE, NODE_CANCELED, NODE_PAUSED] and poolShare.node.readyCommandCount > 0 and poolShare.node.name != 'graphs'])

        # don't proceed to the calculation if no rns availables in the requested pools
        rnsBool = False
        for pool, nodesiterator in groupby(entryPoints, lambda x: x.poolShares.values()[0].pool):
            rnsAvailables = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED, RN_WORKING]])
            if len(rnsAvailables):
                rnsBool = True

        if not rnsBool:
            return []


        # Log time updating max rn
        prevTimer = time.time()

        # sort by pool for the groupby
        entryPoints = sorted(entryPoints, key=lambda node: node.poolShares.values()[0].pool)

        # update the value of the maxrn for the poolshares (parallel dispatching)
        for pool, nodesiterator in groupby(entryPoints, lambda x: x.poolShares.values()[0].pool):

            # we are treating every active node of the pool
            nodesList = [node for node in nodesiterator]

            # the new maxRN value is calculated based on the number of active jobs of the pool, and the number of online rendernodes of the pool
            rnsNotOffline = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED]])
            rnsSize = len(rnsNotOffline)
            # LOGGER.debug("@   - nb rns awake:%r" % (rnsSize) )

            # if we have a userdefined maxRN for some nodes, remove them from the list and substracts their maxRN from the pool's size
            l = nodesList[:]  # duplicate the list to be safe when removing elements
            for node in l:
                # LOGGER.debug("@   - checking userDefMaxRN: %s -> %r maxRN=%d" % (node.name, node.poolShares.values()[0].userDefinedMaxRN, node.poolShares.values()[0].maxRN ) )
                if node.poolShares.values()[0].userDefinedMaxRN and node.poolShares.values()[0].maxRN not in [-1, 0]:
                    # LOGGER.debug("@     removing: %s -> maxRN=%d" % (node.name, node.poolShares.values()[0].maxRN ) )
                    nodesList.remove(node)
                    rnsSize -= node.poolShares.values()[0].maxRN

            # LOGGER.debug("@   - nb rns awake after maxRN:%d" % (rnsSize) )

            if len(nodesList) == 0:
                continue

            # Prepare updatedMaxRN with dispatch key proportions
            dkList = []                 # list of dks (integer only)
            dkPositiveList = []         # Normalized list of dks (each min value of dk becomes 1, other higher elems of dkList gets proportionnal value)
            nbJobs = len(nodesList)     # number of jobs in the current pool
            nbRNAssigned = 0            # number of render nodes assigned for this pool

            for node in nodesList:
                dkList.append(node.dispatchKey)

            dkMin = min(dkList)
            dkPositiveList = map(lambda x: x-dkMin+1, dkList)
            dkSum = sum(dkPositiveList)

            # sort by id (fifo)
            nodesList = sorted(nodesList, key=lambda x: x.id)

            # then sort by dispatchKey (priority)
            nodesList = sorted(nodesList, key=lambda x: x.dispatchKey, reverse=True)
            
            for dk, nodeIterator in groupby(nodesList, lambda x: x.dispatchKey):

                nodes = [node for node in nodeIterator]
                dkPos = dkPositiveList[ dkList.index(dk) ]

                if dkSum > 0:                  
                    updatedmaxRN = int( round( rnsSize * (dkPos / float(dkSum) )))
                else:
                    updatedmaxRN = int(round( rnsSize / float(nbJobs) ))

                for node in nodes:
                    node.poolShares.values()[0].maxRN = updatedmaxRN
                    nbRNAssigned += updatedmaxRN

            # Add remaining RNs to most important jobs
            unassignedRN = rnsSize - nbRNAssigned
            while unassignedRN > 0:
                for node in nodesList:
                    if unassignedRN > 0:
                        node.poolShares.values()[0].maxRN += 1
                        unassignedRN -= 1
                    else:
                        break

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.assignmentTimers['update_max_rn'] = time.time() - prevTimer
        LOGGER.info( "%8.2f ms --> .... updating max RN values", (time.time() - prevTimer)*1000 )

        # now, we are treating every nodes
        # sort by id (fifo)
        entryPoints = sorted(entryPoints, key=lambda node: node.id)
        # then sort by dispatchKey (priority)
        entryPoints = sorted(entryPoints, key=lambda node: node.dispatchKey, reverse=True)

        # Put nodes with a userDefinedMaxRN first
        userDefEntryPoints = ifilter( lambda node: node.poolShares.values()[0].userDefinedMaxRN, entryPoints )
        standardEntryPoints = ifilter( lambda node: not node.poolShares.values()[0].userDefinedMaxRN, entryPoints )
        scoredEntryPoints = chain( userDefEntryPoints, standardEntryPoints)


        # Log time dispatching RNs
        prevTimer = time.time()

        # 
        # HACK update license info for katana with rlmutils
        # This helps having the real number of used licenses before finishing assignment
        # This is done because katana rlm management sometime reserves 2 token (cf BUGLIST v1.4)
        try:
            import subprocess
            strRlmKatanaUsed=''
            strRlmKatanaUsed = subprocess.Popen(["/s/apps/lin/farm/tools/rlm_katana_used.sh"], stdout=subprocess.PIPE).communicate()[0]

            katanaUsed = int(strRlmKatanaUsed)
            LOGGER.debug("HACK update katana license: used = %d" % (katanaUsed))

            # Sets used license number
            try:
                self.licenseManager.licenses["katana"].used = katanaUsed
            except KeyError:
                LOGGER.warning("License katana not found... Impossible to set 'used' value: %d" % katanaUsed)
        except Exception, e:
            LOGGER.warning("Error getting number of katana license used via rlmutil (e: %r, rlmoutput=%r)" % (e,strRlmKatanaUsed))
        # ENDHACK
        #

        # Iterate over each entryPoint to get an assignment
        for entryPoint in scoredEntryPoints:
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
                except NoLicenseAvailableForTask:
                    LOGGER.info("Missing license for node \"%s\" (other commands can start anyway)." % entryPoint.name)
                    pass

        assignmentDict = collections.defaultdict(list)
        for (rn, com) in assignments:
            assignmentDict[rn].append(com)

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.assignmentTimers['dispatch_command'] = time.time() - prevTimer
        LOGGER.info( "%8.2f ms --> .... dispatching commands", (time.time() - prevTimer)*1000  )

        #
        # Check replacements
        #
        # - faire une passe pour les jobs n'ayant pas leur part de gateau
        #     - identifier dans leur pool les jobs killable
        #     - pour chaque ressource, si match : on jette le job en cours ET on desactive son attribut killable


        #
        # Backfill
        #
        # TODO refaire une passe pour les jobs ayant un attribut "killable" et au moins une pool additionnelle

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

            LOGGER.info(" - assignment cleared: command[%r] on rn[%r]" % (command.id, rendernode.name) )


    def handleNewGraphRequestApply(self, graph):
        '''Handles a graph submission request and closes the given ticket
        according to the result of the process.
        '''
        prevTimer = time.time()

        nodes = self.dispatchTree.registerNewGraph(graph)

        LOGGER.info("%.2f ms --> graph registered" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        # handles the case of post job with paused status
        for node in nodes:
            try:
                if node.tags['paused'] == 'true':
                    node.setPaused(True)
            except KeyError:
                continue

        LOGGER.info("%.2f ms --> jobs set in pause if needed" % ( (time.time() - prevTimer)*1000 ) )
        prevTimer = time.time()

        LOGGER.info('Added graph "%s" to the model.' % graph['name'])
        return nodes

    def updateCommandApply(self, dct):
        '''
        Called from a RN with a json desc of a command (ie rendernode info, command info etc).
        Raise an execption to tell caller to send a HTTP404 response to RN, if not error a HTTP200 will be send instead
        '''
        commandId = dct['id']
        renderNodeName = dct['renderNodeName']

        try:
            command = self.dispatchTree.commands[commandId]
        except KeyError:
            raise KeyError("Command not found: %d" % commandId)

        if not command.renderNode:
            # souldn't we reassign the command to the rn??
            raise KeyError("Command %d (%d) is no longer registered on rendernode %s" % (commandId, int(dct['status']), renderNodeName))
        elif command.renderNode.name != renderNodeName:
            # in this case, kill the command running on command.renderNode.name
            # rn = command.renderNode
            # rn.clearAssignment(command)
            # rn.request("DELETE", "/commands/" + str(commandId) + "/")
            LOGGER.warning("The emitting RN %s is different from the RN assigned to the command in pulimodel." % ( renderNodeName, rn.name ) )
            raise KeyError("Command %d is running on a different rendernode (%s) than the one in puli's model (%s)." % (commandId, renderNodeName, command.renderNode.name))

        rn = command.renderNode
        rn.lastAliveTime = max(time.time(), rn.lastAliveTime)

        # if command is no more in the rn's list, it means the rn was reported as timeout or asynchronously removed from RN
        if commandId not in rn.commands:
            if len(rn.commands) == 0 and command.status is not enums.CMD_CANCELED:
                # in this case, re-add the command to the list of the rendernode
                rn.commands[commandId] = command
                # we should re-reserve the lic
                rn.reserveLicense(command, self.licenseManager)
                LOGGER.warning("re-assigning command %d on %s. (TIMEOUT?)" % (commandId, rn.name))
            else:
                # The command has been cancelled on the dispatcher but update from RN only arrives now
                LOGGER.warning("Status update for %d (%d) from %s but command is currently assigned." % (commandId, int(dct['status']), renderNodeName ))
                pass

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

        # Stats info received and not none. Means we need to update it on the command.
        # If stats received is none, no change on the worker, we do not update the command.
        if "stats" in dct and dct["stats"] is not None:
            # LOGGER.debug("Updating stats on server: %r" % dct["stats"] )
            command.stats = dct["stats"]



    def queueWorkload(self, workload):
        self.queue.put(workload)
