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
from octopus.core.tools import elapsedTimeToString

from octopus.dispatcher.model import (DispatchTree, FolderNode, RenderNode,
                                      Pool, PoolShare, enums)
from octopus.dispatcher.strategies import FifoStrategy

from octopus.dispatcher import settings
from octopus.dispatcher.db.pulidb import PuliDB
from octopus.dispatcher.model.enums import *
from octopus.dispatcher.poolman.filepoolman import FilePoolManager
from octopus.dispatcher.poolman.wspoolman import WebServicePoolManager
from octopus.dispatcher.licenses.licensemanager import LicenseManager


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
            # Disable passing framework to super__new__ call.
            # It is automatically avaible via super class hierarchy
            # This removes a deprecation warning when launching dispatcher
            cls.instance = super(Dispatcher, cls).__new__(cls)
        return cls.instance

    def __init__(self, framework):
        LOGGER = logging.getLogger('main.dispatcher')
        if self.init:
            return
        self.init = True
        self.nextCycle = time.time()

        MainLoopApplication.__init__(self, framework)

        self.threadPool = ThreadPool(16, 0, 0, None)

        #
        # Class holding custom infos on the dispatcher.
        # This data can be periodically flushed in a specific log file for
        # later use
        #
        self.cycle = 1
        self.dispatchTree = DispatchTree()
        self.licenseManager = LicenseManager()
        self.enablePuliDB = settings.DB_ENABLE
        self.cleanDB = settings.DB_CLEAN_DATA
        self.restartService = False

        self.pulidb = None
        if self.enablePuliDB:
            self.pulidb = PuliDB(self.cleanDB, self.licenseManager)

        self.dispatchTree.registerModelListeners()
        rnsAlreadyInitialized = self.initPoolsDataFromBackend()

        if self.enablePuliDB and not self.cleanDB:
            LOGGER.warning("--- Reloading database (9 steps) ---")
            prevTimer = time.time()
            self.pulidb.restoreStateFromDb(self.dispatchTree, rnsAlreadyInitialized)

            LOGGER.warning("%d jobs reloaded from database" % len(self.dispatchTree.tasks))
            LOGGER.warning("Total time elapsed %s" % elapsedTimeToString(prevTimer))
            LOGGER.warning("")

        LOGGER.warning("--- Checking dispatcher state (3 steps) ---")
        startTimer = time.time()
        LOGGER.warning("1/3 Update completion and status")
        self.dispatchTree.updateCompletionAndStatus()
        LOGGER.warning("    Elapsed time %s" % elapsedTimeToString(startTimer))

        prevTimer = time.time()
        LOGGER.warning("2/3 Update rendernodes")
        self.updateRenderNodes()
        LOGGER.warning("    Elapsed time %s" % elapsedTimeToString(prevTimer))

        prevTimer = time.time()
        LOGGER.warning("3/3 Validate dependencies")
        self.dispatchTree.validateDependencies()
        LOGGER.warning("    Elapsed time %s" % elapsedTimeToString(prevTimer))
        LOGGER.warning("Total time elapsed %s" % elapsedTimeToString(startTimer))
        LOGGER.warning("")

        if self.enablePuliDB and not self.cleanDB:
            self.dispatchTree.toModifyElements = []

        # If no 'default' pool exists, create default pool
        # When creating a pool with id=None, it is automatically appended in "toCreateElement" list in dispatcher and in the dispatcher's "pools" attribute
        if 'default' not in self.dispatchTree.pools:
            pool = Pool(None, name='default')
            LOGGER.warning("Default pool was not loaded from DB, create a new default pool: %s" % pool)
        self.defaultPool = self.dispatchTree.pools['default']

        LOGGER.warning("--- Loading dispatch rules ---")
        startTimer = time.time()
        self.loadRules()
        LOGGER.warning("Total time elapsed %s" % elapsedTimeToString(startTimer))
        LOGGER.warning("")

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

    def shutdown(self):
        '''
        Clean procedure before shutting done puli server.
        '''
        logging.getLogger('main').warning("-----------------------------------------------")
        logging.getLogger('main').warning("Exit event caught: closing dispatcher...")

        try:
            self.dispatchTree.updateCompletionAndStatus()
            logging.getLogger('main').warning("[OK] update completion and status")
        except Exception:
            logging.getLogger('main').warning("[HS] update completion and status")

        try:
            self.updateRenderNodes()
            logging.getLogger('main').warning("[OK] update render nodes")
        except Exception:
            logging.getLogger('main').warning("[HS] update render nodes")

        try:
            self.dispatchTree.validateDependencies()
            logging.getLogger('main').warning("[OK] validate dependencies")
        except Exception:
            logging.getLogger('main').warning("[HS] validate dependencies")
        try:
            self.updateDB()
            logging.getLogger('main').warning("[OK] update DB")
        except Exception:
            logging.getLogger('main').warning("[HS] update DB")

    def loadRules(self):
        from .rules.graphview import GraphViewBuilder
        graphs = self.dispatchTree.findNodeByPath("/graphs", None)
        if graphs is None:
            logging.getLogger('main.dispatcher').fatal("No '/graphs' node, impossible to load rule for /graphs.")
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
        log = logging.getLogger('main')
        loopStartTime = time.time()
        prevTimer = loopStartTime

        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleDate = loopStartTime

        log.info("-----------------------------------------------------")
        log.info(" Start dispatcher process cycle (old version).")

        try:
            self.threadPool.poll()
        except NoResultsPending:
            pass
        else:
            log.info("finished some network requests")
            pass

        self.cycle += 1

        # Update of allocation is done when parsing the tree for completion and status update (done partially for invalidated node only i.e. when needed)
        self.dispatchTree.updateCompletionAndStatus()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['update_tree'] = time.time() - prevTimer
        log.info("%8.2f ms --> update completion status" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        # Update render nodes
        self.updateRenderNodes()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['update_rn'] = time.time() - prevTimer
        log.info("%8.2f ms --> update render node" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        # Validate dependencies
        self.dispatchTree.validateDependencies()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['update_dependencies'] = time.time() - prevTimer
        log.info("%8.2f ms --> validate dependencies" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        # update db
        self.updateDB()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['update_db'] = time.time() - prevTimer
        log.info("%8.2f ms --> update DB" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        # compute and send command assignments to rendernodes
        assignments = self.computeAssignments()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['compute_assignment'] = time.time() - prevTimer
        log.info("%8.2f ms --> compute assignments." % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        self.sendAssignments(assignments)
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['send_assignment'] = time.time() - prevTimer
            singletonstats.theStats.cycleCounts['num_assignments'] = len(assignments)
        log.info("%8.2f ms --> send %r assignments." % ((time.time() - prevTimer) * 1000, len(assignments)))
        prevTimer = time.time()

        # call the release finishing status on all rendernodes
        for renderNode in self.dispatchTree.renderNodes.values():
            renderNode.releaseFinishingStatus()
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['release_finishing'] = time.time() - prevTimer
        log.info("%8.2f ms --> releaseFinishingStatus" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        loopDuration = (time.time() - loopStartTime)*1000
        log.info("%8.2f ms --> cycle ended. " % loopDuration)

        #
        # Send stat data to disk
        #
        if singletonconfig.get('CORE', 'GET_STATS'):
            singletonstats.theStats.cycleTimers['time_elapsed'] = time.time() - loopStartTime
            singletonstats.theStats.aggregate()

    def updateDB(self):
        if settings.DB_ENABLE:
            self.pulidb.createElements(self.dispatchTree.toCreateElements)
            self.pulidb.updateElements(self.dispatchTree.toModifyElements)
            self.pulidb.archiveElements(self.dispatchTree.toArchiveElements)
            # logging.getLogger('main.dispatcher').info("                UpdateDB: create=%d update=%d delete=%d" % (len(self.dispatchTree.toCreateElements), len(self.dispatchTree.toModifyElements), len(self.dispatchTree.toArchiveElements)) )
        self.dispatchTree.resetDbElements()

    def computeAssignments(self):
        '''Computes and returns a list of (rendernode, command) assignments.'''

        LOGGER = logging.getLogger('main')

        from .model.node import NoRenderNodeAvailable, NoLicenseAvailableForTask
        # if no rendernodes available, return
        if not any(rn.isAvailable() for rn in self.dispatchTree.renderNodes.values()):
            return []

        # first create a set of entrypoints that are not done nor cancelled nor blocked nor paused and that have at least one command ready
        # FIXME: hack to avoid getting the 'graphs' poolShare node in entryPoints, need to avoid it more nicely...
        entryPoints = set([poolShare.node for poolShare in self.dispatchTree.poolShares.values()
                                if poolShare.node.status not in (NODE_BLOCKED, NODE_DONE, NODE_CANCELED, NODE_PAUSED) and poolShare.node.readyCommandCount > 0 and poolShare.node.name != 'graphs'])

        # don't proceed to the calculation if no render nodes available in the requested pools
        isRenderNodesAvailable = False
        for pool, jobsIterator in groupby(entryPoints, lambda x: x.mainPoolShare().pool):
            renderNodesAvailable = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED, RN_WORKING]])
            if len(renderNodesAvailable):
                isRenderNodesAvailable = True
                break
        if not isRenderNodesAvailable:
            return []

        # Log time updating max rn
        prevTimer = time.time()

        # sort by pool for the groupby
        entryPoints = sorted(entryPoints, key=lambda node: node.mainPoolShare().pool)

        # update the value of the maxrn for the poolshares (parallel dispatching)
        for pool, jobsIterator in groupby(entryPoints, lambda x: x.mainPoolShare().pool):

            # we are treating every active job of the pool
            jobsList = [job for job in jobsIterator]

            # the new maxRN value is calculated based on the number of active jobs of the pool, and the number of online rendernodes of the pool
            onlineRenderNodes = set([rn for rn in pool.renderNodes if rn.status not in [RN_UNKNOWN, RN_PAUSED]])
            nbOnlineRenderNodes = len(onlineRenderNodes)
            # LOGGER.debug("@   - nb rns awake:%r" % (nbOnlineRenderNodes) )

            # if we have a userdefined maxRN for some nodes, remove them from the list and substracts their maxRN from the pool's size
            l = jobsList[:]  # duplicate the list to be safe when removing elements
            for job in l:
                # LOGGER.debug("@   - checking userDefMaxRN: %s -> %r maxRN=%d" % (job.name, job.mainPoolShare().userDefinedMaxRN, job.mainPoolShare().maxRN ) )
                if job.mainPoolShare().userDefinedMaxRN and job.mainPoolShare().maxRN not in [-1, 0]:
                    # LOGGER.debug("@     removing: %s -> maxRN=%d" % (job.name, job.mainPoolShare().maxRN ) )
                    jobsList.remove(job)
                    nbOnlineRenderNodes -= job.mainPoolShare().maxRN

            # LOGGER.debug("@   - nb rns awake after maxRN:%d" % (nbOnlineRenderNodes) )
            if len(jobsList) == 0:
                continue

            # Prepare updatedMaxRN with dispatch key proportions
            # list of dks (integer only)
            dkList = [job.dispatchKey for job in jobsList]
            nbJobs = len(jobsList)     # number of jobs in the current pool
            nbRNAssigned = 0            # number of render nodes assigned for this pool

            dkMin = min(dkList)
            # dkPositiveList: Shift all dks values in order that each min value of dk becomes 1
            dkPositiveList = map(lambda x: x-dkMin+1, dkList)  # dk values start at 1
            dkSum = sum(dkPositiveList)

            # sort by id (fifo)
            jobsList = sorted(jobsList, key=lambda x: x.id)

            # then sort by dispatchKey (priority)
            jobsList = sorted(jobsList, key=lambda x: x.dispatchKey, reverse=True)

            for dk, jobIterator in groupby(jobsList, lambda x: x.dispatchKey):

                jobs = [job for job in jobIterator]
                # dkPositive: Shift all dks values in order that each min value of dk becomes 1
                dkPositive = dk - dkMin + 1

                # Proportion of render nodes for
                updatedmaxRN = int(round(nbOnlineRenderNodes * (dkPositive / float(dkSum))))

                for job in jobs:
                    job.mainPoolShare().maxRN = updatedmaxRN
                    nbRNAssigned += updatedmaxRN

            # PRA: Here is the main choice!
            # Add remaining RNs to most important jobs (to fix rounding errors)
            unassignedRN = nbOnlineRenderNodes - nbRNAssigned
            while unassignedRN > 0:
                for job in jobsList:
                    if unassignedRN <= 0:
                        break
                    job.mainPoolShare().maxRN += 1
                    unassignedRN -= 1

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.assignmentTimers['update_max_rn'] = time.time() - prevTimer
        LOGGER.info( "%8.2f ms --> .... updating max RN values", (time.time() - prevTimer)*1000 )

        # now, we are treating every nodes
        # sort by id (fifo)
        entryPoints = sorted(entryPoints, key=lambda node: node.id)
        # then sort by dispatchKey (priority)
        entryPoints = sorted(entryPoints, key=lambda node: node.dispatchKey, reverse=True)

        # Put nodes with a userDefinedMaxRN first
        userDefEntryPoints = ifilter(lambda node: node.mainPoolShare().userDefinedMaxRN, entryPoints)
        standardEntryPoints = ifilter(lambda node: not node.mainPoolShare().userDefinedMaxRN, entryPoints)
        scoredEntryPoints = chain(userDefEntryPoints, standardEntryPoints)

        # Log time dispatching RNs
        prevTimer = time.time()

        # Iterate over each entryPoint to get an assignment
        assignments = []  # list of (renderNode, Command)
        for entryPoint in scoredEntryPoints:
            # If we have dedicated render nodes for this poolShare
            if not any([poolShare.hasRenderNodesAvailable() for poolShare in entryPoint.poolShares.values()]):
                continue
	    
	    try:
	        for (rn, com) in entryPoint.dispatchIterator(lambda: self.queue.qsize() > 0):
        	    assignments.append((rn, com))
        	    # increment the allocatedRN for the poolshare
        	    entryPoint.mainPoolShare().allocatedRN += 1
        	    # save the active poolshare of the rendernode
        	    rn.currentpoolshare = entryPoint.mainPoolShare()
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

                log = logging.getLogger('assign')
                log.info("Sending command: %d from task %s to %s" % (command.id, command.task.name, rendernode))

                commandDict = {
                    "id": command.id,
                    "runner": str(command.task.runner),
                    "arguments": arguments,
                    "validationExpression": command.task.validationExpression,
                    "taskName": command.task.name,
                    "relativePathToLogDir": "%d" % command.task.id,
                    "environment": environment,
                    "runnerPackages": command.runnerPackages,
                    "watcherPackages": command.watcherPackages
                }
                body = json.dumps(commandDict)
                headers["Content-Length"] = len(body)
                headers["Content-Type"] = "application/json"

                try:
                    resp, data = rendernode.request("POST", "/commands/", body, headers)
                    if not resp.status == 202:
                        logging.getLogger('main.dispatcher').error("Assignment request failed: command %d on worker %s", command.id, rendernode.name)
                        failures.append((rendernode, command))
                    else:
                        logging.getLogger('main.dispatcher').info("Sent assignment of command %d to worker %s", command.id, rendernode.name)
                except rendernode.RequestFailed, e:
                    logging.getLogger('main.dispatcher').error("Assignment of command %d to worker %s failed. Worker is likely dead (%r)", command.id, rendernode.name, e)
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

            logging.getLogger('main.dispatcher').info(" - assignment cleared: command[%r] on rn[%r]" % (command.id, rendernode.name))

    def handleNewGraphRequestApply(self, graph):
        '''Handles a graph submission request and closes the given ticket
        according to the result of the process.
        '''
        prevTimer = time.time()
        nodes = self.dispatchTree.registerNewGraph(graph)

        logging.getLogger('main.dispatcher').info("%.2f ms --> graph registered" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        # handles the case of post job with paused status
        for node in nodes:
            try:
                if node.tags['paused'] == 'true' or node.tags['paused'] == True:
                    node.setPaused(True)
            except KeyError:
                continue

        logging.getLogger('main.dispatcher').info("%.2f ms --> jobs set in pause if needed" % ((time.time() - prevTimer) * 1000))
        prevTimer = time.time()

        logging.getLogger('main.dispatcher').info('Added graph "%s" to the model.' % graph['name'])
        return nodes

    def updateCommandApply(self, dct):
        '''
        Called from a RN with a json desc of a command (ie rendernode info, command info etc).
        Raise an execption to tell caller to send a HTTP404 response to RN, if not error a HTTP200 will be send instead
        '''
        log = logging.getLogger('main.dispatcher')
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
            log.warning("The emitting RN %s is different from the RN assigned to the command in pulimodel: %s." % (renderNodeName, command.renderNode.name))
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
                log.warning("re-assigning command %d on %s. (TIMEOUT?)" % (commandId, rn.name))

            # Command is already remove from RN at this point (it happens when receiving a CANCEL order from external GUI)
            # else:
            #     # The command has been cancelled on the dispatcher but update from RN only arrives now
            #     log.warning("Status update for %d (%d) from %s but command is currently assigned." % (commandId, int(dct['status']), renderNodeName))

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
            command.stats = dct["stats"]

    def queueWorkload(self, workload):
        self.queue.put(workload)
