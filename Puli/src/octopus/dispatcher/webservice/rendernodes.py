from __future__ import with_statement

import time
try:
    import simplejson as json
except ImportError:
    import json
import logging
from tornado.web import HTTPError

from octopus.core.communication import HttpResponse, Http400, Http404, Http403, HttpConflict
# from octopus.core.enums.rendernode import RN_PAUSED, RN_IDLE, RN_UNKNOWN, RN_BOOTING, RN_ASSIGNED
from octopus.core.enums.rendernode import *

from octopus.core import enums, singletonstats, singletonconfig
from octopus.dispatcher.model import RenderNode
from octopus.core.framework import queue
from octopus.dispatcher.webservice import DispatcherBaseResource


logger = logging.getLogger("dispatcher")


class RenderNodesResource(DispatcherBaseResource):
    """
    Lists the render nodes known by the dispatcher.
    :param: request the HTTP request
    """

    def get(self):
        rendernodes = self.getDispatchTree().renderNodes.values()
        content = {'rendernodes': list(rendernode.to_json() for rendernode in rendernodes)}
        content = json.dumps(content)
        self.writeCallback(content)


class RenderNodeResource(DispatcherBaseResource):
    ## Sends the JSON detailed representation of a given render node, url: http://server:8004/rendernodes/<rn:port>
    # 
    # @param request the HTTP request object for this request
    # @param computerName the name of the requested render node
    #
    def get(self, computerName):
        computerName = computerName.lower()
        try:
            rendernode = self.getDispatchTree().renderNodes[computerName]
        except KeyError:
            return Http404("RenderNode not found")
        content = rendernode.to_json()
        content = json.dumps(content)
        self.writeCallback(content)

    def post(self, computerName):
        """
        A worker send a request to get registered on the server.
        """
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleCounts['add_rns'] += 1

        computerName = computerName.lower()
        if computerName.startswith(('1', '2')):
            return Http403(message="Cannot register a RenderNode without a name", content="Cannot register a RenderNode without a name")

        dct = self.getBodyAsJSON()

        if computerName in self.getDispatchTree().renderNodes:
            # When the registering worker is already listed in RN list
            logger.warning("RenderNode already registered.")
            existingRN = self.getDispatchTree().renderNodes[computerName]

            if 'commands' not in dct:
                logger.warning("No commands in current RN, reset command that might be still assigned to this RN")
                existingRN.reset()
            else:
                for cmdId in dct['commands']:
                    existingRN.commands[cmdId] = self.getDispatchTree().commands[cmdId]

            if 'status' in dct:
                existingRN.status = int(dct['status'])

            return HttpResponse(304, "RenderNode already registered.")

        else:
            # Add a new worker (and set infos given in request body)
            for key in ('name', 'port', 'status', 'cores', 'speed', 'ram', 'pools', 'caracteristics'):
                if not key in dct:
                    return Http400("Missing key %r" % key, content="Missing key %r" % key)
            port = int(dct['port'])
            status = int(dct['status'])
            if status not in (RN_UNKNOWN, RN_PAUSED, RN_IDLE, RN_BOOTING):
                # FIXME: CONFLICT is not a good value maybe
                return HttpConflict("Unallowed status for RenderNode registration")
            cores = int(dct['cores'])
            speed = float(dct['speed'])
            ram = int(dct['ram'])
            pools = dct['pools']
            caracteristics = dct['caracteristics']
            name, port = computerName.split(":", 1)

            puliversion = dct.get('puliversion',"unknown")
            createDate = dct.get('createDate',time.time())

            renderNode = RenderNode(None, computerName, cores, speed, name, port, ram, caracteristics, puliversion=puliversion, createDate=createDate)

            renderNode.status = status
            poolList = []
            # check the existence of the pools
            for poolName in pools:
                try:
                    pool = self.getDispatchTree().pools[poolName]
                    poolList.append(pool)
                except KeyError:
                    return HttpConflict("Pool %s is not a registered pool", poolName)
            # add the rendernode to the pools
            for pool in poolList:
                pool.addRenderNode(renderNode)
            # add the rendernode to the list of rendernodes
            renderNode.pools = poolList
            self.getDispatchTree().renderNodes[renderNode.name] = renderNode
            self.writeCallback(json.dumps(renderNode.to_json()))

    #@queue
    def put(self, computerName):
        computerName = computerName.lower()
        try:
            renderNode = self.getDispatchTree().renderNodes[computerName]
        except KeyError:
            return Http404("RenderNode not found")
        dct = self.getBodyAsJSON()
        for key in dct:
            if key == "cores":
                renderNode.coresNumber = int(dct["cores"])
            elif key == "speed":
                renderNode.speed = float(dct["speed"])
            elif key == "ram":
                renderNode.ramSize = int(dct["ram"])
            else:
                return Http403("Modifying %r attribute is not authorized." % key)
        self.writeCallback(json.dumps(renderNode.to_json()))

    # Removes a RenderNode from the dispatchTree and all pools.
    # Also call RN's reset method to remove assigned commands.
    #
    # @param request the HTTP request object for this request
    # @param computerName the name of the requested render node
    #
    #@fqdn_request_decorator
    #@queue
    def delete(self, computerName):
        computerName = computerName.lower()

        try:
            renderNode = self.getDispatchTree().renderNodes[computerName]
        except KeyError:
            return Http404("RenderNode not found")
        if renderNode.status in [RN_ASSIGNED, RN_WORKING] :
            renderNode.reset()
        
        for pool in self.getDispatchTree().pools.values():
            pool.removeRenderNode(renderNode)
        renderNode.remove()


class RenderNodeCommandsResource(DispatcherBaseResource):
    #@queue
    def put(self, computerName, commandId):
        '''Update command `commandId` running on rendernode `renderNodeId`.

        Returns "200 OK" on success, or "404 Bad Request" if the provided json data is not valid.
        '''

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleCounts['update_commands'] += 1

        computerName = computerName.lower()
        # try:
        #     updateDict = self.sanitizeUpdateDict(self.getBodyAsJSON())
        # except TypeError, e:
        #     return Http400(repr(e.args))
        updateDict = self.getBodyAsJSON()
        updateDict['renderNodeName'] = computerName

        try:
            self.framework.application.updateCommandApply(updateDict)
        except KeyError, e:
            return Http404(str(e))
        except IndexError, e:
            return Http404(str(e))
        self.writeCallback("Command updated")

    # def sanitizeUpdateDict(self, dct):
    #     res = {}
    #     values = (('id', lambda val: isinstance(val, int)),
    #               ('status', lambda val: isinstance(val, int)),
    #               ('message', lambda val: isinstance(val, basestring)),
    #               ('completion', lambda val: isinstance(val, int) or isinstance(val, float)),
    #               ('validatorMessage', lambda val: isinstance(val, basestring)),
    #               ('errorInfos', lambda val: isinstance(val, dict)),
    #               )
    #     for name, valuetype in values:
    #         if name in dct:
    #             value = dct[name]
    #             if not valuetype(value):
    #                 raise TypeError(name, value)
    #             res[name] = value
    #     return res

    #@queue
    def delete(self, computerName, commandId):
        computerName = computerName.lower()
        commandId = int(commandId)
        try:
            computer = self.framework.application.dispatchTree.renderNodes[computerName]
        except KeyError:
            return HTTPError(404, "No such RenderNode")

        try:
            command = computer.commands[commandId]
        except KeyError:
            return HTTPError(404, "No such command running on this RenderNode")

        if command.id not in computer.commands:
            return HTTPError(400, "Command %d not running on RenderNode %s" % (command.id, computer.name))
        else:
            if enums.command.isFinalStatus(command.status):
                if enums.command.CMD_DONE == command.status:
                    command.completion = 1.0
                command.finish()
                msg = "Command %d removed successfully." % commandId
                self.writeCallback(msg)
            else:
                # command.cancel() ??? dans ce cas c'est pas ce qu'on devrait faire ??? FIXME
                message = "Cannot remove a running command from a RenderNode."
                return HTTPError(403, message)


class RenderNodeSysInfosResource(DispatcherBaseResource):
    #@queue
    def put(self, computerName):
        computerName = computerName.lower()
        rns = self.getDispatchTree().renderNodes

        if not computerName in rns:
            raise Http404("RenderNode not found")

        dct = self.getBodyAsJSON()
        renderNode = rns[computerName]
        if "puliversion" in dct:
            renderNode.puliversion = dct.get('puliversion',"unknown")
        if "caracteristics" in dct:
            renderNode.caracteristics = eval(str(dct["caracteristics"]))
        if "cores" in dct:
            renderNode.cores = int(dct["cores"])
        if "createDate" in dct:
            renderNode.createDate = int(dct["createDate"])
        if "ram" in dct:
            renderNode.ram = int(dct["ram"])
        if "systemFreeRam" in dct:
            renderNode.systemFreeRam = int(dct["systemFreeRam"])
        if "systemSwapPercentage" in dct:
            renderNode.systemSwapPercentage = float(dct["systemSwapPercentage"])
        if "speed" in dct:
            renderNode.speed = float(dct["speed"])
        if "performance" in dct:
            renderNode.performance = float(dct["performance"])
        if "status" in dct:
            if renderNode.status == RN_UNKNOWN:
                # if int(dct["status"]) == RN_PAUSED:
                #     renderNode.status = RN_PAUSED
                # else:
                    #renderNode.status = RN_IDLE
                renderNode.status = int(dct["status"])
                logger.info("status reported is %d" % renderNode.status)

            # if renderNode.status != int(dct["status"]):
            #     logger.warning("The status reported by %s = %r is different from the status on dispatcher %r" % (renderNode.name, RN_STATUS_NAMES[dct["status"]],RN_STATUS_NAMES[renderNode.status]))

        if "isPaused" in dct and "status" in dct:
            logger.debug("reported for %r: remoteStatus=%r remoteIsPaused=%r" % (renderNode.name, RN_STATUS_NAMES[dct["status"]], dct['isPaused']) )

        renderNode.lastAliveTime = time.time()
        renderNode.isRegistered = True


class RenderNodesPerfResource(DispatcherBaseResource):
    """
    Sets a performance index (float) for one or several given rendernode names
    TOFIX: might not be actually used, need to verify
    """
    #@queue
    def put(self):
        dct = self.getBodyAsJSON()
        for computerName, perf in dct.items():
            renderNode = self.getDispatchTree().renderNodes[computerName]
            renderNode.performance = float(perf)
        self.writeCallback("Performance indexes have been set.")


class RenderNodeResetResource(DispatcherBaseResource):
    #@queue
    def put(self, computerName):
        computerName = computerName.lower()
        rns = self.getDispatchTree().renderNodes
        if not computerName in rns:
            return Http404("RenderNode not found")
        dct = self.getBodyAsJSON()
        renderNode = rns[computerName]
        noMoreCmd = int(dct["nomorecmd"])
        if noMoreCmd:
            renderNode.reset()


class RenderNodeQuarantineResource(DispatcherBaseResource):
    def put(self):
        """
        Used to set a quarantine on a list of rendernodes. Quarantine rns have a flag "excluded"
        that prevent them to be considered in assignement process.
        example: curl -d '{"quarantine":true,"rns":["vfxpc64:9005"]}' -X PUT "http://pulitest:8004/rendernodes/quarantine/"
        """

        dct = self.getBodyAsJSON()
        quarantine = dct["quarantine"]

        rns = self.getDispatchTree().renderNodes
        for computerName in dct["rns"]:

            if computerName not in rns:
                logger.warning("following RN '%s' is not referenced, ignoring..." % computerName)
                continue

            renderNode = rns[computerName]
            renderNode.excluded = quarantine

            if not quarantine:
                renderNode.history.clear()
                renderNode.tasksHistory.clear()
        self.writeCallback("Quarantine attributes set.")


class RenderNodePausedResource(DispatcherBaseResource):
    #@queue
    def put(self, computerName):
        dct = self.getBodyAsJSON()
        paused = dct['paused']
        killproc = dct['killproc']
        computerName = computerName.lower()
        rns = self.getDispatchTree().renderNodes
        if not computerName in rns:
            return Http404("RenderNode not found")
        renderNode = rns[computerName]
        if paused:
            renderNode.status = RN_PAUSED
            if killproc:
                renderNode.reset(paused=True)
        else:
            # FIXME maybe set this to RN_FINISHING ?
            renderNode.status = RN_IDLE
            renderNode.excluded = False
