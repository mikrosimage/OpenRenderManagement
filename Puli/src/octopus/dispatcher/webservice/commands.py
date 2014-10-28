

try:
    import simplejson as json
except ImportError:
    import json

from tornado.web import HTTPError
import tornado

from octopus.core.enums.command import *
from octopus.core.framework import queue
from octopus.core.communication.http import Http404, Http400, Http500, HttpConflict

from octopus.dispatcher.model.nodequery import IQueryNode
import logging
import time

logger = logging.getLogger('main.dispatcher.webservice')

__all__ = ['CommandsResource', 'CommandResource']

ALLOWED_STATUS_VALUES = (CMD_READY, CMD_DONE, CMD_CANCELED)


from octopus.dispatcher.webservice import DispatcherBaseResource


class CommandsResource(DispatcherBaseResource):
    def get(self):
        commands = self.framework.application.dispatchTree.commands.values()
        commandRepresentations = [command.to_json() for command in commands]
        commandRepresentations = json.dumps(commandRepresentations)
        self.writeCallback(commandRepresentations)


class CommandResource(DispatcherBaseResource):
    #@queue
    def get(self, commandId):
        try:
            id = int(commandId)
            command = self._findCommand(id)
            rep = command.to_json()
        except KeyError:
            raise Http404("No such command. Command with id %d not found." % id)
        body = json.dumps(rep)
        self.writeCallback(body)

    #@queue
    def put(self, commandId):
        def work(self, commandId, toUpdate):
            commands = self.getDispatchTree().commands
            if commandId not in commands:
                return None
            command = self.getDispatchTree().commands[commandId]
            # TODO should run the following piece of code at boot...
            if command.status in [CMD_ASSIGNED, CMD_RUNNING] and command.renderNode and command.id not in command.renderNode.commands.keys():
                command.status = CMD_ERROR
                raise Http400("Invalid command state. Command has been set to error.")
            if 'description' in toUpdate:
                command.description = toUpdate['description']
            if 'arguments' in toUpdate:
                command.arguments = toUpdate['arguments']
            if 'status' in toUpdate:
                status = toUpdate['status']
                if status != command.status:
                    if status == CMD_CANCELED:
                        command.cancel()
                    elif status == CMD_READY:
                        command.setReadyStatus()
                    elif status == CMD_DONE:
                        command.setDoneStatus()
                    else:
                        raise Http400("Invalid status. Cannot set of command %d to %r" % (commandId, status))
            return "Done"

        # check requested updates
        commandId = int(commandId)
        updatedData = self.getBodyAsJSON()
        toUpdate = {}
        if 'description' in updatedData:
            toUpdate['description'] = updatedData.pop('description')
        if 'arguments' in updatedData:
            toUpdate['arguments'] = updatedData.pop('arguments')
        if 'status' in updatedData:
            toUpdate['status'] = updatedData.pop('status')
        # any remaining field is an error
        if updatedData:
            raise Http400("Invalid fields. Updating the following field(s) is not allowed: %s" % (' '.join(updatedData.keys())))
        # send work to the dispatcher
        result = work(self, commandId, toUpdate)

        if result is None:
            raise Http404('No such command %r' % commandId)
        self.writeCallback(result)

    def _findCommand(self, id):
        return self.getDispatchTree().commands[id]



class CommandQueryResource(DispatcherBaseResource, IQueryNode):
    """
    {
        id: 73,
        task: 41,

        description: "short jobs 10-15s_1_1",
        status: 5,
        completion: 1,

        nbFrames: 1
        avgTimeByFrame: 15000,
        renderNode: null,

        creationTime: 1400260393,
        startTime: 1400673405,
        endTime: 1400673420,
        updateTime: 1400673422,

        arguments: {
            start: 1,
            end: 1,
            packetSize: 1
            delay: 10,
            args: "sleep `shuf -i 10-15 -n 1`",
        },

        retryCount: 0,
        retryRnList: [ ],

        message: "",
        stats: { },
    }
    """


    ADDITIONNAL_SUPPORTED_FIELDS = []
    DEFAULT_FIELDS = ['id', 'task', 'description', 'renderNode', 'nbFrames', 'avgTimeByFrame', 'status', 'creationTime', 'startTime', 'endTime', 'updateTime', 'completion', 'attempt', 'message' ]


    def createRepr( self, pItem, pAttributes ):
        """
        Create a json representation for a given node.
        param: command to represent
        param: attributes to retrieve on each node
        return: a json dict
        """

        result = {}
        for currArg in pAttributes:
            #
            # Get value of additionnally supported field
            #
            if currArg.startswith("arguments:"):
                # Attribute name references a "argumentss" item
                caract = unicode(currArg[10:])
                value = unicode(pItem.caracteristics.get(caract,''))
                result[caract] = value

            elif currArg == "task":
                result[currArg] =  pItem.task.id

            elif currArg == "renderNode":

                if pItem.renderNode is not None:
                    result[currArg] =  pItem.renderNode.name
                else:
                    result[currArg] = 'undefined'
            else:
                # Attribute is a standard attribute of a Node
                result[currArg] =  getattr(pItem, currArg, 'undefined')

        return result



    @tornado.web.asynchronous
    def get(self):
        """
        Handle user query request.
          1. init timer and result struct
          2. check attributes to retrieve
          3. limit item list regarding the given query filters
          4. for each filtered node: add info in result
        """
        args = self.request.arguments

        self.command = self.commandGenerator( args )
        tornado.ioloop.IOLoop.instance().add_callback(self.loop)


    def loop( self ):
        try: 
            self.command.next()
            tornado.ioloop.IOLoop.instance().add_callback(self.loop)
        except StopIteration:
            self.finish()


    def commandGenerator( self, args ):
        """
        Generator that fectches all items corresponding to the given args.
        """
        try:
            start_time = time.time()
            resultData = []
            filteredCommands = []

            commands = self.getDispatchTree().commands.values()
            totalItems = len(commands)

            #
            # --- Check if display attributes are valid
            #     We handle 2 types of attributes: 
            #       - simple node attributes
            #       - additionnal node attributes (no verification, it is not mandatory)
            #
            if 'attr' in args:
                for currAttribute in args['attr']:
                    if not hasattr(commands[0],currAttribute):
                        if currAttribute not in CommandQueryResource.ADDITIONNAL_SUPPORTED_FIELDS :
                            logger.warning('Error retrieving data, invalid attribute requested : %s', currAttribute )
                            raise HTTPError( 500, "Invalid attribute requested:"+str(currAttribute) )
            else:
                # Using default result attributes
                args['attr'] = CommandQueryResource.DEFAULT_FIELDS

            #
            # --- filtering
            #
            for i, cmd in enumerate(self.filterCommands( args, commands )):
                filteredCommands.append( cmd )
                # time.sleep(0.05)
                yield i

            #
            # --- Prepare the result json object
            #
            for currNode in filteredCommands:
                currItem = self.createRepr( currNode, args['attr'] )
                resultData.append( currItem )

            content = { 
                        'summary': 
                            { 
                            'count':len(filteredCommands), 
                            'totalInDispatcher':totalItems, 
                            'requestTime':time.time() - start_time,
                            'requestDate':time.ctime()
                            }, 
                        'items':resultData 
                        }

            #
            # --- Create response and return. Request termination will be handled in handler.
            #
            self.write( json.dumps(content) )

        except HTTPError, e:
            raise e
        except AssertionError, e:
            logger.warning('Arrrgh')
            raise e
        except Exception, e:
            logger.warning('Impossible to retrieve query result: %s - %r', self.request.uri, e)
            raise HTTPError( 500, "Internal error")
