

try:
    import simplejson as json
except ImportError:
    import json

from octopus.core.enums.command import *
from octopus.core.framework import BaseResource, queue
from octopus.core.communication.http import Http404, Http400

__all__ = ['CommandsResource', 'CommandResource']

ALLOWED_STATUS_VALUES = (CMD_READY, CMD_DONE, CMD_CANCELED)


class CommandsResource(BaseResource):
    def get(self):
        commands = self.framework.application.dispatchTree.commands.values()
        commandRepresentations = [command.to_json() for command in commands]
        commandRepresentations = json.dumps(commandRepresentations)
        self.writeCallback(commandRepresentations)


class CommandResource(BaseResource):
    @queue
    def get(self, commandId):
        try:
            id = int(commandId)
            command = self._findCommand(id)
            rep = command.to_json()
        except KeyError:
            return Http404("No such command. Command with id %d not found." % id)
        body = json.dumps(rep)
        self.writeCallback(body)

    @queue
    def put(self, commandId):
        def work(self, commandId, toUpdate):
            commands = self.getDispatchTree().commands
            if commandId not in commands:
                return None
            command = self.getDispatchTree().commands[commandId]
            # TODO should run the following piece of code at boot...
            if command.status in [CMD_ASSIGNED, CMD_RUNNING] and command.renderNode and command.id not in command.renderNode.commands.keys():
                command.status = CMD_ERROR
                return Http400("Invalid command state. Command has been set to error.")
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
                    else:
                        return Http400("Invalid status. Cannot set of command %d to %r" % (commandId, status))
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
            return Http400("Invalid fields. Updating the following field(s) is not allowed: %s" % (' '.join(updatedData.keys())))
        # send work to the dispatcher
        result = work(self, commandId, toUpdate)

        if result is None:
            return Http404('No such command %r' % commandId)
        self.writeCallback(result)

    def _findCommand(self, id):
        return self.getDispatchTree().commands[id]
