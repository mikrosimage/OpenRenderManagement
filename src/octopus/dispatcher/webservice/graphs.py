
import logging

from octopus.core.communication import *

from octopus.core.framework import BaseResource, queue

logger = logging.getLogger("dispatcher.webservice")


class GraphesResource(BaseResource):
    @queue
    def post(self):
        try:
            nodes = self.dispatcher.handleNewGraphRequestApply(self.getBodyAsJSON())
        except Exception, e:
            logger.exception("Graph submission failed")
            return Http500("Failed. %s" % str(e))
        host, port = self.getServerAddress()
        import socket
        try:
            host = socket.gethostbyaddr(host)
        except socket.herror:
            host = socket.gethostname()
        self.set_header('Location', 'http://%s:%s/nodes/%d' % (host, port, nodes[0].id))
        self.set_status(201)
        self.writeCallback("Graph created.\nCreated nodes: %s" % (",".join([str(node.id) for node in nodes])))
