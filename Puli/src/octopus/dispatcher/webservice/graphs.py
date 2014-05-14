
import logging

from octopus.core.communication import *
from octopus.core import singletonconfig, singletonstats
from octopus.core.framework import queue
from octopus.dispatcher.webservice import DispatcherBaseResource

logger = logging.getLogger("dispatcher.webservice")


class GraphesResource(DispatcherBaseResource):
    # @queue
    def post(self):

        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleCounts['add_graphs'] += 1

        try:
            nodes = self.dispatcher.handleNewGraphRequestApply(self.getBodyAsJSON())
        except Exception, e:
            logger.exception("Graph submission failed")
            raise Http500("Failed. %s" % str(e))

        host, port = self.getServerAddress()
        # import socket
        # try:
        #     host = socket.gethostbyaddr(host)
        # except socket.herror:
        #     host = socket.gethostname()

        self.set_header('Location', 'http://%s:%s/nodes/%d' % (host, port, nodes[0].id))
        self.set_status(201)
        self.writeCallback("Graph created.\nCreated nodes: %s" % (",".join([str(node.id) for node in nodes])))
        self.finish()
