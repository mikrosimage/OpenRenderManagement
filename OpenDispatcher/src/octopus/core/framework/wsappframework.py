#!/usr/bin/env python
####################################################################################################
# @file wsappframework.py
# @package
# @author acs, jbs
# @date 2008/12/01
# @version 0.1
#
# @mainpage
#
####################################################################################################

from __future__ import with_statement

import time
import threading
import logging

from octopus.core.framework.mainloopapplication import MainLoopApplication
from octopus.core.framework.ticket import Ticket
from tornado.web import Application
from tornado.ioloop import IOLoop
from threading import Thread

LOGGER = logging.getLogger("framework")


## This class represents the Application Framework based on a webservice.
#
class WSAppFramework(object):
    application = None
    webService = None
    data = None

    ## Constructs a new framework.
    #
    # @param applicationClass The class corresponding to the main loop application
    # @param webserviceClass The class corresponding to the webservice
    #
    def __init__(self, applicationClass=MainLoopApplication, webServiceClass=Application, port=8000):
        self.port = port
        self.application = applicationClass(self)
        self.webService = webServiceClass(self, port)
        self.stopFlag = False
        self.orders = []
        self.lock = threading.RLock()
        self.tickets = {}

    ## The main loop of the framework.
    #
    def mainLoop(self):
        # prepare the application (fetches db data)
        try:
            self.application.prepare()
        except KeyboardInterrupt:
            return
        # launch the webservice
        PuliTornadoServer().start()
        # enter the main loop
        self.stopFlag = False
        while not self.stopFlag:
            try:
                self.loop()
            except KeyboardInterrupt:
                self.stop()
                break

    def loop(self):
        # call the application's main loop
        self.application.mainLoop()
        with self.lock:
            self.executeOrders()
            self.cleanTickets()

    def cleanTickets(self, ttl=60):
        '''Removes stale tickets'''
        now = time.time()
        tickets = [ticket for ticket in self.tickets.values() if (now - ticket.updateTimestamp) > ttl]
        for ticket in tickets:
            del self.tickets[ticket.id]

    def stop(self):
        PuliTornadoServer().stop()
        self.stopFlag = True
        self.application.stop()

    def runOrder(self, method, ticket, *args, **kwargs):
        try:
            method(ticket, *args, **kwargs)
        except Exception:
            message = "order %r failed in exception" % method
            LOGGER.exception(message)
            ticket.status = ticket.ERROR
            ticket.message = message

    def addOrder(self, method, *args, **kwargs):
        ticket = Ticket()
        self.tickets[ticket.id] = ticket
        try:
            self.addAction(self.runOrder, method, ticket, *args, **kwargs)
        except Exception:
            pass
        return ticket

    def addAction(self, action, *args, **kwargs):
        with self.lock:
            self.orders.append([action, args, kwargs])

    def executeOrders(self):
        with self.lock:
            count = len(self.orders)
        for (action, args, kwargs) in self.orders[:count]:
            action(*args, **kwargs)
        with self.lock:
            del self.orders[:count]


class PuliTornadoServer(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)

    def run(self):
        IOLoop.instance().start()

    def stop(self):
        IOLoop.instance().stop()
