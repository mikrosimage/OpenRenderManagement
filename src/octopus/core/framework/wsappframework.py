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
    def __init__(self, applicationClass=MainLoopApplication, webServiceClass=Application, port=8000, journalDir="/tmp/"):
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
#        self.webService.start()
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
            
            
    ## Executes the orders contained in the framework app.
    #
    # @note executeOrders is executed while self.framework.lock is acquired.
#    def executeOrders(self):
#
#        orderCount = len(self.orders)
#        for orderNumber, order in enumerate(self.orders[:orderCount]):
#            LOGGER.debug("Executing order : %s", self.orders[0])
#            t = self.orders.pop(0)
#            method, ticket, args, kwargs = t
#            try:
#                method(ticket, *args,**kwargs)
#            except Exception:
#                message = "order %s failed in exception" % ((method, ticket, args, kwargs),)
#                LOGGER.exception(message)
#                ticket.status = ticket.ERROR
#                ticket.message = message


    def cleanTickets(self, ttl=60):
        '''Removes stale tickets'''
        now = time.time()
        tickets = [ticket for ticket in self.tickets.values() if (now - ticket.updateTimestamp) > ttl]
#        if tickets:
#            LOGGER.debug('removing %d stale tickets', len(tickets))
        for ticket in tickets:
            del self.tickets[ticket.id]

    def stop(self):
        self.webService.stop()
        self.stopFlag = True
        self.application.stop()

    ## Adds the provided order to the list of orders.
    #
    # @param order the order to add
    #
#    def addOrder(self, method, *args,**kwargs):
#        LOGGER.debug("Posting new order (%s, %s, %s)" % (method.__name__, args, kwargs))
#
#        ticket = Ticket()
#        self.tickets[ticket.id] = ticket
#
#        # BEGIN CRITICAL SECTION
#        with self.lock:
#            self.orders.append([method, ticket, args, kwargs])
#        # END CRITICAL SECTION
#        
#        return ticket

    def runOrder(self, method, ticket, *args, **kwargs):
#        LOGGER.debug("Executing order : %r", method)
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
