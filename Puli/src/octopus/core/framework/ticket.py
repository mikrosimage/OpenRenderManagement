#!/usr/bin/env python
####################################################################################################
# @file ticket.py
# @package octopus.core.framework
# @author Olivier Derpierre
# @date 2009/02/05
# @version 0.1
#
# @warning DO NOT CLOSE A TICKET BEFORE ITS ASSOCIATED ACTION'S STRUCTURES HAVE BEEN UPDATED.
#
####################################################################################################

from octopus.core.communication import JSONResponse
import uuid
import time


class Ticket(object):

    STATUS = (OPENED, CLOSED, ERROR) = ('OPENED', 'CLOSED', 'ERROR')

    _status = OPENED

    def __init__(self, id=None, status=OPENED, message='', resultURL=None):
        if id is None:
            id = str(uuid.uuid4())
        else:
            id = str(uuid.UUID(str(id)))
        self.id = id
        self.status = status
        self.message = message
        self.resultURL = resultURL
        self.updateTimestamp = time.time()

    def _setStatus(self, status):
        self._status = status
        self.updateTimestamp = time.time()

    def _getStatus(self):
        return self._status

    status = property(_getStatus, _setStatus)

    def __repr__(self):
        return 'Ticket(%s, %s, %s, %s)' % (repr(self.id), repr(self.status), repr(self.message), repr(self.resultURL))


class TicketResponse(JSONResponse):

    def __init__(self, ticket):
        from octopus.dispatcher.model.representations import TicketRepresentation
        JSONResponse.__init__(self, 202, "Order queued", {'ticket': TicketRepresentation(ticket)})
