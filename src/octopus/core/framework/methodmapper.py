#!/usr/bin/env python
####################################################################################################
# @file methodmapper.py
# @package octopus.core.framework
# @author Olivier Derpierre
# @date 2008/21/01
# @version 0.1
#
# @mainpage
#
####################################################################################################

import logging

from octopus.core.communication.http import Http405, Http500

logger = logging.getLogger('main.dispatcher.webservice')


## Helper class for URI to method routing.
#
class MethodMapper():

    def __init__(self, **kwargs):
        self.mappingDict = dict(((method, callback) for method, callback in kwargs.items()))

    def __call__(self, request, *args, **kwargs):
        try:
            method = self.mappingDict[request.command]
        except KeyError:
            return Http405(allowed=self.mappingDict.keys())
        try:
            return method(request, *args, **kwargs)
        except Exception:
            logger.exception('Unexpected exception caught while routing url "%s" for method "%s"' % (request.path, request.command))
            return Http500()
