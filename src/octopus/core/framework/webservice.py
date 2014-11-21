#!/usr/bin/env python
####################################################################################################
# @file webservice.py
# @package
# @author Arnaud Chassagne, Jean-Baptiste Spieser
# @date 2008/12/01
# @version 0.1
#
# @mainpage
#
####################################################################################################

import logging
import re

from octopus.core.communication.http import Http404
from methodmapper import MethodMapper

logger = logging.getLogger('main.framework.webservice')


## This class puts in correlation a regular expression and a callback method.
#
class Mapping(object):

    def __init__(self, regexp, callback):
        self.regexp = re.compile(regexp)
        if callable(callback):
            self.callback = callback
        elif isinstance(callback, dict):
            self.callback = MethodMapper(**callback)
        else:
            raise RuntimeError("callback must be a callable or a dict mapping http methods to callables")

    ## Calls the callback method coresponding to the provided path, if it exists.
    # @param path the path to match against an existing regexp
    #
    def match(self, request, path):
        retval = re.match(self.regexp, path)
        if retval:
            args = retval.groups()
            kwargs = retval.groupdict()
            if kwargs:
                return self.callback(request, **kwargs)
            else:
                return self.callback(request, *[arg if arg is not None else "" for arg in args])
        else:
            return None


## This class defines a set of mappings.
#
class MappingSet(object):

    def __init__(self, *mappings):
        self.mappings = []
        for mapping, callback in mappings:
            self.add(Mapping(mapping, callback))

    ## Adds the provided mapping to the set.
    # @param mapping the mapping object to add
    #
    def add(self, mapping, *moremappings):
        if isinstance(mapping, Mapping):
            self.mappings.append(mapping)
        elif isinstance(mapping, tuple):
            path, callback = mapping
            self.mappings.append(Mapping(path, callback))
        for onemoremapping in moremappings:
            self.add(onemoremapping)

    ##
    #
    def match(self, request, path=None):
        if path is None:
            path = request.path
        for mapping in self.mappings:
            response = mapping.match(request, path)
            if response:
                return response
        else:
            return Http404()

