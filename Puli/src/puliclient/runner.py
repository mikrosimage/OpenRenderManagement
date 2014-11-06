#!/usr/bin/python2.6
# -*- coding: utf8 -*-
"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2013, Mikros Image"

import site
import sys
import traceback

try:
    import simplejson as json
except ImportError:
    import json

from puliclient.jobs import JobTypeImportError
from puliclient.jobs import CommandError
from puliclient.jobs import CommandRunner
from puliclient.jobs import StringParameter


class CallableRunner(CommandRunner):
    execType = StringParameter(mandatory=True)

    def execute(self, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        self.log.debug("Callable runner arguments: %s" % arguments)

        execType = arguments['execType']

        #
        # Add path to PYTHONPATH
        #
        # TODO instead, we should start rez with a script to load the callable
        try:
            self.log.info("Add site dir: %s" % arguments['sysPath'])
            for path in arguments['sysPath']:
                site.addsitedir(path)

        except Exception, e:
            raise JobTypeImportError(e)

        #
        # Execute simple function
        #
        if execType == 'function':

            try:
                moduleName = arguments['moduleName']
                funcName = arguments['funcName']
            except Exception:
                raise JobTypeImportError("Missing function or module name in callable arguments: %s", arguments)

            self.log.info("Preparing to load: %s from %s" % (funcName, moduleName))

            #
            # Import module with given moduleName and funcName
            # NB: replace with import_lib.import in python2.7
            #
            try:
                module = __import__(moduleName, fromlist=funcName)
            except ImportError, error:
                traceback.print_exc()
                raise JobTypeImportError("No module '%s' on PYTHONPATH:\n%s. (%s)" % (moduleName, "\n".join(sys.path), error))

            #
            # Load target function in module
            #
            try:
                func = getattr(module, funcName)
            except Exception, e:
                raise JobTypeImportError("No function '%s' defined in module %s (%s)" % (funcName, moduleName, e))

            #
            # Go!
            #
            try:
                params = json.loads(arguments.get("params", None))
                self.log.info("params: %s" % params)
                func(**params)
            except Exception, e:
                print "Problem when executing: %s (error: %s)" % (func, e)
                raise CommandError("Problem when executing: %s" % func)

        #
        # Execute instance method
        #
        elif execType == 'method':
            self.log.debug("input: %s" % arguments)
            try:
                moduleName = arguments['moduleName']
                className = arguments['className']
                methodName = arguments['methodName']
            except Exception:
                raise JobTypeImportError("Missing function or module name in callable arguments: %s", arguments)

            self.log.info("Preparing to load: %s.%s from %s" % (methodName, className, moduleName))

            module = __import__(moduleName, fromlist=[className])
            jobtype = getattr(module, className)
            func = jobtype()

            #
            # Go!
            #
            try:
                params = json.loads(arguments.get("params", None))
                self.log.info("params: %s" % params)

                getattr(func, methodName)(**params)
            except Exception, e:
                print "Problem when executing: %s (error: %s)" % (func, e)
                raise CommandError("Problem when executing: %s" % func)

        else:
            raise CommandError("Callable not supported")
