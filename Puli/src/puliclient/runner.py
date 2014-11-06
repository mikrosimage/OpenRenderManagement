#!/usr/bin/python2.6
# -*- coding: utf8 -*-
"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2013, Mikros Image"

import site
import os

from puliclient.jobs import JobTypeImportError
from puliclient.jobs import CommandDone
from puliclient.jobs import CommandRunner
from puliclient.jobs import StringParameter

try:
    import simplejson as json
except ImportError:
    import json

class CallableRunner(CommandRunner):
    execType = StringParameter(mandatory=True)

    def execute(self, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        self.log.debug("Callable runner arguments: %s" % arguments)

        execType = arguments['execType']

        #
        # Add path to PYTHONPATH
        # Use site.addsitedir and pth ?
        #
        # TODO instead, we should start rez with a script to load the callable
        try:
            self.log.info("Add site dir: %s" % arguments['sysPath'])
            # paths = arguments['sysPath'].split(os.pathsep)
            print "type path:%s" % type(arguments['sysPath'])

            for path in arguments['sysPath']:
                self.log.info("Adding site path: %s" % path)
                site.addsitedir(path)

            # TEST HACK
            # site.addsitedir('/s/prods/mikros_test/jsa/exec')
            # self.log.info("Add site dir: /s/prods/mikros_test/jsa/exec")
        except Exception, e:
            raise JobTypeImportError(e)

        # raise CommandDone
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

        # try:
        #     self.callable = arguments['callable']
        #     self.params = arguments['params']
        # except Exception, e:
        #     self.log.error("Error retrieving callable details: %s" % e)
        #     raise CommandError(e)

        # self.log.info("Preparing to execute:")
        # self.log.info("  - callable: %s" % self.callable)
        # self.log.info("  - params: %s" % self.params)

        # #
        # # Check callable type
        # #
        # import inspect
        # if inspect.isfunction(self.callable):
        #     try:
        #         self.callable(*self.params)
        #     except (CommandDone, CommandError) as e:
        #         raise e
        #     except Exception, e:
        #         print "Problem when executing: %s (error: %s)" % (self.callable, e)
        #         raise CommandError("Problem when executing: %s" % self.callable)

        # elif inspect.ismethod(self.callable):
        #     #
        #     # Import module with given moduleName and funcName
        #     # NB: replace with import_lib.import in python2.7
        #     #
        #     try:
        #         # (modName, className) = self.callable.__name__
        #         # module = __import__(moduleName, fromlist="funcName")
        #         myModule = inspect.getmodule(self.callable)
        #         myModuleName = inspect.getmodule(self.callable).__name__
        #     except ImportError, error:
        #         traceback.print_exc()
        #         raise JobTypeImportError("No module '%s' on PYTHONPATH:\n%s. (%s)" % (myModuleName, "\n".join(sys.path), error))

        #     myClassName = inspect.getmro(self.callable.im_class)[0].__name__
        #     # myClass = inspect.getmro(self.callable.im_class)[0]
        #     myClass = getattr(myModule, myClassName)
        #     myMethodName = self.callable.__name__

        #     self.log.info("moduleName = %s" % myModuleName)
        #     self.log.info("className = %s" % myClassName)
        #     self.log.info("methodName = %s" % myMethodName)

        #     # old style
        #     module = __import__(myModuleName, fromlist=[myClassName])
        #     jobtype = getattr(module, myClassName)
        #     execClass = jobtype()

        #     getattr(execClass, myMethodName)(*self.params)


        #     # getattr(myClass, myMethodName)(*self.params)
        # else:
        #     raise CommandError("Callable not supported")
