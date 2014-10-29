#!/usr/bin/env python
####################################################################################################
# @file mainloopapplication.py
# @package
# @author Arnaud Chassagne, Jean-Baptiste Spieser
# @date 2008/12/01
# @version 0.1
#
# @mainpage
#
####################################################################################################

import logging
LOGGER = logging.getLogger("main.framework.application")


## This class defines the main loop application (for example : the dispatcher).
#
class MainLoopApplication(object):

    ## Constructs a new MainLoopApplication.
    #
    # @param framework the application framework instance
    #
    def __init__(self, framework):
        self.framework = framework

    ## the main loop.
    #
    def mainLoop(self):
        raise NotImplementedError

    ## Prepares this object by fetching the data from the database.
    #
    def prepare(self):
        raise NotImplementedError

    def stop(self):
        pass
