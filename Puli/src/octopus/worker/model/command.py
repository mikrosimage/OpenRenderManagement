#!/usr/bin/env python
####################################################################################################
# @file command.py
# @package
# @author
# @date 2009/01/12
# @version 0.1
#
# @mainpage
#
####################################################################################################

import os
import sys
import platform

from octopus.core.enums.command import CMD_RUNNING


## This class represents a Command for the worker
#
class Command(object):

    def __init__(self, id, runner, arguments={}, validationExpression="VAL_TRUE", taskName="", relativePathToLogDir="", message="", environment={}):
        '''
        :param id: command id
        :param arguments: command arguments as a dict
        :param validationExpression: -
        :param taskName: a string representing the parent task name
        :param relativePathToLogDir: relative path to log
        :param message:
        :param environment: A dict of env vars which will be added to current os.environ
        '''
        self.status = CMD_RUNNING
        self.id = id
        self.completion = 0
        self.arguments = arguments.copy()
        self.runner = runner
        self.validationExpression = validationExpression
        self.validatorMessage = None
        self.errorInfos = None
        self.taskName = taskName
        self.relativePathToLogDir = relativePathToLogDir
        self.message = message
        self.environment = os.environ.copy()
        self.environment.update(environment)
