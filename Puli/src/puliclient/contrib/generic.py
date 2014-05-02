'''
Created on Sep 28, 2011

@author: Arnaud Chassagne
'''
import os
import time

from puliclient.jobs import TaskDecomposer, CommandRunner, StringParameter, IntegerParameter
from puliclient.contrib.helper.helper import PuliActionHelper

START = "start"
END = "end"
STEP = "step"
PACKET_SIZE = "packetSize"
CMD = "cmd"


class GenericDecomposer(TaskDecomposer):
    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.generic.GenericRunner"

        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)

    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class GenericRunner(CommandRunner):
    cmd = StringParameter( mandatory = True )
    timeout = IntegerParameter( default=0 , min=0 )

    def execute(self, arguments, updateCompletion, updateMessage, updateStats ):
        # init the helper
        helper = PuliActionHelper(cleanTemp=True)
        cmd = arguments[CMD]

        print 'Running command "%s"' % cmd
        updateCompletion(0.0)

        if int(arguments['timeout']) == 0:
            helper.execute( cmd.split(" "), env=os.environ )
        else:
            helper.executeWithTimeout( cmd.split(" "), env=os.environ, timeout=timeout )

        # helper.execute(cmd.split(" "), env=os.environ)
        updateCompletion(1)
