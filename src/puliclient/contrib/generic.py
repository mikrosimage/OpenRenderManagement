'''
Created on Sep 28, 2011

@author: red
'''
import subprocess
from puliclient.jobs import TaskDecomposer, CommandRunner
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
    def execute(self, arguments, updateCompletion, updateMessage):
        cmd = arguments[CMD]
        print 'Running command "%s"' % cmd
        updateCompletion(0)
        subprocess.call(cmd.split(" "), close_fds=True, shell=True)
        updateCompletion(1)