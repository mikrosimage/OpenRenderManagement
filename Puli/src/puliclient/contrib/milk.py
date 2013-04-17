'''
Created on May 4, 2010

@author: Arnaud Chassagne
'''
from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper

import shlex

#<arguments>
PRE_ARGS = "preArgs"
MID_ARGS = "midArgs"
POST_ARGS = "postArgs"
OVERWRITE = "overwrite"
#</arguments>

#<generic>
START = "start"
END = "end"
STEP = "step"
PACKET_SIZE = "packetSize"
WIDTH = "resx"
HEIGHT = "resy"
SCENE = "filein"
OUTPUT_IMAGES = "fileout"
#</generic>


class MilkDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        task.runner = "puliclient.contrib.milk.MilkRunner"
        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)

    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd

        # default values
        if OVERWRITE not in cmdArgs:
            cmdArgs[OVERWRITE] = "1"

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class MilkRunner(CommandRunner):
    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper(cleanTemp = True)

        env = {}
        env["RLM_LICENSE"] = "2764@dispatch:2375@kiwi:5053@kiwi"

        # init log
        helper.printStartLog("milkrunner", "v1.0")

        # cmd path
        cmdArgs = [helper.mapPath("/s/apps/lin/Milk/milk")]

        # pre args
        if PRE_ARGS in arguments.keys() and len(arguments[PRE_ARGS]) != 0:
            cmdArgs.extend(shlex.split(arguments[PRE_ARGS]))

        cmdArgs += ["-fi", arguments[SCENE]]
        cmdArgs += ["-ti","%s-%s" % (arguments[START],arguments[END])]

        # middle args
        if MID_ARGS in arguments.keys() and len(arguments[MID_ARGS]) != 0:
            cmdArgs.extend(shlex.split(arguments[MID_ARGS]))

        cmdArgs += ["-to","%s-%s" % (arguments[START],arguments[END])]

        # resize arg
        if WIDTH in arguments.keys() and HEIGHT in arguments.keys() and len(str(arguments[WIDTH])) != 0 and len(str(arguments[HEIGHT])) != 0:
            cmdArgs += ["-resize", str(arguments[WIDTH]), str(arguments[HEIGHT])]

        # fileout arg
        cmdArgs += ["-fo", arguments[OUTPUT_IMAGES]]

        # post args
        if POST_ARGS in arguments.keys() and len(arguments[POST_ARGS]) != 0:
            cmdArgs.extend(shlex.split(arguments[POST_ARGS]))

        # overwrite
        if int(arguments[OVERWRITE]):
            cmdArgs.append("-overwrite")
        else:
            cmdArgs.append("-pass")

        # launch cmd
        updateCompletion(0)
        print cmdArgs
        helper.execute(cmdArgs, env)
        updateCompletion(1)
        print "done."
