'''
Created on Jan 12, 2010

@author: Arnaud Chassagne
'''
import os

from puliclient.jobs import CommandRunner, TaskDecomposer
from puliclient.contrib.helper.helper import PuliActionHelper

#<arguments>
CAMERA = "cam"
RENDER_LAYERS = "rl"
FORMAT = "of"
ALPHA = "alpha"
DEPTH = "depth"
#
ARNOLD_VERSION = "arnold"
MAYA_VERSION = "maya"
NUKE_VERSION = "nuke"
AFTEREFFECTS_VERSION = "after"
SHAVE_VERSION = "shave"
CROWD_VERSION = "crowd"
#</arguments>

#<generic>
PROJECT = "proj"
START = "s"
END = "e"
STEP = "b"
OUTPUT_IMAGE = "im"
RENDER_DIR = "rd"
SCENE = "scene"
PACKET_SIZE = "packetSize"
WIDTH = "x"
HEIGHT = "y"
PADDING = "pad"
FRAMES_LIST = "framesList"
#</generic>


class MayaDecomposer(TaskDecomposer):

    def __init__(self, task):
        TaskDecomposer.__init__(self, task)
        self.task = task
        self.task.runner = "puliclient.contrib.maya.MayaRunner"

        # initializes the maps for translating arguments
        self.formatsMap = {'als': 6, 'cin': 11, 'dds': 35, 'eps': 9, 'gif': 0, 'jpg': 8, 'iff': 7, 'psd': 31,
                        'png': 32, 'yuv': 12, 'rla': 2, 'sgi': 5, 'pic': 1, 'tga': 19, 'tif': 3, 'bmp': 20}

        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)

    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd
        # handle the format argument
        cmdArgs[FORMAT] = self.formatsMap[self.task.arguments[FORMAT]]
        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class MayaRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper(cleanTemp=True)

        # convert the paths
        prodPath = helper.mapPath(arguments[PROJECT])
        arguments[RENDER_DIR] = helper.mapPath(arguments[RENDER_DIR])
        arguments[SCENE] = helper.mapPath(arguments[SCENE])

        env = helper.getEnv(am_version=arguments[ARNOLD_VERSION],
                            maya_version=arguments[MAYA_VERSION],
                            shave_version=arguments[SHAVE_VERSION],
                            crowd_version="",
                            home=os.environ["HOME"],
                            job=os.path.basename(prodPath),
                            jobdrive=os.path.dirname(prodPath),
                            applis=helper.mapPath("/s/apps/lin"),
                            use_shave=0)

        # init log
        helper.printStartLog("mayarunner", "v1.0")

        cmdArgs = helper.buildMayaCommand("MikserActionMayaRender", arguments, [prodPath, arguments[SCENE]], env)
        print "Executing command : %s" % str(cmdArgs)
        comp = 0.1
        updateCompletion(comp)
        ret = helper.execute(cmdArgs, env)
        print ret
        #if ret != 0:
        #    raise Exception("A problem occured in the render script, see log")

        # TODO check images
