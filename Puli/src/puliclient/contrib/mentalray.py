'''
Created on Jan 12, 2010

@author: Arnaud Chassagne
'''
from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper
import os

#<arguments>
CAMERA = "cam"
RENDER_LAYERS = "rl"
FORMAT = "of"

RGB = "rgb"
ALPHA = "alpha"
DEPTH = "depth"

ASPECT_RATIO = "ard"
VERBOSE = "v"

ARNOLD_VERSION = "arnold"
MAYA_VERSION = "maya"
NUKE_VERSION = "nuke"
AFTEREFFECTS_VERSION = "after"
SHAVE_VERSION = "shave"
#</arguments>

#<generic>
PROJECT = "proj"
START = "s"
END = "e"
STEP = "step"
OUTPUT_IMAGE = "im"
RENDER_DIR = "rd"
SCENE = "scene"
PACKET_SIZE = "packetSize"
WIDTH = "x"
HEIGHT = "y"
PADDING = "pad"
FRAMES_LIST = "framesList"
#</generic>


class MentalrayDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        task.runner = "puliclient.contrib.mentalray.MentalrayRunner"

        # FIXME temporary fix
        if FRAMES_LIST not in task.arguments:
            task.arguments[FRAMES_LIST] = ""
        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self, task.arguments[FRAMES_LIST])


    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd
        # default values
        if VERBOSE not in self.task.arguments.keys():
            cmdArgs[VERBOSE] = "5"
        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)



class MentalrayRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper(cleanTemp = True)

        start = int(arguments[START])
        end = int(arguments[END])
        padding = int(arguments[PADDING])
        prefix = str(arguments[OUTPUT_IMAGE])

        # convert the paths
        prodPath = helper.mapPath(arguments[PROJECT])
        arguments[RENDER_DIR] = helper.mapPath(arguments[RENDER_DIR])
        arguments[SCENE] = helper.mapPath(arguments[SCENE])

        # set the env
        env = helper.getEnv(am_version=arguments[ARNOLD_VERSION],
                            maya_version=arguments[MAYA_VERSION],
                            shave_version=arguments[SHAVE_VERSION],
                            home=os.environ["HOME"],
                            job=os.path.basename(prodPath),
                            jobdrive=os.path.dirname(prodPath),
                            applis=helper.mapPath("/s/apps/lin"))

        helper.printStartLog("MentalRayRunner", "v1.1")
        cmdArgs = helper.buildMayaCommand("MikserActionMentalRayRender", arguments, [prodPath, arguments[SCENE]], env)
        print "Executing command : %s" % str(cmdArgs)
        comp = 0.1
        updateCompletion(comp)
        ret = helper.execute(cmdArgs, env)
        if ret != 0:
            raise Exception("A problem occured in the render script, see log")

        # check images
        layer2Render = []
        if RENDER_LAYERS in arguments:
            layer2Render = str(arguments[RENDER_LAYERS]).split(",")
        if len(layer2Render):
            for layer in layer2Render:
                for frameInt in range(start, end+1):
                    frameStr = str(frameInt).rjust(padding, "0")
                    # FIXME improve this...
                    # check the file
                    if layer == "defaultRenderLayer":
                        layerfolder = "masterLayer"
                    else:
                        layerfolder = layer
                    renderdir = str(arguments[RENDER_DIR]) + "/" + layerfolder + "/"
                    filename = renderdir + prefix + "-" + layer + "." + frameStr + "." + str(arguments[FORMAT])
                    print "Checking \"%s\"..." % filename
                    if not os.path.isfile(filename):
                        raise Exception("File does not exist")
                    else:
                        print "OK."
        else:
            for frameInt in range(start, end+1):
                frameStr = str(frameInt).rjust(padding, "0")
            # check the file
            filename = str(arguments[RENDER_DIR]) + "/" + prefix + "." + frameStr + "." + str(arguments[FORMAT])
            print "Checking \"%s\"..." % filename
            if not os.path.isfile(filename):
                raise Exception("File does not exist")
            else:
                print "OK."

        updateCompletion(1)
        print "\nrender done."