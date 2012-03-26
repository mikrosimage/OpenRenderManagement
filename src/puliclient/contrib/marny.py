# -*- coding: utf-8 -*-
'''
Created on Jan 12, 2010

@author: acs
'''

from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper
import os
import subprocess

#<arguments>
CAMERA = "cam"
RENDER_LAYERS = "rl"
FORMAT = "o"

USE_ANTI_ALIASING = "use_AA"
ANTI_ALIASING_SAMPLES = "as"
ANTI_ALIASING_THRESHOLD = "at"

PIXEL_FILTER_TYPE = "aaf"
PIXEL_FILTER_WIDTH = "aafw"

DIFFUSE_SAMPLES = "ds"
SPECULAR_SAMPLES = "ss"

TOTAL_RAY_DEPTH = "td"
DEPTH_FOR_DIFFUSE_RAYS = "dd"
DEPTH_FOR_REFLECTION_RAYS = "red"
DEPTH_FOR_REFRACTION_RAYS = "rrd"

USE_MOTION_BLUR = "use_MB"
TRANSFORMATION_MOTION_BLUR = "tmb"
DEFORMATION_MOTION_BLUR = "dmb"
OBJECT_MOTION_BLUR_KEYS = "ombs"
CAMERA_MOTION_BLUR_ON = "cmb"
CAMERA_MOTION_BLUR_KEYS = "cmbs"
CAMERA_SHUTTER_START = "shs"
CAMERA_SHUTTER_END = "she"

DISPLAY_OUTPUT_FRAME = "d"

NUMBER_OF_THREADS = "t"
BUCKET_SCAN = "bo"
BUCKET_SIZE = "bs"

PREMULTIPLIED_ALPHA = "pa"
PIXEL_ASPECT_RATIO = "par"

GENERATE_ASS_FILE = "ass"
OUTPUT_ASS_FILE = "assf"
VERBOSE_LEVEL = "v"

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
OUTPUT_IMAGE = "p"
RENDER_DIR = "f"
SCENE = "scene"
PACKET_SIZE = "packetSize"
WIDTH = "rx"
HEIGHT = "ry"
PADDING = "pad"
#</generic>

class MarnyDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        task.runner = "puliclient.contrib.marny.MarnyRunner"
        self.formatsMap = {'jpg':6, 'sgi':7, 'tif':11, 'exr':12, 'hdr':3, 'pfm':2}

        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)

    def addCommand(self, packetStart, packetEnd):
#        cmdLine += ["/s/apps/lin/maya/scripts/MikserActionMarnyRender.py"]

        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd
        # translate the arguments
        for arg, value in self.task.arguments.items():
            if arg == FORMAT:
                # handle the format argument
                cmdArgs[FORMAT] = self.formatsMap[value]
                continue
            
        cmdArgs[GENERATE_ASS_FILE] = 1;
        cmdArgs[OUTPUT_ASS_FILE] = "/tmp/assfiles"
        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class MarnyRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper()
        
        # convert the paths
        prodPath = helper.mapPath(arguments[PROJECT])
        
        env = helper.getEnv(am_version=arguments[ARNOLD_VERSION], 
                            maya_version=arguments[MAYA_VERSION], 
                            shave_version=arguments[SHAVE_VERSION], 
                            home=os.environ["HOME"], 
                            job=os.path.basename(prodPath), 
                            jobdrive=os.path.dirname(prodPath), 
                            applis=helper.mapPath("/s/apps/lin"))
        # this is necessary for the python-bin of maya to work properly
        env["PYTHONHOME"] = env["MAYA_LOCATION"]

        # init log
        helper.printStartLog("MarnyRunner", "v1.1")

        # check existence of output ass folders
        helper.checkExistenceOrCreateDir(arguments[OUTPUT_ASS_FILE], "output ass folder")
        helper.checkExistenceOrCreateDir(arguments[RENDER_DIR], "render dir")

        cmdArgs = helper.buildMayaCommand("MikserActionMarnyRender", arguments, None, env)

        # Execute the command line that will export the ass
        updateCompletion(0)
        helper.execute(cmdArgs, env=env)
        
        # update the completion
        comp = 0.3
        updateCompletion(comp)

        # kick the ass to render images
        # kick -l /s/apps/arnold/mArny2.27.21/shaders ./RE-sq14sh20-all_z-v002.1019.ass -dcau -log -v 2
        # au lieu de
        # kick -nstdin -dp -i /tmp/assfiles/RE-sq14sh20-all_z-v002.1019.ass
        start = int(arguments[START])
        end = int(arguments[END]) + 1
        for frameInt in range(start, end):
            argList = [env["ARNOLD_LOCATION"] + "/bin/kick"]
            frameStr = str(frameInt).rjust(int(arguments[PADDING]), "0")
            # add the arguments to the kick command line
            argList += ["-l", env["ARNOLD_LOCATION"] + "/shaders"]
            assFilePath = arguments[OUTPUT_ASS_FILE] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + ".ass"
            if assFilePath.find(" ") != -1:
                assFilePath = "\"" + assFilePath + "\""
            argList += [assFilePath]
            argList += ["-dcau"]
            argList += ["-log"]
            argList += ["-%s" % VERBOSE_LEVEL, "2"]
            # kick the command
            print "\nKicking command : " + " ".join(argList)
            kickOut = subprocess.Popen(argList, close_fds=True, env=env)
            kickOut.communicate()
            # check the file
            filename = arguments[RENDER_DIR] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + "." + str(arguments[FORMAT]) 
            print "Checking \"%s\"..." % filename
            if not os.path.exists(filename):
                raise Exception
            print "OK."
            # update completion
            comp += 0.7 / (end - start)
            updateCompletion(comp)
        updateCompletion(1)
        print "\nrender done."