'''
Created on Mar 5, 2012

@author: Arnaud Chassagne
'''
import os
import subprocess
import shutil
from puliclient.jobs import CommandRunner, TaskDecomposer
from puliclient.contrib.helper.helper import PuliActionHelper


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

class ExrmakescanlinedDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.exrmakescanlined.ExrmakescanlinedRunner"
        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)


    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class ExrmakescanlinedRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper()
        # get input sequence
        seq = helper.mapPath(arguments[SCENE])
        parentDir = os.path.dirname(seq)
        self.srcDirName = os.path.basename(parentDir)
        # convert the paths
        prodPath = helper.mapPath(arguments[PROJECT])
        # get start and end
        self.start = arguments[START]
        self.end = arguments[END]
        # cmdline
        self.cmd = helper.mapPath("/s/apps/lin/bin/exrmakescanlined")
        # temp dir
        self.tmpDir = os.path.join(prodPath, "_admin", "mikser", "tmp")
        if not os.path.isdir(self.tmpDir):
            os.mkdir(self.tmpDir)
        os.path.walk(parentDir, self.visit, "")


    def visit(self, arg, dirname, names):
        for name in names:
            subname = os.path.join(dirname, name)
            if not os.path.isdir(subname):
                frame = int(subname.split(".")[-2])
                # for each image that is comprised in the provided range
                if frame >= self.start and frame <= self.end:
                    # constructs the command arguments
                    argList = [self.cmd, "-v"]
                    argList.append(subname)

                    destination = os.path.join(self.tmpDir, name)
                    argList.append(destination)

                    # launch the process
                    output, error = subprocess.Popen(argList, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                    print output
                    if "nothing to do" in output:
                        pass
                    elif error:
                        print error
                    else:
                        # once the process is finished, move the tiled file and replace it with the scanlined one
                        subnameDir = os.path.dirname(subname)
                        subnameDirReplacement = subnameDir.replace(self.srcDirName, self.srcDirName + "_Tiled")
                        tiledDestination = subname.replace(subnameDir, subnameDirReplacement)
                        if not os.path.isdir(os.path.dirname(tiledDestination)):
                            os.mkdir(os.path.dirname(tiledDestination))
                        shutil.move(subname, tiledDestination)
                        shutil.move(destination, subname)