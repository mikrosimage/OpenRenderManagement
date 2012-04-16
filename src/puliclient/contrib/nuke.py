'''
Created on Jan 12, 2010

@author: acs
'''

from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper
import os
import re
import subprocess
import sys
import datetime
import time


#<arguments>
FULL_SIZE_RENDER = "fullSizeRender"
WRITE_NODE = "writeNode"

ARNOLD_VERSION = "arnold"
MAYA_VERSION = "maya"
NUKE_VERSION = "nuke"
AFTEREFFECTS_VERSION = "after"
SHAVE_VERSION = "shave"
#</arguments>

#<generic>
PROJECT = "proj"
START = "start"
END = "end"
STEP = "step"
OUTPUT_IMAGES = "outImages"
SCENE = "scene"
PACKET_SIZE = "packetSize"
#</generic>

#<runner>
ARGS = "args"
NUKE_SCENE = "scene"
LOCAL_NUKE_SCENE = "localNukeScene"
FRAME_WRITE_PATTERN = r"^Writing .* took .* seconds"
#</runner>

class NukeDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.nuke.NukeRunner"
        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)

    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd
        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)
        
        
class NukeRunner(CommandRunner):
    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        self.helper = PuliActionHelper(cleanTemp = True)

        # convert the paths
        prodPath = self.helper.mapPath(arguments[PROJECT])
        arguments[NUKE_SCENE] = self.helper.mapPath(arguments[NUKE_SCENE])
        outImages = self.helper.mapPath(arguments[OUTPUT_IMAGES])
        
        # set the env
        env = self.helper.getEnv(nuke_rep=arguments[NUKE_VERSION],  
                                 home=os.environ["HOME"], 
                                 job=os.path.basename(prodPath), 
                                 jobdrive=os.path.dirname(prodPath), 
                                 applis=self.helper.mapPath("/s/apps/lin"))
        
        # init log
        self.helper.printStartLog("NukeRunner", "1.2")
        
        # replace the local nuke scene argument in the argslist
        if outImages != "":
            localNukeScene = self.updateOutputFiles(arguments[NUKE_SCENE], outImages, arguments[WRITE_NODE])

        cmdArgs = self.helper.buildNukeCommand(arguments, localNukeScene)

        updateCompletion(0.1)
        updateMessage("Executing command %s" % cmdArgs)
        print "\nExecuting command : %s\n" % cmdArgs
        sys.stdout.flush()
        
        out = subprocess.Popen(cmdArgs, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, env=env)
        begintime = time.time()
        completedFrames = 0
        totalFrames = (int(arguments[END]) - int(arguments[START]) + 1) // int(arguments[STEP])
        if arguments.get("views", ''):
            totalFrames = totalFrames * len(arguments["views"].split(","))
        while True:
            line = out.stdout.readline()
            if not line:
                break
            print line,
            sys.stdout.flush()
            if re.match(FRAME_WRITE_PATTERN, line):
                completedFrames += 1
                print "%s -- frame %d of %d rendered --" % (time.strftime('[%H:%M:%S]', time.gmtime(time.time() - begintime)), completedFrames, totalFrames)
                sys.stdout.flush()
                updateMessage("%d frames rendered" % completedFrames)
                updateCompletion(float(completedFrames) / totalFrames)
            if completedFrames == totalFrames:
                print "\nrender done."
                updateMessage("render done.")
                updateCompletion(1)
                break
        out.terminate()
        if completedFrames != totalFrames:
            raise Exception, "Incomplete job: %d/%d images rendered" % (completedFrames, totalFrames)


    def updateOutputFiles(self, srcNukeFilePath, outImages, writeNode):
        if self.helper.isLinux():
            tempDir = "/tmp"
        else:
            tempDir = "T:/Temp"
            outImages = outImages.replace("\\", "/")

        date = datetime.datetime.now()
        dateStr = date.strftime("%d%m%Y_%H_%M_%S")
        localNukeScene = os.path.join(tempDir, os.path.basename(srcNukeFilePath) + "_" + dateStr + ".nk")

        ## Creating render images dir
        outFolder = os.path.dirname(outImages)
        if "%v" in outFolder or "%V" in outFolder:
            ## test parsing views        
            srcNukeFile = open(srcNukeFilePath, 'r')
            lines = srcNukeFile.readlines()
            srcNukeFile.close()
            lines.reverse()
            
            index = lines.index(" name " + writeNode + "\n")
            
            onlyLeft = False
            onlyRight = False
            
            for i in range(index, len(lines)):
                if 'views {' in lines[i]:
                    if 'left' in lines[i]:
                        onlyLeft = True
                    else:
                        onlyRight = True
                if 'Write {' in lines[i]:
                    break
            if "%v" in outFolder:
                if not onlyRight:
                    self.helper.checkExistenceOrCreateDir(outFolder.replace('%v', 'l'), "render dir left")
                if not onlyLeft:
                    self.helper.checkExistenceOrCreateDir(outFolder.replace('%v', 'r'), "render dir right")
            elif "%V" in outFolder:
                if not onlyRight:
                    self.helper.checkExistenceOrCreateDir(outFolder.replace('%V', 'left'), "render dir left")
                if not onlyLeft:
                    self.helper.checkExistenceOrCreateDir(outFolder.replace('%V', 'right'), "render dir right")
        else:    
            self.helper.checkExistenceOrCreateDir(outFolder, 'render dir')

        srcNukeFile = open(srcNukeFilePath, 'r')
        ## Creating nuke tmp out file in tempDir
        dstNukeFile = open(localNukeScene, 'w')

        ## Copying nuke file in tempFile
        contentStr = srcNukeFile.read()
        srcNukeFile.close()
        dstNukeFile.write(contentStr)

        outImagesPadding = outImages.count("#")
        outStrPadding = "%0" + str(outImagesPadding) + "d"
        inStrPadding = ""
        inStrPadding = inStrPadding.rjust(outImagesPadding, "#")
        outImages = outImages.replace(inStrPadding, outStrPadding)

        dstNukeFile.write("\n#### ADDED BY PULI : " + dateStr + " ###\n")

        ## Pre scripts
#        dstNukeFile.write("\n## PRE SCRIPTS ###\n\n")
#        for scripPath in inScripts:
#            dstNukeFile.write("load \"" + scripPath + "\"\n")

        # Write Node
        dstNukeFile.write("\n## WRITE NODE ###\n")
        dstNukeFile.write("\nknob " + writeNode + ".file \"" + outImages + "\"\n")

        print "\nLocal Nuke scene created : '" + localNukeScene + "'."
        dstNukeFile.close()
        
        return localNukeScene