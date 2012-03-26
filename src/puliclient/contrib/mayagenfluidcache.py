# coding: utf-8
'''
Generates a maya fluid cache for a given fluid.
'''

import os
import platform
from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper

#<arguments>
FLUID="fluid"
ONE_FILE="oneFile"
SIMULATION_RATE="simulationRate"
SAVE_RATE="saveRate"
DENSITY="density"
VELOCITY="velocity"
TEMPERATURE="temperature"
FUEL="fuel"
COLOR="color"
TEXTURE="texture"
FALLOFF="falloff"
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

class MayagenfluidcacheDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.mayagenfluidcache.MayagenfluidcacheRunner"

        cmdName = self.task.name + '_%s_%s' % (self.task.arguments[START], self.task.arguments[END])

        cmdArgs = self.task.arguments.copy()

        self.task.addCommand(cmdName, cmdArgs)
        

class MayagenfluidcacheRunner(CommandRunner):

    inArgsNames =   [
                    "fluid",
                    "oneFile",          # maya param 3
                    "simulationRate",   # maya param 11
                    "saveRate",         # maya param 12
                    "density",          # maya param 16
                    "velocity",         # maya param 17
                    "temperature",      # maya param 18
                    "fuel",             # maya param 19
                    "color",            # maya param 20
                    "texture",          # maya param 21
                    "falloff"           # maya param 22   
                     ]
    
    melArgs =   [
                '0',                #0 time range mode, 0 : use args[1] and args[2] as start-end, 1 : use render globals, 2 : use timeline
                '0',                #1 start frame (if time range mode == 0)
                '0',                #2 end frame (if time range mode == 0)
                'OneFilePerFrame',  #3 cache file distribution, either "OneFile" or "OneFilePerFrame"
                '0',                #4 0/1, whether to refresh during caching
                '',                 #5 directory for cache files, if "", then use project data dir
                '0',                #6 0/1, whether to create a cache per geometry
                '',                 #7 name of cache file. An empty string can be used to specify that an auto-generated name is acceptable
                '0',                #8 0/1, whether the specified cache name is to be used as a prefix
                'add',              #9 action to perform: "add", "replace", "merge" or "mergeDelete"
                '0',                #10 force save even if it overwrites existing files
                '1',                #11 simulation rate, the rate at which the fluid simulation is forced to run
                '1',                #12 sample mulitplier, the rate at which samples are written, as a multiple of simulation rate
                '0',                #13 0/1, whether modifications should be inherited from the cache about to be replaced
                '1',                #14 0/1, whether to store doubles as floats
                'mcc',              #15 name of cache format
                '1',                #16 0/1, whether density should be cached
                '1',                #17 0/1, whether velocity should be cached
                '1',                #18 0/1, whether temperature should be cached
                '1',                #19 0/1, whether fuel should be cached
                '1',                #20 0/1, whether color should be cached
                '1',                #21 0/1, whether texture coordinates should be cached
                '1'                 #22 0/1, whether falloff should be cached
                ]
    
    def updateMelArgs(self, melParamId, mikserParamName, decomposerArguments):
        try:
            self.melArgs[melParamId] = str(decomposerArguments[mikserParamName])
        except KeyError:
            pass
    
    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper()
        ###
        #Pull info from decomposer
        ###
        mayaScene = helper.mapPath(arguments[SCENE])
        mayaSceneRootName = '3d_pr'
        if mayaScene.count(mayaSceneRootName) == 0:
            # TODO: a real mikser warning and exit
            print ('Warning: unable to find the maya root directory')
            
        self.melArgs[1] = str(arguments[START])
        self.melArgs[2] = str(arguments[END])
        self.melArgs[5] = arguments[RENDER_DIR]
        self.melArgs[7] = os.path.splitext(OUTPUT_IMAGE)[0]
        try:
            self.melArgs[3] = 'OneFile' if int(arguments[ONE_FILE]) == 1 else 'OneFilePerFrame'
        except KeyError:
            pass
        self.updateMelArgs(11, SIMULATION_RATE, arguments)
        self.updateMelArgs(12, SAVE_RATE, arguments)
        self.updateMelArgs(16, DENSITY, arguments)
        self.updateMelArgs(17, VELOCITY, arguments)
        self.updateMelArgs(18, TEMPERATURE, arguments)
        self.updateMelArgs(19, FUEL, arguments)
        self.updateMelArgs(20, COLOR, arguments)
        self.updateMelArgs(21, TEXTURE, arguments)
        self.updateMelArgs(22, FALLOFF, arguments)
        melArgsString = ' '.join(self.melArgs)
        argsList = ['mel', melArgsString]
        argsList += [FLUID, arguments[FLUID]]
        readyForJsonArgs = '"%s"' % str(argsList) # this formatting is needed in MikserCommonRender.MikserCommonHeaderAndInit
      
        ###
        #Env set up
        ###
        prodPath = helper.mapPath(arguments[PROJECT])
        env = helper.getEnv(maya_version=arguments['maya'], 
                            home=os.environ["HOME"], 
                            job=os.path.basename(prodPath), 
                            jobdrive=os.path.dirname(prodPath), 
                            applis=helper.mapPath("/s/apps/lin"))

        ###
        #Additionnal arguments
        ###
        # maya's python
        if platform.system() == 'Linux':
            mayaPython = env["MAYA_LOCATION"] + "/bin/python-bin"
        else:
            mayaPython = env["MAYA_LOCATION"] + "\\bin\\mayapy.exe"

        # script doing the actual simulation baking process
        mayaGenFluidScript = '/s/apps/lin/maya/scripts/MikserActionMayaGenFluidCache.py'

        ###
        #Execute the command line
        ###
        argsListN = [mayaPython, mayaGenFluidScript, readyForJsonArgs, prodPath, mayaScene]
        
        print 'Launching maya python (%s)' % mayaPython
        print 'With script: %s' % mayaGenFluidScript
        print 'And arguments: %s' % readyForJsonArgs
        print 'Using maya scene: %s' % mayaScene
        
        updateCompletion(0)
        helper.execute(argsListN, env=env)
        # update the completion
        updateCompletion(0.7)
        
        ###
        #Check cache file existence
        ###
        cacheFile = os.path.join(arguments[RENDER_DIR], arguments[OUTPUT_IMAGE]+'.xml')
        if not os.path.exists(cacheFile):
            print "Error: cache file %s has not been written" % cacheFile
            raise Exception
        
        ###
        #This is the end
        ###
        print "\n\n---- %s has been written SUCCESSFULLY ----" % cacheFile
