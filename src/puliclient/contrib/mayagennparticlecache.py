# coding: utf-8
'''
Generates a maya nparticle cache for a given nparticle.
'''

import os
import platform
from puliclient.jobs import TaskDecomposer, CommandRunner
from puliclient.contrib.helper.helper import PuliActionHelper

#<arguments>
NPARTICLE="nparticle"
ONE_FILE="oneFile"
SIMULATION_RATE="simulationRate"
SAVE_RATE="saveRate"
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

class MayagennparticlecacheDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.mayagennparticlecache.MayagennparticlecacheRunner"

        cmdName = self.task.name + '_%s_%s' % (self.task.arguments[START], self.task.arguments[END])

        cmdArgs = self.task.arguments.copy()

        self.task.addCommand(cmdName, cmdArgs)
        

class MayagennparticlecacheRunner(CommandRunner):

    inArgsNames =   [
                    "nparticle",
                    "oneFile",          # maya param 3
                    "simulationRate",   # maya param 11
                    "saveRate",         # maya param 12
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
            
        #check existence and writability of render dir
        renderDir = arguments[RENDER_DIR]
        helper.checkExistenceOrCreateDir(renderDir, 'render dir')
        if not os.access(renderDir, os.W_OK):
            print('Error: unable to write in %s' % renderDir)
            raise Exception
        
        self.melArgs[1] = str(arguments[START])
        self.melArgs[2] = str(arguments[END])
        self.melArgs[5] = arguments[RENDER_DIR]
        self.melArgs[7] = os.path.splitext(arguments[OUTPUT_IMAGE])[0]
        try:
            self.melArgs[3] = 'OneFile' if int(arguments[ONE_FILE]) == 1 else 'OneFilePerFrame'
        except KeyError:
            pass
        self.updateMelArgs(11, SIMULATION_RATE, arguments)
        self.updateMelArgs(12, SAVE_RATE, arguments)
        melArgsString = ' '.join(self.melArgs)
        argsList = ['mel', melArgsString]
        argsList += [NPARTICLE, arguments[NPARTICLE]]
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
        mayaGenNParticleScript = '/s/apps/lin/maya/scripts/MikserActionMayaGenNParticleCache.py'

        ###
        #Execute the command line
        ###
        argsListN = [mayaPython, mayaGenNParticleScript, readyForJsonArgs, prodPath, mayaScene]
        
        print 'Launching maya python (%s)' % mayaPython
        print 'With script: %s' % mayaGenNParticleScript
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
