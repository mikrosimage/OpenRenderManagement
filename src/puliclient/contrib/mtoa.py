'''
Created on Nov 23, 2010

@author: acs
'''
import subprocess
import os
import shutil
import datetime
from puliclient.jobs import CommandRunner, TaskDecomposer
from puliclient.contrib.helper.helper import PuliActionHelper


#<arguments>
CAMERA = "cam"
FORMAT = "o"
#
USE_AA_PARAM = "use_aa"
ANTI_ALIASING_SAMPLES = "as"
USE_AA_CLAMP = "use_clamp"
ANTI_ALIASING_SAMPLES_CLAMP = "asc"
#
PIXEL_FILTER_TYPE = "aaf"
PIXEL_FILTER_WIDTH = "aafw"
#
GI_TRANSPARENCY_MODE = "tm"
GI_TRANSPARENCY_DEPTH = "td"
GI_TRANSPARENCY_THRESHOLD = "tt"
GI_TOTAL_RAY_DEPTH = "trd"
GI_GLOSSY_DEPTH = "glo"
GI_DEPTH_DIFFUSE_RAYS = "dd"
GI_DEPTH_REFLECTION_RAYS = "red"
GI_DEPTH_REFRACTION_RAYS = "rrd"
GI_DIFFUSE_SAMPLES = "ds"
GI_SPECULAR_SAMPLES = "ss"
#
USE_MOTION_BLUR = "umb"
MB_BY = "mbb"
MB_STATIC_OFFSET = "bso"
MB_BACK_OFFSET = "bbo"
MB_CAMERA_SHUTTER_TYPE = "sht"
MB_CAMERA_SHUTTER_START = "shs"
MB_CAMERA_SHUTTER_END = "she"
MB_USE_OBJECT_TRANSFORM = "bot"
MB_OBJECT_TRANSFORM_SAMPLE = "ots"
MB_USE_OBJECT_DEFORM = "bod"
MB_OBJECT_DEFORM_SAMPLE = "ods"
MB_USE_CAMERA_TRANSFORM = "bct"
MB_CAMERA_TRANSFORM_SAMPLE = "cts"
MB_USE_LIGHT_TRANSFORM = "blt"
MB_LIGHT_TRANSFORM_SAMPLE = "lts"

#
DISPLAY_OUTPUT_FRAME = "d"
#
USE_THREAD = "th"
NUMBER_OF_THREADS = "t"
BUCKET_SCAN = "bo"
BUCKET_SIZE = "bs"
#
PIXEL_ASPECT_RATIO = "par"
#
GENERATE_ASS_FILE = "ass"
OUTPUT_ASS_FILE = "assf"
COMPRESS_ASS = "compress"
LEAVE_ASS_FILES = "laf"
VERBOSE_LEVEL = "v"
MTOA_EXTENSIONS = "l"
EXTRA_KICK_FLAGS = "ekf"
#
ARNOLD_VERSION = "arnold"
MAYA_VERSION = "maya"
NUKE_VERSION = "nuke"
AFTEREFFECTS_VERSION = "after"
SHAVE_VERSION = "shave"
CROWD_VERSION = "crowd"
#
ABORT_ON_LIC_FAIL = "abortonlicfail"
NO_LIC_CHECK = "noliccheck"
MAKE_SCANLINED = "makescanlined"
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
FRAMES_LIST = "framesList"
#</generic>

class MtoaDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.mtoa.MtoaRunner"
        
        # initializes the maps for translating arguments
        self.formatsMap = {'jpg':1, 'tif': 2, 'exr':3, 'png':4}
        
        self.pixelFilterTypeMap = {'Box':0, 'Disk':1, 'Triangle':2,
                                   'Cone':3, 'Cubic':4, 'Catmull-Rom':5,
                                   'catmull-rom2d':6, 'Cook':7, 'Mitchell-Netravali':8,
                                   'Video':9, 'Gaussian':10, 'Sinc':11, 
                                   'Farthest':12, 'Variance':13, 'Closest':14}
        
        self.transparencyModeMap = {'Always':0, 'Shadow Only':1, 'Never':2}
        
        self.bucketScanMap = {'Bottom-Up':1, 'Top-Down':2, 'Left-To-Right':3, 
                              'Right-To-Left':4, 'Random':5, 'Woven':6,
                              'Spiral':7, 'Hilbert':8}
        
        self.bucketSizeMap = {'4':0, '8':1, '16':2, '32':3, '64':4, '128':5}
        
        self.verboseLevelMap = {'Error':0, 'Warning':1, 'Message':2,
                                'Info':3, 'Debug':4, 'Fine Debug':5,
                                'Full Debug':6}

        # FIXME temporary fix
        if FRAMES_LIST not in task.arguments:
            task.arguments[FRAMES_LIST] = ""
        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self, task.arguments[FRAMES_LIST])


    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd
        # translate the arguments
        for arg, value in self.task.arguments.items():
            if arg == FORMAT:
                # handle the format argument
                cmdArgs[FORMAT] = self.formatsMap[value]
                continue
            elif arg == PIXEL_FILTER_TYPE:
                # handle the pixel filter type argument
                cmdArgs[PIXEL_FILTER_TYPE] = self.pixelFilterTypeMap[value]
                continue
            elif arg == GI_TRANSPARENCY_MODE:
                # handle the transparency mode argument
                cmdArgs[GI_TRANSPARENCY_MODE] = self.transparencyModeMap[value]
                continue
            elif arg == BUCKET_SCAN:
                # handle the bucket scan argument
                cmdArgs[BUCKET_SCAN] = self.bucketScanMap[value]
                continue
            elif arg == BUCKET_SIZE:
                # handle the bucket size argument
                cmdArgs[BUCKET_SIZE] = self.bucketSizeMap[value]
                continue
            elif arg == VERBOSE_LEVEL:
                # handle the verbose level argument
                cmdArgs[VERBOSE_LEVEL] = self.verboseLevelMap[value]
                continue
        
        # Default values
        keys = self.task.arguments.keys()
        # mtoa version test
        if ARNOLD_VERSION in keys:
            version = cmdArgs[ARNOLD_VERSION]
            ver = version.split("_")
            v = float(ver[1][0:ver[1].rfind(".")])
            if v >= 1.6:
                if COMPRESS_ASS not in keys:
                    cmdArgs[COMPRESS_ASS] = "1"
                if NUMBER_OF_THREADS not in keys:
                    cmdArgs[NUMBER_OF_THREADS] = "0"
            else:
                cmdArgs[COMPRESS_ASS] = "0"
                cmdArgs[NUMBER_OF_THREADS] = "8"
        if LEAVE_ASS_FILES not in keys:
            cmdArgs[LEAVE_ASS_FILES] = "0"
        if ABORT_ON_LIC_FAIL not in keys:
            cmdArgs[ABORT_ON_LIC_FAIL] = "1"
        if NO_LIC_CHECK not in keys:
            cmdArgs[NO_LIC_CHECK] = "0"
        if MAKE_SCANLINED not in keys:
            cmdArgs[MAKE_SCANLINED] = "1"
        if VERBOSE_LEVEL not in keys:
            cmdArgs[VERBOSE_LEVEL] = "2"
         
        # always add the ass and assf arguments
        cmdArgs[GENERATE_ASS_FILE] = 1
        cmdArgs[OUTPUT_ASS_FILE] = "/datas/tmp/assfiles"

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class MtoaRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper(cleanTemp = True)
        
        # convert the paths
        prodPath = helper.mapPath(arguments[PROJECT])
        arguments[OUTPUT_ASS_FILE] = helper.mapPath(arguments[OUTPUT_ASS_FILE])
        arguments[RENDER_DIR] = helper.mapPath(arguments[RENDER_DIR])
        arguments[SCENE] = helper.mapPath(arguments[SCENE])
            
        # set the env
        use_shave = 0
        if MTOA_EXTENSIONS in arguments.keys() and SHAVE_VERSION in arguments[MTOA_EXTENSIONS]:
            use_shave = 1
        crowd_version = ""
        if CROWD_VERSION in arguments.keys():
            crowd_version = arguments[CROWD_VERSION]
        env = helper.getEnv(am_version=arguments[ARNOLD_VERSION], 
                            maya_version=arguments[MAYA_VERSION], 
                            shave_version=arguments[SHAVE_VERSION],
                            crowd_version=crowd_version,
                            home=os.environ["HOME"], 
                            job=os.path.basename(prodPath), 
                            jobdrive=os.path.dirname(prodPath), 
                            applis=helper.mapPath("/s/apps/lin"),
                            use_shave=use_shave)
        
        # init log
        helper.printStartLog("mtoarunner", "v2.0")

        # check existence of output ass folders
        helper.checkExistenceOrCreateDir(arguments[OUTPUT_ASS_FILE], "output ass folder")
        helper.checkExistenceOrCreateDir(arguments[RENDER_DIR], "render dir")

        # MtoA Extensions
        extensionsNodesSearchPath = []
        if MTOA_EXTENSIONS in arguments.keys():
            xtensions = arguments[MTOA_EXTENSIONS]
            addonPath = env["ARNOLD_LOCATION"] + "/add-ons"
            if xtensions == "<All>":
                # List directories in add-ons to add all extensions
                print "Will force load of all MtoA extensions available :"
                if not os.path.isdir(addonPath):
                    print "No Extension dir found"
                else:
                    print "Search in " + addonPath
                    fileList = os.listdir(addonPath)
                    if fileList:
                        fileList.sort()
                        nb = 0
                        for nb, extfile in enumerate(fileList):
                            if os.path.isdir(addonPath + "/" + extfile):
                                print " [ %s ]" % (extfile)
                                arguments['le'] = extfile
                                if os.path.isdir(os.path.join(addonPath, extfile, "nodes")):
                                    extensionsNodesSearchPath += [ "-l", os.path.join(addonPath, extfile, "nodes") ]
                                if nb == 0:
                                    extStr = extfile
                                else:
                                    extStr = extStr + ',' + extfile
                        if nb:
                            arguments['le'] = extStr
                        else:
                            print "No Extension found"
                    else:
                        print "Empty directory."
            else:
                lesExt = xtensions.split(",")
                for xt in lesExt:
                    extName = xt.strip()
                    if extName:
                        print "Will force load of MtoA Extension : [ %s ]" % (extName)
                        if os.path.isdir(os.path.join(addonPath, extName, "nodes")):
                            extensionsNodesSearchPath += [ "-l", os.path.join(addonPath, extName, "nodes") ]
                arguments['le'] = xtensions
        
        # Add arg to write outputs image files list
        outputsFile = '/datas/tmp/filestocheck_' + datetime.datetime.now().strftime('%y%m%d_%H%M%S')
        arguments['ifo'] = outputsFile
        
        cmdArgs = helper.buildMayaCommand("MikserActionMtoARender", arguments, [prodPath, arguments[SCENE]], env)
        
        # Execute the command line that will export the ass
        print '\nLaunch export command "%s"' % cmdArgs
        updateCompletion(0)
        ret = helper.execute(cmdArgs, env=env)
        print '===================================================='
        print '  MikserActionMtoARender returns with code %d' % (ret)
        print '===================================================='
        if ret != 0:
            print '  Export failed, exiting...'
            raise Exception, '  Export failed, exiting...'

        # update the completion
        comp = 0.3
        updateCompletion(comp)

        # Extra kick flags
        extraKickFlags = []
        if EXTRA_KICK_FLAGS in arguments.keys():
            extraKickFlags = self.parseExtraKickArgs(arguments[EXTRA_KICK_FLAGS])

        # kick the ass to render images
        # kick -l /s/apps/arnold/mArny2.27.21/shaders ./RE-sq14sh20-all_z-v002.1019.ass -dcau -log -v 2
        # au lieu de
        # kick -nstdin -dp -i /datas/tmp/assfiles/RE-sq14sh20-all_z-v002.1019.ass
        start = int(arguments[START])
        end = int(arguments[END])

        tiledDestination = arguments[RENDER_DIR] + "_Tiled"
        helper.checkExistenceOrCreateDir(tiledDestination, "tiled directory")

        for frameInt in range(start, end + 1):
            argList = [env["ARNOLD_LOCATION"] + "/bin/kick"]
            frameStr = str(frameInt).rjust(int(arguments[PADDING]), "0")
            # add the arguments to the kick command line
            argList += ["-l", env["ARNOLD_LOCATION"] + "/nodes"]
            # add extensions paths
            argList += extensionsNodesSearchPath
            assFilePath = arguments[OUTPUT_ASS_FILE] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + ".ass"
            # support for compress ass
            if COMPRESS_ASS in arguments.keys():
                if int(arguments[COMPRESS_ASS]):
                    assFilePath += ".gz"
            if assFilePath.find(" ") != -1:
                assFilePath = "\"" + assFilePath + "\""
            argList += [assFilePath]
            argList += ["-nokeypress"]
            argList += ["-dp"]
            argList += ["-dw"]
            argList += ["-log"]
            # no license check
            if NO_LIC_CHECK in arguments.keys():
                if int(arguments[NO_LIC_CHECK]):
                    argList += ["-sl"]
            # verbose level
            if VERBOSE_LEVEL in arguments.keys():
                argList += ["-v", str(arguments[VERBOSE_LEVEL])]
            # number of threads
            if NUMBER_OF_THREADS in arguments.keys():
                argList += ["-t", str(arguments[NUMBER_OF_THREADS])]
            # Extra kick flags
            argList += extraKickFlags
            
            # kick the command
            print "\nKicking command : " + " ".join(argList)
            kickret = helper.execute(argList, env=env)
            print '===================================================='
            print '  kick command returns with code %d' % (kickret)
            print '===================================================='
            if kickret != 0:
                print '  Kick command failed...'
                raise Exception, '  Kick command failed...'  
            
            # suppression of ass files
            if not int(arguments[LEAVE_ASS_FILES]):
                try:
                    os.remove(assFilePath)
                except OSError:
                    print "\nFailed to remove '"+assFilePath+"'\n"
            else:
                print "\nLeaving Ass extfile on disk : '"+assFilePath+"'\n"
            
            # Check the output image files by reading outputs extfile
            # Make them scanlined if in exr format
            self.invertedFormatsMap = {'1':'jpg',  '2':'tif', '3':'exr', '4':'png'}
            print "\nCheck render outputs :"
            outputsByFrame = outputsFile + '.' + frameStr
            lines = []
            try:
                outputsFileHdl = open(outputsByFrame, "r")
                lines = outputsFileHdl.readlines()
                outputsFileHdl.close()
            except IOError:
                filename = arguments[RENDER_DIR] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + "." + self.invertedFormatsMap[str(arguments[FORMAT])]
                lines = [ filename ]
            
            for line in lines:
                # Check
                filename = line.rstrip()
                print "> %s" % filename
                if not os.path.exists(filename):
                    raise Exception("file does not exist!")
                print "  OK."
                
                # exrmakescanlined
                make_scanlined = 1
                if MAKE_SCANLINED in arguments:
                    make_scanlined = int(arguments[MAKE_SCANLINED])
                if make_scanlined:
                    extension = os.path.splitext(filename)[1][1:]
                    if extension == 'exr':
                        print "  Make exr scanlined..."
                        exrscanlinedCmdArgs = ["/s/apps/lin/bin/exrmakescanlined"]
                        exrscanlinedCmdArgs.append("-v")
                        exrscanlinedCmdArgs.append(filename)
                        dest = '/tmp/' + os.path.basename(filename)
                        exrscanlinedCmdArgs.append(dest)
                        output, error = subprocess.Popen(exrscanlinedCmdArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                        #print output
                        if "nothing to do" in output:
                            print "    Nothing to do."
                        elif error:
                            print output
                            print error
                        else:
                            # move the extfile in a tiled dir and replace it with the new extfile
                            oldy = filename.replace(arguments[RENDER_DIR], tiledDestination)
                            print '    Replace image by scanlined version and move it to : '
                            print '      ' + oldy
                            shutil.move(filename, oldy)
                            shutil.move(dest, filename)
            
            # Suppress image outputs list
            try:
                os.remove(outputsByFrame)
            except OSError:
                print "\nFailed to remove "+outputsByFrame+"\n"
            
            # update completion
            comp += 0.7 / (end - start + 1)
            updateCompletion(comp)
        updateCompletion(1)
        print "\nrender done."

    # Parse extra kick flag args
    #
    def parseExtraKickArgs(self, argsStr):
        temp = argsStr.split()
        rags = []
        i = 0
        while i<len(temp):
            if temp[i][:1]=='"':
                recompose = temp[i]
                if temp[i][-1]!='"':
                    i += 1
                    while (i<len(temp) and temp[i][-1]!='"'):
                        recompose = recompose + " " + temp[i]
                        i += 1
                    if i<len(temp):
                        recompose = recompose + " " + temp[i]
                    else:
                        print "Error in extra kick flags"
                recompose = recompose[1:len(recompose)-1]
                rags.append(recompose)
            else:
                rags.append(temp[i])
            i += 1
        print "Will add Extra kick args :"
        print rags
        return rags
