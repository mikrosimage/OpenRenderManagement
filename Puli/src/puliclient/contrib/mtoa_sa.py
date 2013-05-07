'''
Created on Nov 23, 2010

@author: acs
'''
import subprocess, os, shutil, time, datetime, sys, re, tempfile
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
LAUNCH_KICK = "kick"
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
PROD = "prod"
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

class Mtoa_saDecomposer(TaskDecomposer):

    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.mtoa_sa.Mtoa_saRunner"

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

        if FRAMES_LIST not in task.arguments:
            task.arguments[FRAMES_LIST] = ''
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
        if COMPRESS_ASS not in keys:
            cmdArgs[COMPRESS_ASS] = "1"
        if LEAVE_ASS_FILES not in keys:
            cmdArgs[LEAVE_ASS_FILES] = "0"
        if ABORT_ON_LIC_FAIL not in keys:
            cmdArgs[ABORT_ON_LIC_FAIL] = "1"
        if NO_LIC_CHECK not in keys:
            cmdArgs[NO_LIC_CHECK] = "0"
        if MAKE_SCANLINED not in keys:
            cmdArgs[MAKE_SCANLINED] = "1"

        # always add the ass and assf arguments
        if GENERATE_ASS_FILE not in keys:
            cmdArgs[GENERATE_ASS_FILE] = 1
        if OUTPUT_ASS_FILE not in keys:
            cmdArgs[OUTPUT_ASS_FILE] = "/datas/tmp/assfiles"
        if LAUNCH_KICK not in keys:
            cmdArgs[LAUNCH_KICK] = 1

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class Mtoa_saRunner(CommandRunner):
    def setSolidAngleEnv(self, env, key, val):
        if key in env:
            if env[key]:
                if not val in env[key].split(':'): 
                    env[key] = val + ':' + env[key]
            else:  
                env[key] = val
        else:
            env[key] = val

    def execute(self, arguments, updateCompletion, updateMessage):
        # init the helper
        helper = PuliActionHelper()
        # convert the paths
        
        prodPath = helper.mapPath(arguments[PROD])
        arguments[OUTPUT_ASS_FILE] = helper.mapPath(arguments[OUTPUT_ASS_FILE])
        arguments[RENDER_DIR] = helper.mapPath(arguments[RENDER_DIR])
        arguments[SCENE] = helper.mapPath(arguments[SCENE])
        projPath = os.path.dirname(arguments[SCENE].split('scenes')[0])

        cameras = arguments["cam"].split(',')

        # init log
        helper.printStartLog("mtoa_sa runner", "v0.0")

        # check existence of output ass folders
        helper.checkExistenceOrCreateDir(arguments[OUTPUT_ASS_FILE], "output ass folder")
        helper.checkExistenceOrCreateDir(arguments[RENDER_DIR], "render dir")
        
        env = helper.getEnv(am_version=arguments[ARNOLD_VERSION],
                            maya_version=arguments[MAYA_VERSION],
                            shave_version=arguments[SHAVE_VERSION],
                            crowd_version=arguments[CROWD_VERSION],
                            home=os.environ["HOME"],
                            job=os.path.basename(prodPath),
                            jobdrive=os.path.dirname(prodPath),
                            applis=helper.mapPath("/s/apps/lin"),
                            use_shave=0)
        #
        print "Forcing custom mtoa variables"
        env['ARNOLD_LOCATION'] = '/s/apps/lin/arnold/mtoa0.22.0'
        self.setSolidAngleEnv(env, 'AM_DRIVE', env["ARNOLD_LOCATION"])
        self.setSolidAngleEnv(env, 'MAYA_MODULE_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"])
        self.setSolidAngleEnv(env, 'PYTHONPATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/scripts')
        self.setSolidAngleEnv(env, 'MAYA_PRESET_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/presets')
        self.setSolidAngleEnv(env, 'XBMLANGPATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/icons/%B')
        self.setSolidAngleEnv(env, 'MAYA_PLUG_IN_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/plug-ins')
        self.setSolidAngleEnv(env, 'LD_LIBRARY_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/lib')
        self.setSolidAngleEnv(env, 'ARNOLD_PLUGIN_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/shaders')
        self.setSolidAngleEnv(env, 'MAYA_RENDER_DESC', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"])
        self.setSolidAngleEnv(env, 'PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/bin')
        self.setSolidAngleEnv(env, 'MAYA_SCRIPT_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/scripts/mtoa/2013/mel')
        self.setSolidAngleEnv(env, 'MAYA_SCRIPT_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/scripts')
        self.setSolidAngleEnv(env, 'MAYA_PLUG_IN_RESOURCE_PATH', env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + '/resources')
        self.setSolidAngleEnv(env, 'ARNOLD_LICENSE_PORT', '5053')
        self.setSolidAngleEnv(env, 'ARNOLD_LICENSE_HOST', 'jacket')

        print "################################################"
        print "     ASS EXPORT:",  int(arguments.get(GENERATE_ASS_FILE, '1'))
        print "     KICK:",  int(arguments.get(LAUNCH_KICK, '1'))
        print "################################################"
        if int(arguments.get(GENERATE_ASS_FILE, '1')):
        #if exportTest:
            #+/+Mikanada+/+
            cmdArgs = helper.buildMayaCommand("MikserActionMtoA_SARender", arguments, [projPath, arguments[SCENE]], env)
            # Execute the command line that will export the ass
            print '\nLaunch export command "%s"' % cmdArgs
            comp = 0
            updateCompletion(comp)
            startTime = time.time()
            ret = helper.execute(cmdArgs, env=env)
            print '===================================================='
            print '  MikserActionMtoA_SARender returns with code %d' % (ret)
            print '  Process took %s s' %str(time.time() - startTime).split('.')[0]
            print '===================================================='
            #
            #    mtoa exits with an error code.... Arrrrhhh
            #
            if ret != 0:
                print 'Warning, maya has been quit with an error code'

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
        totalFrames = end - start + 1
        
        frameCompletionPattern = re.compile("\| (.*)% done .* rays/pixel")


        argList = [ env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + "/bin/kick"]
        argList += ["-l", env["ARNOLD_LOCATION"] + '/' + env["MAYA_VERSION"] + "/shaders"]
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
        # anti-aliasing samples
        if ANTI_ALIASING_SAMPLES in arguments.keys():
            argList += ["-as", str(arguments[ANTI_ALIASING_SAMPLES])]
        # Extra kick flags
        argList += extraKickFlags
        
        for frameInt in range(start, end + 1):
            frameStr = str(frameInt).rjust(int(arguments[PADDING]), "0")
            # add the arguments to the kick command line
            assFilePath = arguments[OUTPUT_ASS_FILE] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + ".ass"
            # support for compress ass
            if COMPRESS_ASS in arguments.keys():
                if int(arguments[COMPRESS_ASS]):
                    assFilePath += ".gz"
            if assFilePath.find(" ") != -1:
                assFilePath = "\"" + assFilePath + "\""
                
            cmdList = argList[:]
            cmdList.append(assFilePath)

            # kick the command
            print "\nKicking command : " + " ".join(cmdList)

            os.umask(2)
            out = subprocess.Popen(cmdList, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, env=env)
            rc = None
            while rc is None:
                line = out.stdout.readline()
                if not line:
                    break
                print line,
                sys.stdout.flush()
                fcp = frameCompletionPattern.search(line)
                if fcp:
                    framecomp = float(fcp.group(1).strip())
                    fc = float(framecomp / 100) / float(totalFrames)
                    updateCompletion(comp + fc)
                rc = out.poll()

            out.communicate()
            rc = out.poll()
            print '===================================================='
            print '  kick command returns with code %d' % rc
            print '===================================================='
            if rc != 0:
                print '  Kick command failed...'
                raise Exception('  Kick command failed...')
            comp += 1.0 / float(totalFrames)

            # suppression of ass files
            if not int(arguments[LEAVE_ASS_FILES]):
                try:
                    os.remove(assFilePath)
                except OSError:
                    print "\nFailed to remove '" + assFilePath + "'\n"
            else:
                print "\nLeaving Ass file on disk : '" + assFilePath + "'\n"

            self.invertedFormatsMap = {'1': 'jpg',  '2': 'tif', '3': 'exr', '4': 'png'}
            filename = arguments[RENDER_DIR] + "/" + arguments[OUTPUT_IMAGE] + "." + frameStr + "." + self.invertedFormatsMap[str(arguments[FORMAT])]

            # Check file existence
            filename = filename.rstrip()
            print "> %s" % filename
            if not os.path.exists(filename):
                raise Exception("file does not exist!")
            print "  OK."

            # exrmakescanlined
            make_scanlined = 1
            if MAKE_SCANLINED in arguments and os.path.splitext(filename)[1][1:] == 'exr':
                print "  Make exr scanlined..."
                exrscanlinedCmdArgs = ["/s/apps/lin/bin/exrmakescanlined-test"]
                exrscanlinedCmdArgs.append("-c")
                exrscanlinedCmdArgs.append(filename)
                tf = tempfile.NamedTemporaryFile(prefix='/tmp/')
                dest = tf.name
                tf.close()
                exrscanlinedCmdArgs.append(dest)
                subp = subprocess.Popen(exrscanlinedCmdArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, error = subp.communicate()
                if subp.returncode == 0:
                    # idiff comparison
                    idiffcmd = ["/s/apps/lin/bin/idiff-1.1.7"]
                    idiffcmd.append(filename)
                    idiffcmd.append(dest)
                    devnull = open('/dev/null', 'w')
                    idiffret = subprocess.Popen(idiffcmd, env=os.environ, stdout=devnull).wait()
                    if True:
                        print "       idiff : tiled and scanlined images match, replacing the tiled version..."
                        shutil.move(dest, filename+".tmp")
                        os.rename(filename+".tmp", filename)
                        print "       tiled version overwritten"
                    else :
                        print "       idiff : Keeping Tiled version : images do not match !!!"
                    # FIXME check permissions
                    import stat
                    st = os.stat(filename)
                    if not bool(st.st_mode & stat.S_IRGRP) or not bool(st.st_mode & stat.S_IWGRP):
                        logtemp = open("/s/apps/lin/puli/tmplog.csv", 'a')
                        import socket
                        logtemp.write("%s\t%s\t%s\tmtoa\t%s\t%s\n" % (filename, socket.gethostname(), os.path.basename(prodPath), oct(stat.S_IMODE(st.st_mode)), time.strftime('%H:%M', time.localtime())))
                        logtemp.close()
                elif subp.returncode == 1:
                    print "    Nothing to do."
                elif error:
                    print error
                else:
                    print "an error has occured, keeping the tiled version"

        updateCompletion(1)
        print "\nrender done."

    # Parse extra kick flag args
    #
    def parseExtraKickArgs(self, argsStr):
        temp = argsStr.split()
        rags = []
        i = 0
        while i < len(temp):
            if temp[i][:1] == '"':
                recompose = temp[i]
                if temp[i][-1] != '"':
                    i += 1
                    while (i < len(temp) and temp[i][-1] != '"'):
                        recompose = recompose + " " + temp[i]
                        i += 1
                    if i < len(temp):
                        recompose = recompose + " " + temp[i]
                    else:
                        print "Error in extra kick flags"
                recompose = recompose[1:len(recompose) - 1]
                rags.append(recompose)
            else:
                rags.append(temp[i])
            i += 1
        print "Will add Extra kick args :"
        print rags
        return rags

