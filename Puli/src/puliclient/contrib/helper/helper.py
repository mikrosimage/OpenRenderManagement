# coding: utf-8
'''
Created on Jan 22, 2012

@author: Arnaud Chassagne, lcl, mgi
'''

import subprocess
import os
import socket
import datetime
import platform
import sys
import threading
import re
sys.path.append("/s/apps/common/python")
sys.path.append("\\\\exastore2\\Applis\\common\\python")
import mikrosEnv
mikrosEnv.MikrosEnv().addVersionnedPath(default=True)
import utilities.cleanTempDir.cleanTempDir as cleanLib
import env.templates.mayaVar as mayaVar
import env.templates.nukeVar as nukeVar

NUKE_VERSION = "nuke"
ENV_MAYA_LOCATION = "MAYA_LOCATION"

class TimeoutError ( Exception ):
    ''' Raised when helper execution is too long. '''

class PuliActionHelper(object):
    MikrosEnv = mikrosEnv.MikrosEnv()

    def __init__(self, cleanTemp=False):
        # first of all, call the clean temp dir function
        if cleanTemp:
            cleanLib.cleanTempDir()
        self.mikUtils = mikrosEnv.MikrosEnv()




    def decompose(self, start, end, packetSize, callback, framesList=""):
        packetSize = int(packetSize)
        if len(framesList) != 0:
            frames = framesList.split(",")
            for frame in frames:
                if "-" in frame:
                    frameList = frame.split("-")
                    start = int(frameList[0])
                    end = int(frameList[1])

                    length = end - start + 1
                    fullPacketCount, lastPacketCount = divmod(length, packetSize)

                    if length < packetSize:
                        callback.addCommand(start, end)
                    else:
                        for i in range(fullPacketCount):
                            packetStart = start + i * packetSize
                            packetEnd = packetStart + packetSize - 1
                            callback.addCommand(packetStart, packetEnd)
                        if lastPacketCount:
                            packetStart = start + (i + 1) * packetSize
                            callback.addCommand(packetStart, end)
                else:
                    callback.addCommand(int(frame), int(frame))
        else:
            start = int(start)
            end = int(end)

            length = end - start + 1
            fullPacketCount, lastPacketCount = divmod(length, packetSize)

            if length < packetSize:
                callback.addCommand(start, end)
            else:
                for i in range(fullPacketCount):
                    packetStart = start + i * packetSize
                    packetEnd = packetStart + packetSize - 1
                    callback.addCommand(packetStart, packetEnd)
                if lastPacketCount:
                    packetStart = start + (i + 1) * packetSize
                    callback.addCommand(packetStart, end)

    def mapPath(self, path):
        return self.mikUtils.mapPath(path)

    def getEnv(self, am_version="", maya_version="", shave_version="", crowd_version="", home="", job="", jobdrive="", applis="", use_shave=0, nuke_rep=""):
        if nuke_rep == "":
            if use_shave:
                env = mayaVar.shaveCreateEnvDict(am_version=am_version,
                                    maya_version=maya_version,
                                    shave_version=shave_version,
                                    crowd_version=crowd_version,
                                    home=home,
                                    job=job,
                                    jobdrive=jobdrive,
                                    applis=applis)
            else:
                env = mayaVar.createEnvDict(am_version=am_version,
                                    maya_version=maya_version,
                                    shave_version=shave_version,
                                    crowd_version=crowd_version,
                                    home=home,
                                    job=job,
                                    jobdrive=jobdrive,
                                    applis=applis)

            # this is necessary for the python-bin of maya to work properly
            if platform.system() == "Linux":
                env["PYTHONHOME"] = env["MAYA_LOCATION"]
            else:
                env["PYTHONHOME"] = env["MAYA_LOCATION"] + "\\Python"
                env["SYSTEMROOT"] = "C:\\WINDOWS"
            try:
                env["TEMP"] = os.environ["TEMP"]
            except KeyError:
                pass
        else:
            env = nukeVar.createEnvDict(nuke_rep=nuke_rep,
                                    home=home,
                                    job=job,
                                    jobdrive=jobdrive,
                                    applis=applis)

        # regularize the env
        for envVar in sorted(env):
            if platform.system() == 'Linux':
                env[envVar] = env[envVar].replace(";", ":")
            else:
                if envVar != "MAYA_HELP_URL":
                    env[envVar] = env[envVar].replace("/", "\\")

        # normalize the env (windows does not understand unicode)
        envN = {}
        for key in env:
            envN[str(key)] = str(env[key])

        print "========================================================"
        print "== SETTING ENV VARIABLES"
        print "========================================================"
        keys = envN.keys()
        keys.sort()
        for k in keys:
            print "%s = %s" % (k, envN[k])
        print "========================================================\n"

        return envN

    def isLinux(self):
        return platform.system() == 'Linux'

    def printStartLog(self, name, version):
        date = datetime.datetime.now()
        dateStr = date.strftime("%d/%m/%Y %H:%M:%S")
        print "\n========================================================"
        print " Starting %s %s" % (name, version)
        print " Running on %s" % socket.gethostname()
        print " Start time : %s" % dateStr
        print "========================================================"

    def checkExistenceOrCreateDir(self, path, name):
        if not os.path.exists(path):
            print "\nCreating %s..." % name
            os.umask(2)
            try:
                os.makedirs(path, 0775)
            except OSError:
                if os.path.isdir(path):
                    pass
                else:
                    raise Exception("Could not create or access dir : %s" % path)
        else:
            print "%s already exists" % name

    def execute(self, cmdArgs, env):
        os.umask(2)
        process = subprocess.Popen(cmdArgs, env=env)
        return process.wait()

    def executeWithTimeout(self, cmdArgs, env, timeout):
        self.process = None

        def target():
            os.umask(2)
            self.process = subprocess.Popen(cmdArgs, env=env)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.process.terminate()
            thread.join()
            raise TimeoutError("Execution has taken more than allowed time (%d)" % timeout)

    def buildMayaCommand(self, mikserActionScript, arguments, additionalArguments, env):
        if self.isLinux():
            cmdArgs = ["%s/bin/python-bin" % env["MAYA_LOCATION"]]
        else:
            cmdArgs = ["%s\\bin\\mayapy.exe" % env["MAYA_LOCATION"]]
        if mikserActionScript is not None:
            if not mikserActionScript.endswith(".py"):
                cmdArgs.append("/s/apps/lin/maya/scripts/%s.py" % mikserActionScript)
            else:
                cmdArgs.append("/s/apps/lin/maya/scripts/%s" % mikserActionScript)
        if arguments is not None:
            # continue building the command arguments
            argsList = []
            for arg, value in arguments.items():
                argsList += [arg, value]
            cmdArgs.append("\"%s\"" % str(argsList))
        if additionalArguments is not None:
            for arg in additionalArguments:
                cmdArgs.append(arg)
        return cmdArgs

    def buildNukeCommand(self, arguments, localNukeScene):
        # set the path for the nuke Executable
        p = re.compile("\.")
        version_array = p.split(arguments[NUKE_VERSION])
        nukeExeVersion = version_array[0][-1] + "." + version_array[1][0]
        if self.isLinux():
            cmdArgs = ["/s/apps/lin/nuke/Nuke%s/Nuke%s" % (arguments[NUKE_VERSION], nukeExeVersion)]
        else:
            cmdArgs = ["S:/Nuke/Nuke%s/Nuke%s" % (arguments[NUKE_VERSION], nukeExeVersion)]

        if "nukex"in arguments and arguments["nukex"] == "1":
            cmdArgs.append("--nukex")
        cmdArgs.append("-t")
        cmdArgs.append("-V")
        if arguments["fullSizeRender"] == "1":
            cmdArgs.append("-f")
        cmdArgs.append("-X")
        cmdArgs.append(arguments["writeNode"])
        cmdArgs.append(localNukeScene)
        cmdArgs.append("%s,%s,%s" % (arguments["start"], arguments["end"], arguments["step"]))
        return cmdArgs

    def sendMail(self, dest, jobname):
        import smtplib
        from email.mime.text import MIMEText
        frommail = "puli@puliserver"
        msg = MIMEText("Your job %s is now complete" % jobname)
        msg['Subject'] = "Render %s done" % jobname
        msg['From'] = frommail
        msg['To'] = dest
        s = smtplib.SMTP('aspmx.l.google.com')
        s.sendmail(frommail, [dest], msg.as_string())
        s.quit()
