# coding: utf-8

'''
Created on Jan 12, 2010

@author: bud <oderpierre@quintaindustries.com>
'''

import sys
from puliclient.jobs import TaskDecomposer, CommandRunner, StringParameter

START = "start"
END = "end"
STEP = "step"
PACKET_SIZE = "packetSize"
FULL_SIZE_RENDER = "fullSizeRender"
WRITE_NODE = "writeNode"
SCENE = "scene"
NUKE_EXECUTABLE = "nukeExecutable"

DEFAULT_NUKE_EXECUTABLE = "nuke"


class NukeDecomposer(TaskDecomposer):
    """Splits a nuke Task into several commands."""

    def __init__(self, task):
        TaskDecomposer.__init__(task)
        task.runner = "dupuli.jobs.nuke.NukeRunner"

        if not NUKE_EXECUTABLE in task.arguments:
            task.arguments[NUKE_EXECUTABLE] = DEFAULT_NUKE_EXECUTABLE
        start = task.arguments[START]
        end = task.arguments[END]
        step = task.arguments[STEP]
        packetSize = task.arguments[PACKET_SIZE]

        assert isinstance(start, int)
        assert isinstance(end, int)
        assert isinstance(step, int)
        assert isinstance(packetSize, int)

        length = end - start + 1
        fullPacketCount, lastPacketCount = divmod(length, packetSize)

        for i in range(fullPacketCount):
            packetStart = start + i * packetSize
            packetEnd = start + (i + 1) * packetSize - 1
            self.addCommand(task, packetStart, packetEnd)
        if lastPacketCount:
            packetStart = start + fullPacketCount * packetSize
            self.addCommand(task, packetStart, end)


    def addCommand(self, task, packetStart, packetEnd):
        cmdName = task.name + "_%s_%s" % (str(packetStart), str(packetEnd))
        task.addCommand(cmdName, {START: packetStart, END: packetEnd})


NUKE_SCENE = "scene"
OUTPUT_IMAGES = "outImages"
LOCAL_NUKE_SCENE = "localNukeScene"
NUKE_WRITE_NODE = "writeNode"


import re
import subprocess

FRAME_WRITE_PATTERN = r"^Writing .* took .* seconds"


class NukeRunner(CommandRunner):

    scene = StringParameter()
    writeNode = StringParameter()

    def buildCommandLine(self, arguments):
        args = []
        args.append(arguments[NUKE_EXECUTABLE])
        args.append("-V")
        if arguments[FULL_SIZE_RENDER]:
            args.append("-f")
        args.append("-X")
        args.append(arguments[WRITE_NODE])
        args.append(arguments[NUKE_SCENE])
        args.append("%s,%s,%s" % (arguments[START], arguments[END], arguments[STEP]))
        return args


    def execute(self, arguments, updateCompletion, updateMessage):
        args = self.buildCommandLine(arguments)
        updateCompletion(0)
        updateMessage("Running command " + subprocess.list2cmdline(args))
        print "# Running command " + subprocess.list2cmdline(args) + "\n"
        sys.stdout.flush()
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, close_fds=True)
        completedFrames = 0
        totalFrames = (int(arguments[END]) - int(arguments[START]) + 1) // int(arguments[STEP])
        while True:
            line = p.stdout.readline()
            print line,
            sys.stdout.flush()
            if re.match(FRAME_WRITE_PATTERN, line):
                completedFrames += 1
                print "# %d frames computed" % completedFrames
                sys.stdout.flush()
                updateMessage("%d frames computed" % completedFrames)
                updateCompletion(float(completedFrames) / totalFrames)
        if completedFrames == totalFrames:
            updateMessage("Done")
            updateCompletion(1)
        else:
            raise RuntimeError, "Incomplete job: %d/%d images computed" % (completedFrames, totalFrames)

