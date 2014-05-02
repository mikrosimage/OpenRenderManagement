'''
Created on Sep 28, 2011

@author: Arnaud Chassagne
'''
import os
import time
import subprocess


from puliclient.jobs import TaskDecomposer, CommandRunner, StringParameter, IntegerParameter
# from puliclient.contrib.helper.helper import PuliActionHelper

START = "start"
END = "end"
STEP = "step"
PACKET_SIZE = "packetSize"
CMD = "cmd"


class GenericDecomposer(TaskDecomposer):
    def __init__(self, task):
        self.task = task
        self.task.runner = "puliclient.contrib.generic.GenericRunner"

        packetSize = int(task.arguments[PACKET_SIZE])
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
                        self.addCommand(start, end)
                    else:
                        for i in range(fullPacketCount):
                            packetStart = start + i * packetSize
                            packetEnd = packetStart + packetSize - 1
                            self.addCommand(packetStart, packetEnd)
                        if lastPacketCount:
                            packetStart = start + (i + 1) * packetSize
                            self.addCommand(packetStart, end)
                else:
                    self.addCommand(int(frame), int(frame))
        else:
            start = int(task.arguments[START])
            end = int(task.arguments[END])

            length = end - start + 1
            fullPacketCount, lastPacketCount = divmod(length, packetSize)

            if length < packetSize:
                self.addCommand(start, end)
            else:
                for i in range(fullPacketCount):
                    packetStart = start + i * packetSize
                    packetEnd = packetStart + packetSize - 1
                    self.addCommand(packetStart, packetEnd)
                if lastPacketCount:
                    packetStart = start + (i + 1) * packetSize
                    self.addCommand(packetStart, end)


    def addCommand(self, packetStart, packetEnd):
        cmdArgs = self.task.arguments.copy()
        cmdArgs[START] = packetStart
        cmdArgs[END] = packetEnd

        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        self.task.addCommand(cmdName, cmdArgs)


class GenericRunner(CommandRunner):
    cmd = StringParameter( mandatory = True )
    timeout = IntegerParameter( default=0 , min=0 )

    def execute(self, arguments, updateCompletion, updateMessage, updateStats ):
        # init the helper
        cmd = arguments[CMD]

        print 'Running command "%s"' % cmd
        updateCompletion(0.0)

        if int(arguments['timeout']) == 0:
            os.umask(2)
            process = subprocess.Popen(cmd.split(" "), env=os.environ)
            process.wait()

        else:
            # helper.executeWithTimeout( currCommand.split(" "), env=os.environ, timeout=timeout )
            # def executeWithTimeout(self, cmdArgs, env, timeout):
            self.process = None

            def target():
                os.umask(2)
                self.process = subprocess.Popen(cmd.split(" "), env=os.environ)
                self.process.communicate()

            thread = threading.Thread(target=target)
            thread.start()
            thread.join(timeout)
            if thread.is_alive():
                self.process.terminate()
                thread.join()
                raise TimeoutError("Execution has taken more than allowed time (%d)" % timeout)

        updateCompletion(1)
