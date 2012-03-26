'''
Created on Jan 12, 2010

@author: acs
'''
from puliclient.jobs import TaskDecomposer
from puliclient.contrib.helper import PuliActionHelper

#<arguments>
CAMERA = "cam"
RENDER_LAYERS = "rl"
FORMAT = "format"
#
ARNOLD_VERSION = "arnold"
MAYA_VERSION = "maya"
NUKE_VERSION = "nuke"
AFTEREFFECTS_VERSION = "after"
SHAVE_VERSION = "shave"
#</arguments>

#<generic>
START = "start"
END = "end"
STEP = "step"
OUTPUT_IMAGE = "image"
RENDER_DIR = "rd"
SCENE = "scene"
PACKET_SIZE = "packetSize"
WIDTH = "width"
HEIGHT = "height"
PADDING = "padding"
#</generic>


class MayaDecomposer(TaskDecomposer):

    def __init__(self, task):
        TaskDecomposer.__init__(self, task)
        task.runner = "puliclient.contrib.mayarunner.MayaRunner"

        PuliActionHelper().decompose(task.arguments[START], task.arguments[END], task.arguments[PACKET_SIZE], self)


    def addCommand(self, task, packetStart, packetEnd):
        cmd = 'mayarunner -r sw -pad %s -fnc 3 -of %s -cam %s -x %s -y %s -s %s -e %s -im %s -rd %s %s' % (
                          task.arguments[PADDING],
                          task.arguments[FORMAT],
                          task.arguments[CAMERA],
                          task.arguments[WIDTH],
                          task.arguments[HEIGHT],
                          packetStart,
                          packetEnd,
                          task.arguments[OUTPUT_IMAGE],
                          task.arguments[RENDER_DIR],
                          task.arguments[SCENE])
        cmdName = "%s_%s_%s" % (self.task.name, str(packetStart), str(packetEnd))
        task.addCommand(cmdName, {'args': cmd})
