#!/usr/bin/python
# coding: utf-8

import sys
sys.path.insert(0,"/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/src")

from puliclient import Task, Graph

# maya version
mayaVersion = '2013'

# mtoa version
mtoaVersion = 'mimtoa1.0.0'

# shave version
shaveVersion = 'shaveHaircut-7.0v35'

# prod path
prodPath = '/s/prods/mikros_test'

# maya project path
mayaProjPath = '/s/prods/mikros_test/fam/3d_pr'

# maya scene file path
mayaScenePath = '/s/prods/mikros_test/fam/3d_pr/scenes/3rendu/test_shave.ma'

# start, end frames
startFrame = 1
endFrame = 1

# packet size
packetSize = 1000

# frames list
framesList = ''

# resolution
resX = 320
resY = 200

# maya camera
camera = 'persp'

# output image file format
outFileFormat = 'exr'

# frame padding size
paddingSize = 4

# render directory path
renderDirPath = '/tmp'

# output filename prefix
outFileNamePrefix = 'output'

arguments={
'maya': mayaVersion,
'arnold': mtoaVersion,
'shave': shaveVersion,

'l':'shave',

'prod': prodPath,
'proj': mayaProjPath,
'scene': mayaScenePath,

's': startFrame,
'e': endFrame,
'packetSize': packetSize,
'framesList': framesList,

'rx': resX,
'ry': resY,
'cam': camera,

'as': '1',

'o': outFileFormat,
'pad': paddingSize,
'f': renderDirPath,
'p': outFileNamePrefix
}

tags =  { "prod":"test", "shot":"test" }

decomposer='puliclient.contrib.puliDbg.mtoa.MtoaDecomposer'

graph = Graph('mtoa_graph', tags=tags, poolName='default' )

graph.addNewTask( "mtoa_task", tags=tags, arguments=arguments, decomposer=decomposer, lic="shave&mtoa" )

graph.submit("vfxpc64", 8004)
# graph.execute()
