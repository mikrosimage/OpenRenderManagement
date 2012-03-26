'''
Created on Dec 7, 2009

@author: acs
'''

mayaTask = Task(name="mayaTask",
                arguments={'cam': 'persp',
                           'start': 1,
                           'end': 100,
                           'rl': '',
                           'format': 'jpeg',
                           'width': 640,
                           'height': 480,
                           'image': 'bouleSimple',
                           'rd': '/s/q/DevImages/acs/bouleSimple/',
                           'scene': '/s/q/DevImages/acs/monprojetmaya/scenes/boule_simple.mb',
                           'step': 1,
                           'padding': 4,
                           'packetSize': 4},
                decomposer="puliclient.contrib.maya.MayaDecomposer")

submit(mayaTask)
