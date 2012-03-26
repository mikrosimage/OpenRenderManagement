'''
Created on May 4, 2010

@author: acs
'''
milkTask = Task(name="milkTask",
                   arguments={'start': 1,
                              'end': 100,
                              'step':1,
                              'filein':'',
                              'cmd':'',
                              'fileout':'',
                              'packetSize':4},
                   decomposer="puliclient.contrib.milk.MilkDecomposer")

submit(milkTask)