#!/usr/bin/python
# coding: utf-8

from puliclient import Graph

tags = {
    "prod": "prod_name",
    "shot": "shot_code",
    # Add any valuable info relative to the job here: type, step, version, iteration...
}

# First we create a graph
# Added to the graph is a dict of tags that will be used to clarify the job process
graph = Graph('simple_job', tags=tags)

# In this example we will create 2 tasks with multiple frames
# The first task executes one process per command, the second will group several processes by commands
# We still need to define its name and arguments dict

# The default runner automatically handles the following arguments:
#   - cmd: a string representing the command to start
#   - start: start frame number
#   - end: end frame number

# First task
name = "multiple commands"

arguments = {
    "cmd": "sleep %%MI_FRAME%%",
    "start": 1,
    "end": 10,
}

graph.addNewTask(name, arguments=arguments)

# Second task
name = "multiple commands grouped by packet size"

# To handle several processes in a command, we add the following arg:
#   - packetSize: number of frames to run in the same command
arguments = {
    "cmd": "sleep %%MI_FRAME%%",
    "start": 1,
    "end": 10,
    "packetSize": 3
}

graph.addNewTask(name, arguments=arguments)

# Finally submit the graph to the server
graph.submit("vfxpc64", 8004)
