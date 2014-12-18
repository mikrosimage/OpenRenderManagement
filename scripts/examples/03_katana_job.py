#!/usr/bin/python
# coding: utf-8
from puliclient import Graph

#
# Submission script
#

# First we create a graph
# here we can use a tags dict to attach custom values to the graph
tags = {
    "prod": "prod_name",
    "shot": "shot_code",
    "nbFrames": 1
}

graph = Graph('katana_on_5_frames', tags=tags)

# To define a Task, we need 4 arguments :
#   - the job name
#   - the class to run the command
#   - arguments dict

name = "katana_render"

runner = "rezrunner.RezRunner"

# NB: RezRunner will automatically interpret %%MI_START%% or %%MI_END%% and
# replace it with current command "start/end" values
arguments = {
    'command': 'katana --batch --katana-file /s/prods/mikros_test/jsa/testSimple.katana -t %%MI_START%%-%%MI_END%% --render-node Render',
    'start': 1,
    'end': 5,
    'packetSize': 2
}

# Then add a new task to the graph
graph.addNewTask(name, runner=runner, arguments=arguments, lic='katana')

# Finally submit the graph to the server
graph.submit("pulitest", 8004)
