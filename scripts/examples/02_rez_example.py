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

graph = Graph('simple_rez_job', tags=tags)

# To define a Task, we need 4 arguments :
#   - the job name
#   - the class to run the command
#   - arguments dict

name = "simple_ls_task"

runner = "rezrunner.RezRunner"

arguments = {
    'command': 'ls',
    'runOptions': '-vv'
}

# Then add a new task to the graph
graph.addNewTask(name, runner=runner, arguments=arguments)

# Finally submit the graph to the server
graph.submit("pulitest", 8004)
