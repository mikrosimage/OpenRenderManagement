#!/usr/bin/python
# coding: utf-8

from puliclient import Graph

# First we create a graph
graph = Graph('simple_job')

# Then we define a task. A task is basically a group of one or several commands all of the same kind.
# It means, each command will start the same process but might have different params. For instance you will
# create a task to execute a single process or to exec several times the same process like when rendering
# an image sequence.

# In this example we will create a simple task with only one command
# Therefore we need to define only 2 attributes:
#   - its name
#   - a dict of its arguments
name = "my_command"

# The arguments dict can handle many arguments.
# When creating a classic command (like a shell command) we internally use a "runner" called "CommandLineRunner".
# This default runner automatically handles the following arguments:
#   - cmd: a string representing the command to start
#   - start: start frame number
#   - end: end frame number
#   - packetSize: how many frames to calculate during the same command

# Here we only need to define the "cmd" to start a simple shell process
# (More information on runners and how to specialize them in documentation)
arguments = {
    "cmd": "ls -ltr"
}

# Then add a new task to the graph
graph.addNewTask(name, arguments=arguments)

# Finally submit the graph to the server
graph.submit("vfxpc64", 8004)
