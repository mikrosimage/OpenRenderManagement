#!/usr/bin/python
# coding: utf-8

from puliclient import Graph

# First we create a graph
graph = Graph('simple_job')

# Then we define a task. A task is basically a group of one or several commands all of the same kind.
# It means, each command will start the same process but might have different params. For instance you will
# create a task to execute a single process or to exec several times the same process like when rendering
# an image sequence.

# In this example we will create a simple task with multiple frames, we still need to define its name and
# arguments dict
name = "task_with_multiple_commands"

# This default runner automatically handles the following arguments:
#   - cmd: a string representing the command to start
#   - start: start frame number
#   - end: end frame number
arguments = {
    "cmd": "sleep %%FRAME%%",
    "start": 1,
    "end": 10,
}

# Then add a new task to the graph
graph.addNewTask(name, arguments=arguments)

# Finally submit the graph to the server
graph.submit("vfxpc64", 8004)
