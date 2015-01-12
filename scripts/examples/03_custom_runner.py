#!/usr/bin/python
# coding: utf-8
from puliclient import Graph
from puliclient.jobs import CommandRunner


#
# Defining the class to execute when the command is assigned and started
# This class is called a runner, it must be accessible to each rendernode.
# Therefore it is stored by convention in a shared folder based in "pulicontrib" package
#
class MyRunner(CommandRunner):
    def execute(self, arguments):
        self.log.info("Hey we are running a simple job.")

        wait = arguments.get('wait', 0)
        self.log.info("We have found 'wait' value in arguments: wait=%d" % int(wait))

        import time
        time.sleep(wait)

        self.log.info("Process is finished.")


if __name__ == "__main__":

    #
    # Submission script
    #
    tags = {
        "prod": "prod_name",
        "shot": "shot_code",
        # Add any valuable info relative to the job here: type, step, version, iteration...
    }

    # First we create a graph
    # Added to the graph is a dict of tags that will be used to clarify the job process
    graph = Graph('simple job', tags=tags)

    # To define a Task, we need 3 arguments :
    #   - its name
    #   - a runner is a python class that defines the workflow execution for a given job type.
    #     Here, we will use MyRunner which has been declared previously
    #   - an arguments dict
    name = "wait 10s"
    runner = "example.MyRunner"
    arguments = {"wait": 10}

    # Then add a new task to the graph
    graph.addNewTask(name, runner=runner, arguments=arguments)

    # Finally submit the graph to the server
    graph.submit("pulitest", 8004)
