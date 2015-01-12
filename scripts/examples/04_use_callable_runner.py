#!/usr/bin/python
# coding: utf-8

from puliclient import Graph

from mymodule import myFunction
from mymodule import MyClass

if __name__ == '__main__':

    # In this example we will create 2 tasks to execute custom callables
    # Internally we will use a predefined runner: CallableRunner
    # The function or class method we are asking to execute must be accessible to the render nodes.

    tags = {
        "prod": "prod_name",
        "shot": "shot_code",
        # Add any valuable info relative to the job here: type, step, version, iteration...
    }

    # First we create a graph
    # Added to the graph is a dict of tags that will be used to clarify the job process
    graph = Graph('job with callable', tags=tags)

    # First task
    name = "callable function"

    graph.addNewCallable(
        myFunction,
        "callable function",
        user_args=("param1", "param2"),
        user_kwargs={"wait": 10},
    )

    # Second task
    name = "callable method"

    graph.addNewCallable(
        MyClass.myMethod,
        "callable method",
        user_args=(),
        user_kwargs={
            "param1": "first_param",
            "param2": "second_param",
            "wait": 10
        },
    )

    # Finally submit the graph to the server
    graph.submit("vfxpc64", 8004)
