#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph

if __name__ == '__main__':

	# Erreur 1
    # graph = Graph('simpleGraph')
    # graph.add("toto")

    # Erreur 2
    # simpleTask = Task(name="first",
    #                   arguments={ "cmd":"sleep 10", "start":1, "end":10, "packetSize":1, "prod":"test", "shot":"test" },
    #                   decomposer="puliclient.contrib.generic.GenericDecomposer")
    # graph = Graph('simpleGraph', simpleTask)
    # graph.addNewTask(name="bis",
    #                   arguments={ "cmd":"sleep 10", "start":1, "end":10, "packetSize":1, "prod":"test", "shot":"test" },
    #                   decomposer="puliclient.contrib.generic.GenericDecomposer")
