#! /usr/bin/env puliscript.py

plan40 = TaskGroup(name="Plan40", arguments={"first": 0, "last": 100}, tags={"prod": "under3", "plan": "plan40"})

# 3D subgroup
plan40_3D = TaskGroup(name="3D", arguments={"scene": "/prod/under3/plan40/scene001.ma"})
plan40.addTaskGroup(plan40_3D)

# -- shadows
shadows = Task(name="Shadows",
               decomposer="under3.plan40.FrameDecomposer",
               runner="under3.plan40.ShadowRunner",
               arguments={})
plan40_3D.addTask(shadows)

# -- colors
colors = Task(name="Colors",
              decomposer="octopus.core.jobtypes.decomposers.taskdecomposer.DefaultTaskDecomposer",
              runner="under3.plan40.ColorRunner",
              arguments={})
plan40_3D.addTask(colors)

submit(plan40)
