#! /usr/bin/env puliscript.py

rootTaskGroup = TaskGroup("standalone complex")
for i in xrange(10):
    rootTaskGroup.addTaskGroup(TaskGroup(name="myName%d" % i, expander="myexpander.MyExpander" ))
submit(rootTaskGroup)
