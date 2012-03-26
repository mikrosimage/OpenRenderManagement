#! /usr/bin/env puliscript -d localhost

from octopus.client.api import Task, TaskGroup, Graph, DONE

# declare tasks
arguments = {
    "sources": ["liba/a.c", "liba/b.c", "liba/c.c"],
    "output": "liba/liba.a"
}
taskLibA = TaskGroup(name="libA", expander="compil.StaticLibrary", arguments=arguments)

arguments = {
    "sources": ["prog.c"],
    "libs": ["liba/liba.a"],
    "output": "prog"
}
taskProg = Task(name="prog", decomposer="compil.Prog", arguments=arguments)

# add dependencies
taskProg.dependsOn(taskLibA, [DONE])

# group tasks to form a job
taskCompil = TaskGroup(name="Prog build")
taskCompil.addTask(taskLibA)
taskCompil.addTask(taskProg)

# setup some environment variables
env = {
    "PWD": "/home/bud/puli/tests/compil",
    "CC": "gcc",
    "CFLAGS": "-g -O2",
}
taskCompil.setEnv(env)

# submit the job
#submit(taskCompil)
job = Graph("compil", taskCompil, "bud")
print job.toRepresentation()

