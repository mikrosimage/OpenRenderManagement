from puliclient import Task, jobs

class MyExpander(jobs.TaskExpander):

    def __init__(self, taskGroup):
        jobs.TaskExpander.__init__(self, taskGroup)
#        myTask = Task("myTask", taskGroup.arguments, "puliclient.contrib.testrunner.TestRunner", decomposer="testjobs.MyDecomposer", maxNbCores=1, ramUse=2000)
        arguments = {"args": "sleep 10"}
        myTask = Task("myTask", arguments, "puliclient.contrib.commandline.CommandLine", decomposer="testjobs.MyDecomposer", maxNbCores=1, ramUse=2000)
        taskGroup.addTask(myTask)


class MyDecomposer(jobs.TaskDecomposer):

    def __init__(self, task):
        jobs.TaskDecomposer.__init__(self, task)
        task.addCommand("mySupacommand", task.arguments)
