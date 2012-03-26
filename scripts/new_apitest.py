from puliclient import TaskGroup, Graph, cleanEnv
from random import choice
import os

NAMES = ['a job %d', 'some job %d', 'yet another job %d']
NAMES = ['Quicktime generation %i: /prod/under3/037/139/', 'Compositing %i ', 'Maya Render %i', 'ZBrush %i', 'Software update %i: Puli']
USERS = ['fzarroca', 'ndumay', 'pgenin', 'cbonjean', 'tank', 'shotgun', 'dvolny', 'ndarfeuille', 'rlauren']
PRODS = ['micmac', 'under3', 'mdc']
PLANS = ["0030", "0040", "0041", "0042"]
SEQUENCES = ['001', '002', '004', '017']
DPTS = ['2D', '3D', 'Layout']

def sendJob(i):
    name = choice(NAMES) % i
    user = choice(USERS)
    prod = choice(PRODS)
    plan = choice(PLANS)
    sequence = choice(SEQUENCES)
    dpt = choice(DPTS)

    taskPlan40 = TaskGroup(name=name,
                           expander="testjobs.MyExpander",
                           tags={"prod": prod, "plan": plan, "sequence": sequence, "dpt": dpt},
                           environment=cleanEnv(os.environ))

    myGraph = Graph('graphFor' + taskPlan40.name, taskPlan40, user)
    myGraph.root.expand(True)
#    from pprint import pprint
#    pprint(myGraph.toRepresentation())
#    result = myGraph.submit("127.0.0.1", 8004)
    result = myGraph.submit("kevin", 8004)
    print result

import time
for i in range(1):
    sendJob(i)
#    time.sleep(5)
