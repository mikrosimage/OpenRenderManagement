'''
Created on Dec 7, 2009

@author: acs
'''
from puliclient import Task, Graph
from random import choice

users = ['georges', 'bud', 'jbs', 'acs', 'alx', 'mholzer']

if __name__ == '__main__':

    mayaTask = Task(name="mayaTask",
                    arguments={'cam': 'persp',
                               'start': 1,
                               'end': 100,
                               'rl': '',
                               'format': 'dpx',
                               'width': 640,
                               'height': 480,
                               'image': 'bouleSimple',
                               'rd': '/s/q/DevImages/acs/bouleSimple/',
                               'scene': '/s/q/DevImages/acs/monprojetmaya/scenes/boule_simple.mb',
                               'step': 1,
                               'padding': 4,
                               'packetSize': 4},
                    decomposer="puliclient.contrib.maya.MayaDecomposer")

    graph = Graph('mayaGraph', mayaTask, choice(users))

    from pprint import pprint
    r = graph.toRepresentation()
    pprint(r)

    localhost = False
    localhost = True

    if localhost:
        host, port = ("127.0.0.1", 8004)

        JOBCOUNT = 1

    else:
        host, port = ("hd3d-test02", 8004)
        JOBCOUNT = 1
    def avg(data):
        return float(sum(data)) / len(data)
    def var(data):
        return sum((datum * datum) for datum in data) - avg(data) * avg(data)
    deltas = []
    for i in xrange(JOBCOUNT):
#    i = 0
#    while True:
        import time
        t0 = time.time()
        graph.user = choice(users)
        result = graph.submit(host, port)
        deltas.append(time.time() - t0)
        i += 1
        print "%d" % i
        print "last: %f" % deltas[-1]
        print "min: %f" % min(deltas)
        print "max: %f" % max(deltas)
        print "avg: %f" % avg(deltas)
        print "var: %f" % var(deltas)
