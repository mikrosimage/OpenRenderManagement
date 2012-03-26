'''
Created on Dec 8, 2009

@author: acs
'''
from puliclient import Task, Graph
from random import choice

users = ['georges', 'bud', 'jbs', 'acs', 'alx', 'mholzer']

if __name__ == '__main__':

    nukeTask = Task("nukeTask",
                    arguments={'start': 1,
                               'end': 100,
                               'step': 1,
                               'packetSize': 4,
                               'fullSizeRender': 1,
                               'writeNode': 'Write1',
                               'scene': '/s/q/DevImages/toto.nk',
                               'outImages': '/s/q/DevImages/acs/toto/toto.###.jpg'},
                    decomposer="puliclient.contrib.nuke.NukeDecomposer")

    graph = Graph('nukeGraph', nukeTask, choice(users))

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
