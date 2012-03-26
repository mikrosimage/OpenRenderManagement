'''
Created on 21 oct. 2009

@author: jean-baptiste.spiese
''' 
from octopus.client import *
import datetime
from random import choice

users = ['georges', 'bud', 'jbs', 'acs', 'alx', 'mholzer']
prods = ['puli', 'micmac', 'under3']
plans = ['001', '042']
seqs = ['001', '002', '007']
dpts = ['studio', 'rd', 'system', 'reflex', '2d', '3d']

PROD = 'PROD'
PLAN = 'PLAN'
SEQ = 'SEQ'
DPT = 'DPT'

if __name__ == '__main__':    
    
    shotTask = Task("myShot",
                    "octopus.core.jobtypes.shot.Shot",
                    arguments={'prod': 'ASTERIX', 'shot':55, 'length':101})
    
    shotTask.tags[PROD] = choice(prods)
    shotTask.tags[PLAN] = choice(plans)
    shotTask.tags[SEQ] = choice(seqs)
    shotTask.tags[DPT] = choice(dpts)
    shotTask = shotTask.decompose(True)
    
    graph = Graph('shotGraph' , shotTask, choice(users))    
    
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
        #time.sleep(0.1)

#import simplejson as json
#print len(json.dumps(graph.toRepresentation()))

