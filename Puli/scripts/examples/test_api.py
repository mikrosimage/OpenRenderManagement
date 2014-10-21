from __future__ import absolute_import

import time
import requests

from puliclient.server import *

rn = RenderNodeHandler.getRenderNode( "vfxpc64", 9000 )
#rnh = RenderNodeHandler()
#rn = rnh.getRenderNode( "vfxpc64", 9000 )

if rn:
	print "----------------------"
	print "Render node %s has a total of %d MB RAM but only %d MB are considered free by the system." % (rn.name, rn.ramSize, rn.systemFreeRam)
	print "Detailled data: "
	print rn.to_JSON( indent=4 )

	#print ""
	#print "status=%s"% rn.status
	#print "Pausing: %s" % rn.pause()
	#print "status=%s"% rn.status
	#
	#time.sleep(1)
	#rn._refresh()
	#print "status=%s"% rn.status
	#print ""
	#print "Resume: %s" % rn.resume()
	#
	#time.sleep(1)
	#rn._refresh()
	#print "status=%s"% rn.status
	
job = JobHandler.getJob( id=3)
if job:
	print "----------------------"
	print "Job: %s" % (job)
	print "Detailled data: "
	print job.to_JSON(indent=4)

	#import pudb;pu.db
	print "PRIO"
	print "before: %d" % job.dispatchKey
	job.setDispatchKey(job.dispatchKey+10)
	print "after: %d" % job.dispatchKey

	print "POOL:"
	#print "  before: %d" % job.pool
	job.setPool("toto")

	#print "  after: %d" % job.pool

	print "MAXRN"
	print "before: %d" % job.maxRN
	job.setMaxRn(job.maxRN+1)
	print "after: %d" % job.maxRN
	


#jobs = JobHandler.getJobs( [13, 9] )
#if jobs:
#	print "----------------------"
#	print "Jobs retrieved: %d" % (len(jobs))
#	print "Detailled data: "
#	for job in jobs:
#		print job
	
	