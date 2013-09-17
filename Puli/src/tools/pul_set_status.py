#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
pul_set_status.py: Utilitaire d'edition des jobs dans puli
Permet de lancer des requetes http au serveur pour editer le status des jobs.

requete: edit?value=0&constraint_user=jsa
		 edit?value=1&constraint_user=jsa&constraint_user=render
		 edit?value=0&constraint_id=1&constraint_id=2&constraint_id=3

value: represente le nouveau statut a attribuer au job [0-6]
		0="Blocked"
		1="Ready"
		2="Running"
		3="Done"
		4="Error"
		5="Canceled"
		6="Paused"
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


# Imports from libs
from tornado import ioloop, escape
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from optparse import OptionParser, IndentedHelpFormatter
from datetime import datetime
import simplejson as json
import sys
import time
import urllib

# Imports from local dir
from settings import Settings
from common import ConstraintFactory

VERBOSE = Settings.verbose

REQUEST_BEGIN_TIME = ''
REQUEST_END_TIME = ''


def handle_request(response):
	"""
	Callback for handling the request result.
	We try to load the data as JSON and display it according to the arguments or default specification
	"""

	if response.error:
		print "Error:", response.error
		sys.exit()

	REQUEST_END_TIME = time.time() - REQUEST_BEGIN_TIME

	if VERBOSE: print ""
	if VERBOSE: print "Getting response for request \""+_target+"\" in " + str(REQUEST_END_TIME)

	if response.body == "":
		print ""
		print "No jobs in queue, nothing to edit."
		print ""
		sys.exit()

	try:
		_body = json.loads( response.body )
		if VERBOSE: print json.dumps(_body, indent=4)

		print ""
		print "Summary: Update ended in %.2f ms." % (_body['summary']['requestTime']*1000)
		print "     Total jobs: %d" % _body['summary']['totalInDispatcher']
		print "  Filtered jobs: %d" % _body['summary']['filteredCount']
		print "   Updated jobs: %d" % _body['summary']['editedCount']
		print ""
		print "Updated items: %s" % _body['editedJobs']
		print ""

	except Exception, e:
		print "Error reading json body."

	# Quit loop
	ioloop.IOLoop.instance().stop()


###########################################################################################################################

class PlainHelpFormatter(IndentedHelpFormatter): 
    '''
    Subclass of OptParse format handler, will allow to have a raw text formatting in usage and desc fields.
    '''
    def format_description(self, description):
        if description:
            return description + "\n"
        else:
            return ""

def process_args():
    '''
    Manages arguments parsing definition and help information
    '''

    usage = "usage: %prog -u <new status> [general options] [restriction list]"
    desc="""Updates status of one or several jobs.
To restrict the action to jobs of interest, a list of zero or more restrictions may be supplied. Each restriction may be one of:
    - a user --> matches all jobs owned by the specified owner
    - (TODO) a job id  --> matches the specified job
    - (TODO)a date  --> matches all jobs created after the given date
    - a constraint expression --> matches all jobs that satisfy the specified expressions: FILTER="VALUE"
        user=jsa
        status=1 (value is a number corresponding to the states: "Blocked", "Ready", "Running", "Done", "Error", "Cancel", "Pause")
        name="mon job"
        creationtime="2013-09-09 14:00:00" (It filters all jobs created AFTER the given date/time)
"""

    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1", formatter=PlainHelpFormatter() )
 
    parser.add_option("-C", "--constraint", action="append", type="string", help="Allow user to specify one or more filter constraints")

    # TODO limit range of values or use a "choice" string sequence
    parser.add_option("--set", action="store", type="int", metavar="STATUS", help="Allow user to specify a new status value [1-6]")

    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="Verbose mode [%default]", default=False)

    parser.add_option("-s", "--server", action="store", dest="hostname", help="Specified a target host to send the request")
    parser.add_option("-p", "--port", action="store", dest="port", help="Specified a target port")

    options, args = parser.parse_args()

    return options, args



if __name__ == '__main__':

	(options, args) = process_args()
	VERBOSE = options.verbose

	if VERBOSE:
		print "Command options: %s" % options
		print "Command arguments: %s" % args

	_target = "edit?"
	_hostname = Settings.hostname
	_port = Settings.port

	#
	# Manually check set_status specific rules:
	#   - "update" option must exists because it is a requirement of pul_set_status
	#   - we don't allow to affect the whole queue, a restriction or constraint must be present
	#
	if not hasattr(options, 'set'):
		print "Usage: pul_set_status [general options] [restriction list] \
				\n\npul_set_status.py: error: parameter --set is required."
		sys.exit()
	elif options.set not in xrange(0,7):
		print "Usage: pul_set_status [general options] [restriction list] \
				\n\npul_set_status.py: error: parameter --set must be in the range [0-6]."
		sys.exit()

	else:
		_target += "update_status=%d" % options.set
		# print 'Adding "update" in target query'
		# # options['update']="update_status=4"# ("update=%s"%options.update)
		# print "opt = "+options.update
		# print "Command options: %s" % options

	if (len(args)==0) and not options.constraint:
		print "Usage: pul_set_status [general options] [restriction list]\
				\n\npul_set_status.py: error: It is not allowed to set a new status without any restriction. \
				\n                          Please specify a restriction or a constraint."
		sys.exit()


	#
	# Create a query string
	#
	_target += ConstraintFactory.makeQuery( pUserArguments=args, pUserOptions=options)


	#
	# Update hotsname/port if given as arguments
	#
	if options.hostname is not None:
		_hostname = options.hostname

	if options.port is not None:
		_port = options.port


	if VERBOSE:
		print "Host: %s" % _hostname
		print "Port: %s" % _port
		print "Request: %s" % _target

	_url = "http://%s:%s/%s" % ( _hostname, _port, _target )

	headers = {'Content-Type': 'application/json; charset=UTF-8'}
	body = {}

	_request = HTTPRequest( _url, method='PUT', headers = headers, body = json.dumps(body) )

	http_client = AsyncHTTPClient()

	REQUEST_BEGIN_TIME = time.time()
	http_client.fetch( _request, handle_request )

	ioloop.IOLoop.instance().start()
