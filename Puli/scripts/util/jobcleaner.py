#!/usr/bin/python
# coding: utf-8
"""
Maintenance script that clean a local puli server.
The cleaning is done in 2 times:
- a first step is to retrieve from the BDD the elements older than a desired number of days
- a final step call a webservice to DELETE the targeted jobs on the server (the server will automatically tags jobs as 'archived' and delete them from memory)

This process is done twice, a first time to clean folder_nodes a second time for the task_nodes

It is usually called from the server itself and croned to execute every day at 7:00
"""

from optparse import OptionParser
from sqlobject import SQLObject, UnicodeCol, IntCol, FloatCol, DateTimeCol, BoolCol, sqlhub, connectionForURI
from sqlobject.sqlbuilder import *
import json
try:
    import http.client as httplib
except ImportError:
    import httplib

VERSION = "1.0"
DISPATCHER = "localhost"


class PuliJobCleaner(object):
    def __init__(self, delay):
        self.delay = delay
        sqlhub.processConnection = connectionForURI("mysql://puliuser:0ct0pus@127.0.0.1/pulidb")

    def processFolderNodes(self):
        taskGroupsResult = sqlhub.processConnection.queryAll("select task_group_id from folder_nodes where end_time < DATE_SUB(now(), interval %s day) and archived = 0" % self.delay)
        taskgroupsIds = ""
        for taskId in taskGroupsResult:
            if taskId[0] is not None:
                taskgroupsIds += str(taskId[0]) + ","
        taskgroupsIds = taskgroupsIds.rstrip(',')

        logging.info( "archiving taskgroups older than %s days --> %r" % (self.delay, taskgroupsIds) )

        result = True
        if options.noaction is False:
            result = self.clean(taskgroupsIds)
        return result 

    def processTaskNodes(self):
        tasksResult = sqlhub.processConnection.queryAll("select task_id from task_nodes where end_time < DATE_SUB(now(), interval %s day) and archived = 0 and parent_id = 1" % self.delay)
        tasksIds = ""
        for taskId in tasksResult:
            if taskId[0] is not None:
                tasksIds += str(taskId[0]) + ","
        tasksIds = tasksIds.rstrip(',')
        logging.info( "archiving tasks older than %s days --> %r" % (self.delay, tasksIds) )

        result = True
        if options.noaction is False:
            res = self.clean(tasksIds)
        return result 


    def clean(self, tasksIds):
        if len(tasksIds):
            print "ids :" + tasksIds
            url = "/tasks/delete/"
            dct = {}
            dct['taskids'] = tasksIds
            body = json.dumps(dct)
            headers = {'Content-Length': len(body)}
            try:
                httpconn = httplib.HTTPConnection(DISPATCHER, 8004)
                httpconn.request('POST', url, body, headers)
                response = httpconn.getresponse()
                httpconn.close()
            except httplib.HTTPException, e:
                logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            except socket.error, e:
                logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            else:
                if response.status == 200:
                    logging.info("archived %s elements" % len(tasksIds) )
                    return True
                else:
                    logging.warning("A problem occured : %s" % response.msg)
                    raise Exception() # A quoi ca sert ! on perd toutes les infos
        return False

    def finish(self):
        sqlhub.processConnection.close()


if __name__ == '__main__':

    import logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info("---")

    # import pudb;pu.db
    parser = OptionParser("PuliJobCleaner v%s - Commandline to archive jobs on Puli" % VERSION)
    parser.add_option("-d", "--delay", action="store", dest="delay", help="number of days from today. All jobs older than that will be archived", default="7")
    parser.add_option("-n", "--noaction", action="store_true", dest="noaction", help="", default=False)
    options, args = parser.parse_args()
    jc = PuliJobCleaner(options.delay)

    if options.noaction is True:
        logging.info("NO ACTION flag is set: nothing will be completed")

    try:
        res = jc.processFolderNodes()
        res = jc.processTaskNodes() or res
        if not res:
            logging.info("nothing to archive")
    except Exception, e:
        logging.info("Exception raised during clean process: %r" % e)
    finally:
        jc.finish()
