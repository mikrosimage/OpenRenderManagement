#!/usr/bin/python
# coding: utf-8

from optparse import OptionParser
from sqlobject import SQLObject, UnicodeCol, IntCol, FloatCol, DateTimeCol, BoolCol, sqlhub, connectionForURI
from sqlobject.sqlbuilder import *
import json
try:
    import http.client as httplib
except ImportError:
    import httplib
    
VERSION = "1.0"
DISPATCHER = "puliserver"
    
if __name__ == '__main__':
    parser = OptionParser("PuliJobCleaner v%s - Commandline to archive jobs on Puli" % VERSION)
    parser.add_option("-d", "--delay", action="store", dest="delay", help="number of days from today. All jobs older than that will be archived", default="7")
    options, args = parser.parse_args()
    httpconn = httplib.HTTPConnection(DISPATCHER, 8004)
    sqlhub.processConnection = connectionForURI("mysql://puliuser:0ct0pus@%s/pulidb" % DISPATCHER)
    taskGroupsResult = sqlhub.processConnection.queryAll("select task_group_id from folder_nodes where end_time < DATE_SUB(current_date, interval %s day) and archived = 0" % options.delay)
    tasksResult = sqlhub.processConnection.queryAll("select task_id from task_nodes where end_time < DATE_SUB(current_date, interval %s day) and archived = 0" % options.delay)
    tasks = taskGroupsResult + tasksResult
    tasksIds = ""
    for taskId in tasks:
        if taskId[0] is not None:
            tasksIds += str(taskId[0]) + ","
    if len(tasksIds):
        print "archiving tasks older than %s days" % options.delay
        print "ids :" + tasksIds
        url = "/tasks/"
        dct = {}
        dct['taskids'] = tasksIds
        body = json.dumps(dct)
        headers = {'Content-Length': len(body)}
        try:
            httpconn.request('DELETE', url, body, headers)
            response = httpconn.getresponse()
        except httplib.HTTPException:
            LOGGER.exception('"DELETE %s" failed', url)
        except socket.error:
            LOGGER.exception('"DELETE %s" failed', url)
        else:
            if response.status == 200:
                print "archived %s tasks" % len(tasks)
        finally:
            httpconn.close()
    else:
        print "nothing to archive"
