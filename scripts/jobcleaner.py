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
DISPATCHER = "localhost"
    
class PuliJobCleaner(object):
    
    def __init__(self, delay):
        self.delay = delay
        sqlhub.processConnection = connectionForURI("mysql://puliuser:0ct0pus@127.0.0.1/pulidb")
    
    def processFolderNodes(self):
        taskGroupsResult = sqlhub.processConnection.queryAll("select task_group_id from folder_nodes where end_time < DATE_SUB(current_date, interval %s day) and archived = 0" % self.delay)
        taskgroupsIds = ""
        for taskId in taskGroupsResult:
            if taskId[0] is not None:
                taskgroupsIds += str(taskId[0]) + ","
        print "archiving taskgroups older than %s days" % self.delay
        return self.clean(taskgroupsIds)
    
    def processTaskNodes(self):
        tasksResult = sqlhub.processConnection.queryAll("select task_id from task_nodes where end_time < DATE_SUB(current_date, interval %s day) and archived = 0" % self.delay)
        tasksIds = ""
        for taskId in tasksResult:
            if taskId[0] is not None:
                tasksIds += str(taskId[0]) + ","
        print "archiving tasks older than %s days" % self.delay
        return self.clean(tasksIds)
    
    def clean(self, tasksIds):
        if len(tasksIds):
            print "ids :" + tasksIds
            url = "/tasks/"
            dct = {}
            dct['taskids'] = tasksIds
            body = json.dumps(dct)
            headers = {'Content-Length': len(body)}
            try:
                httpconn = httplib.HTTPConnection(DISPATCHER, 8004)
                httpconn.request('DELETE', url, body, headers)
                response = httpconn.getresponse()
                httpconn.close()
            except httplib.HTTPException, e:
                print '"DELETE %s" failed : %s' % (url, e)
            except socket.error, e:
                print '"DELETE %s" failed : %s' % (url, e)
            else:
                if response.status == 200:
                    print "archived %s elements" % len(tasksIds)
                    return True
                else:
                    print "A problem occured : %s" % response.msg
                    raise Exception()
        return False
    
    def finish(self):
        sqlhub.processConnection.close()


if __name__ == '__main__':
    parser = OptionParser("PuliJobCleaner v%s - Commandline to archive jobs on Puli" % VERSION)
    parser.add_option("-d", "--delay", action="store", dest="delay", help="number of days from today. All jobs older than that will be archived", default="7")
    options, args = parser.parse_args()
    jc = PuliJobCleaner(options.delay)
    try:
        res = jc.processFolderNodes()
        res = jc.processTaskNodes() or res
        if not res:
            print "nothing to archive"
    except Exception, e:
        raise e
    finally:
        jc.finish()
