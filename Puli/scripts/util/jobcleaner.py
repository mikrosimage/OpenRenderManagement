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
import socket
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
    def __init__(self, delay, delay_start_time=30):
        self.delay = delay
        self.delay_start_time = delay_start_time
        sqlhub.processConnection = connectionForURI("mysql://puliuser:0ct0pus@127.0.0.1/pulidb")

    def processFolderNodes(self):
        taskGroupsResult = sqlhub.processConnection.queryAll(
                "select task_group_id from folder_nodes where ( (end_time < DATE_SUB(now(), interval %s day)) or (creation_time < DATE_SUB(now(), interval %s day)) ) and archived = 0 order by id" 
                % (self.delay, self.delay_start_time)
            )

        # taskGroupsResult = sqlhub.processConnection.queryAll("select task_group_id from folder_nodes where (end_time < DATE_SUB(now(), interval %s day)) and archived = 0" % self.delay)
        taskgroupsIds = ""
        for taskId in taskGroupsResult:
            if taskId[0] is not None:
                taskgroupsIds += str(taskId[0]) + ","
        taskgroupsIds = taskgroupsIds.rstrip(',')

        logging.info( "archiving %d taskgroups older than %s days --> %r" % (len(taskgroupsIds), self.delay, taskgroupsIds) )

        result = True
        if options.noaction is False:
            result = self.clean(taskgroupsIds)
        return result 

    def processTaskNodes(self):
        tasksResult = sqlhub.processConnection.queryAll(
                "select task_id from task_nodes where ( (end_time < DATE_SUB(now(), interval %s day)) or (creation_time < DATE_SUB(now(), interval %s day)) ) and archived = 0 and parent_id = 1 order by id" 
                % (self.delay, self.delay_start_time)
            )
        # tasksResult = sqlhub.processConnection.queryAll("select task_id from task_nodes where end_time < DATE_SUB(now(), interval %s day) and archived = 0 and parent_id = 1" % self.delay)
        tasksIds = ""
        for taskId in tasksResult:
            if taskId[0] is not None:
                tasksIds += str(taskId[0]) + ","
        tasksIds = tasksIds.rstrip(',')
        logging.info( "archiving %d tasks older than %s days --> %r" % (len(tasksIds), self.delay, tasksIds) )

        result = True
        if options.noaction is False:
            result = self.clean(tasksIds)
        return result 


    @staticmethod
    def clean(tasksIds):
        if len(tasksIds):
            url = "/tasks/delete/"
            dct = { 'taskids': tasksIds }
            body = json.dumps(dct)
            headers = {'Content-Length': len(body)}
            try:

                from tornado.httpclient import HTTPClient
                http_client = HTTPClient()
                response = http_client.fetch( "http://%s:%d/tasks/delete" % (DISPATCHER,8004), method="POST", body=body, headers=headers)
            except HTTPError,e:
                logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            except socket.error,e:
                logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            else:
                if response.error:
                    logging.warning( "" )
                    logging.warning( "Error:   %s" % response.error )
                    logging.warning( "         %s" % response.body )
                    logging.warning( "" )
                else:
                    logging.info("Result: %s" % response.body )
                    return True

                # httpconn = httplib.HTTPConnection(DISPATCHER, 8004)
                # httpconn.request('POST', url, body, headers)
                # response = httpconn.getresponse()
                # httpconn.close()
            # except httplib.HTTPException, e:
            #     logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            # except socket.error, e:
            #     logging.warning( '"DELETE %s" failed : %s' % (url, e) )
            # else:
            #     if response.status == 200:
            #         logging.info("archived %s elements" % len(tasksIds) )
            #         return True
            #     else:
            #         logging.warning("A problem occured : %s" % response.msg)
            #         raise Exception() # A quoi ca sert ! on perd toutes les infos
        return False

    @staticmethod
    def finish( ):
        sqlhub.processConnection.close()


if __name__ == '__main__':

    import logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info("---")

    # import pudb;pu.db
    parser = OptionParser("PuliJobCleaner v%s - Commandline to archive jobs on Puli" % VERSION)
    parser.add_option("-d", "--delay_end_time", action="store", dest="delay", help="number of days from today. All jobs ended before the number specified will be archived", default="7")
    parser.add_option("-e", "--delay_start_time", action="store", dest="delay_start_time", help="number of days from today. All jobs started before the number specified will be archived", default="30")
    parser.add_option("-n", "--noaction", action="store_true", dest="noaction", help="", default=False)
    options, args = parser.parse_args()
    jc = PuliJobCleaner(options.delay, options.delay_start_time)

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
