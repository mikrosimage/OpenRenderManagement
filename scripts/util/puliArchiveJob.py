#!/usr/bin/python2.7
# coding: utf-8

# Function must be called in this order : 
#       processFolderNodes()
#       processTaskNodes()
#       processTaskGroups()
#       processTasks()
#       processCommands()
#       processPoolShares()
#       processDependencies()

# We don't use commit/rollbakc transactions since the DB engine is MYISAM and do not support it. 

"""
Script to archive a job by its ID.
"""

import argparse
import MySQLdb
import signal
import time
from contextlib import closing


class PuliArchiveJob(object):
    def __init__(self, connection, jobID):
        self.connection = connection
        self.jobID = jobID

    def getFolderNodesID(self, parent_id, folderNodeList):
        with closing(self.connection.cursor()) as cursor:
            sql = "SELECT id FROM folder_nodes WHERE parent_id = {parentID};".format(parentID=parent_id)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                folderNodeList.append(row[0])
                self.getFolderNodesID(row[0], folderNodeList)


    def processFolderNodes(self):
        print "---- Process folderNodes ---"
        with closing(self.connection.cursor()) as cursor:

            # Get folder nodes ID
            self.folderNodesIDsAsString = "(%s" % self.jobID
            folderNodeList = []
            self.getFolderNodesID(self.jobID, folderNodeList)
            for id in folderNodeList:
                self.folderNodesIDsAsString += "," + str(id)
            self.folderNodesIDsAsString += ")"
            print self.folderNodesIDsAsString
            
            # Update folder nodes
            nbFolderNodesUpdated = 0
            cursor.execute("LOCK TABLES folder_nodes WRITE;")
            sql = "UPDATE folder_nodes SET archived=1 where id IN {id} AND archived=0;".format(id=self.folderNodesIDsAsString)
            nbFolderNodesUpdated = cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            # Print information for user
            if nbFolderNodesUpdated  < 1:
                print "No folder node to archive"
                return
            print "Folder nodes archived : " + self.folderNodesIDsAsString
        

    def processTaskNodes(self):
        print "---- Process taskNodes ---"
        with closing(self.connection.cursor()) as cursor:
            # Get task nodes ID
            taskNodesIDsAsString = "("
            taskNodesIDs = []
            sql = "SELECT id FROM task_nodes WHERE parent_id IN {folderNodesIDs} AND archived=0;".format(folderNodesIDs=self.folderNodesIDsAsString)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                taskNodesIDs.append(str(row[0]))
            taskNodesIDsAsString += ",".join(taskNodesIDs) + ")"

            # Update task nodes
            if not taskNodesIDs:
                print "No task node to archive"
                return
            cursor.execute("LOCK TABLES task_nodes WRITE;")
            sql = "UPDATE task_nodes SET archived=1 WHERE id IN {taskNodesIDs}".format(taskNodesIDs=taskNodesIDsAsString)
            cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            # Print information for user
            print "Task nodes archived : " + taskNodesIDsAsString


    def processTaskGroups(self):
        print "---- Process taskGroups ---"
        with closing(self.connection.cursor()) as cursor:
            # Get task groups ID
            taskGroupsIDsAsString = "("
            taskGroupsIDs = []
            sql = "SELECT task_group_id from folder_nodes WHERE id IN {folderNodesIDs};".format(folderNodesIDs=self.folderNodesIDsAsString)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                taskGroupsIDs.append(str(row[0]))
            taskGroupsIDsAsString += ",".join(taskGroupsIDs) + ")"

            # Update task groups
            nbTaskGroupsUpdated = 0
            if not taskGroupsIDs:
                print "No task group to archive"
                return
            cursor.execute("LOCK TABLES task_groups WRITE;")
            sql = "UPDATE task_groups SET archived=1 WHERE id IN {taskGroupsIDs} AND archived=0".format(taskGroupsIDs=taskGroupsIDsAsString)
            nbTaskGroupsUpdated = cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")


            # Print information for user
            if nbTaskGroupsUpdated < 1:
                print "No task group to archive"
                return
            print "Task groups archived : " + taskGroupsIDsAsString

    def processTasks(self):
        print "---- Process tasks ---"
        with closing(self.connection.cursor()) as cursor:
            # Get tasks ID
            self.tasksIDsAsString = "("
            tasksIDs = []
            sql = "SELECT task_id FROM task_nodes WHERE parent_id IN {folderNodesIDs};".format(folderNodesIDs=self.folderNodesIDsAsString)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                tasksIDs.append(str(row[0]))
            self.tasksIDsAsString += ",".join(tasksIDs) + ")"

            # Update tasks
            if not tasksIDs:
                print "No task to archive"
                return
            cursor.execute("LOCK TABLES tasks WRITE;")
            sql = "UPDATE tasks SET archived=1 WHERE id IN {tasksIDs} AND archived=0;".format(tasksIDs=self.tasksIDsAsString)
            nbTaskArchived = 0
            nbTaskArchived = cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            # Print information for user
            if nbTaskArchived  < 1:
                print "No task to archive"
                return
            print "Tasks archived : " + self.tasksIDsAsString


    def processCommands(self):
        print "---- Process commands ---"
        with closing(self.connection.cursor()) as cursor:
            # Get commands ID
            commandsIDsAsString = "("
            commandsIDs = []
            sql = "SELECT id FROM commands WHERE task_id IN {tasksIds} AND archived=0;".format(tasksIds=self.tasksIDsAsString)    
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                commandsIDs.append(str(row[0]))
            commandsIDsAsString += ",".join(commandsIDs) + ")"

            # Update commands
            if not commandsIDs:
                print "No command to archive"
                return
            cursor.execute("LOCK TABLES commands WRITE;")
            sql = "UPDATE commands SET archived=1 WHERE id IN {commandsIds}".format(commandsIds=commandsIDsAsString)
            cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            print "Commands archived : " + commandsIDsAsString

    def processPoolShares(self):
        print "---- Process pool shares ---"
        with closing(self.connection.cursor()) as cursor:
            # Get pool shares ID
            poolSharesIDsAsString = "("
            poolSharesIDs = []
            sql = "SELECT id FROM pool_shares WHERE node_id IN {folderNodes} AND archived=0;".format(folderNodes=self.folderNodesIDsAsString)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                poolSharesIDs.append(str(row[0]))
            poolSharesIDsAsString += ",".join(poolSharesIDs) + ")"

            # Update pool shares
            if not poolSharesIDs:
                print "No pool share to archive"
                return
            cursor.execute("LOCK TABLES pool_shares WRITE;")
            sql = "UPDATE pool_shares SET archived=1 WHERE id IN {poolSharesIds}".format(poolSharesIds=poolSharesIDsAsString)
            cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            # Print information for user
            print "Pool shares archived : " + poolSharesIDsAsString


    def processDependencies(self):
        print "---- Process dependencies ---"
        with closing(self.connection.cursor()) as cursor:
            # Get dependendies ID
            dependendiesIDsAsString = "("
            dependenciesIDs = []
            sql = "SELECT id FROM dependencies WHERE task_nodes_id IN {tasksIDs} AND archived=0;".format(tasksIDs=self.tasksIDsAsString)
            cursor.execute(sql)
            data = cursor.fetchall()
            for row in data:
                dependenciesIDs.append(str(row[0]))
            dependendiesIDsAsString += ",".join(dependenciesIDs) + ")"

            # Update dependencies
            if not dependenciesIDs:
                print "No dependency to archive"
                return
            cursor.execute("LOCK TABLES dependencies WRITE;")
            sql = "UPDATE dependencies SET archived=1 WHERE id IN {dependenciesIds}".format(dependenciesIds=dependendiesIDsAsString)
            cursor.execute(sql)
            cursor.execute("UNLOCK TABLES;")

            # Print information for user
            print "Dependencies archived : " + dependendiesIDsAsString


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='"PuliArchiveJob  - Commandline to archive jobs on Puli')
    parser.add_argument('dispatcher', 
                   metavar='DISPATCHER',
                   type=str,
                   help='Dispatcher on which we want to archive job')
    parser.add_argument('user', 
                   metavar='USER',
                   type=str,
                   help='User for connection to database')
    parser.add_argument('password', 
                   metavar='PASSWORD',
                   type=str,
                   help='Password for connection to database')
    parser.add_argument('database', 
                   metavar='DATABASE',
                   type=str,
                   help='Data base for connection to database')
    parser.add_argument('job_id', 
                   metavar='JOB_ID',
                   type=int,
                   help='ID of the job to archive')
    args = parser.parse_args()


    # Open database connection
    with closing(MySQLdb.connect(args.dispatcher, args.user, args.password, args.database)) as connection:
        archivist = PuliArchiveJob(connection, args.job_id)

        # Process
        archivist.processFolderNodes()  
        archivist.processTaskNodes()
        archivist.processTaskGroups()
        archivist.processTasks()
        archivist.processCommands()
        archivist.processPoolShares()
        archivist.processDependencies()