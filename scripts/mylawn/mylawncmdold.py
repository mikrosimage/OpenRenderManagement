#!/usr/bin/python
# -*- coding: iso-8859-1 -*-
#
#Created on Mar 22, 2010

#@author: nre
#enable user to allow local renders or not
#using killfile method for both alfserver ans puliworker


##################################################
## nre01/2011
## rajout de l'option -c pour faire un check du server status seulement
## util quand la commande est utilisee en ssh sur plusieures machines a la fois
## ex: for i in `cat machineListe.txt | grep lin` ; do ssh $i /s/apps/lin/bin/mylawncmd -c;done

####
# creation de logFile avec droit foireux...


import os, sys, getopt ,time, stat
date = time.strftime('%Y/%m/%d %H:%M',time.localtime())
version = 0.4

##################################################
## On check si des process donnes sont en cours (alfserver ou puliworker)
def pidExists(processus):
    #pid = os.popen('pidof %s |wc -w ' % processus).read()
    pidSearch = os.popen('ps aux | grep -i %s| grep -v grep' % processus ).read()
    pid = len(pidSearch)
    if ( pid == 0):
        caTourne = False
    else:
        caTourne = True
    return caTourne

###################################################
## Create / Delete killFile
def setRender(activation, killfile, content=""):
    #if DEBUG: print("killfile = %s" % (killfile))
    if not activation:
        #print "\tRender ENABLED"
        os.remove(killfile)
    else:
        #print "\tRender DISABLED"
        f = open(killfile, 'w')
        f.write(content)
        f.close()
        os.chmod(killfile,0666)

##################################################
## On check si des process donnes sont en cours (maya kick Nuke)
def checkRender(processes):
    CHECK = False
    print("Checking possible local renders...")
    login = "render"
    for process in processes:
        pidSearch = os.popen("ps aux | grep "+login+" | grep -i %s| grep -v grep" % process ).read()
        if pidSearch != "":
            print(pidSearch)
            CHECK = True
        else:
            print("\t'%s' render quiet for login '%s'" % (process, login))
    return CHECK

##################################################################
# Affichage d'un message a l'ecran
#
def message(message):
    global logFile, LOG
    print(message)
    logMess = "%s %s" % (date, message)
    if LOG: ajoute(logFile, logMess)

##################################################################
# Ecriture du fichier
#
def ajoute(outFile, outData):
    f = open(outFile, 'a')
    f.write(outData)
    f.close()
    os.umask(0)
    os.chmod(outFile,0777)
    print "Writing file :\n   " + outFile


##################################################################
# aide pour la ligne de commande
#
def Usage():
    print "Usage: " + os.path.basename(sys.argv[0]) + " [hkc] boolean"
    print "'Do not step on My Lawn'. command line to allow or not local renders"
    print "Version: " + str(version) + " - nre 2010"
    print "Flags:"
    print "  -h            help"
    print "  -k            kill all render processes"
    print "  -c            only check server status & killfile"
    #print "  -l path       log action in path file"
    print("  renderStatus  1/0 DISABLE/ENABLE tranquility")
    print("                1 = render DISABLED")
    print("                0 = render ENABLED")
    print "ex: to disable job submissions on my computer:"
    print "    " + os.path.basename(sys.argv[0]) + " 1"
    sys.exit(0)

try:
    opts, args = getopt.getopt(sys.argv[1:], "hdkca:p:", ["help"])
except getopt.GetoptError, err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    Usage()
    sys.exit(2)
# process options
global DEBUG, killfile, OKTORENDER, logFile, LOG
workers = ["alfserver", "workerd.py"]
DEBUG = False
RENDERING = False
KILL = False
workerState =""
running = {}
logFile =""
LOG = False
ALLCHECK = True

for o, a in opts:
    if o in ("-h", "--help"):
        Usage()
        sys.exit()
    elif o == ("-d"):
        DEBUG = True
    elif o == ("-a"):
        worker = "alfserver"
        workerState = int(a)
    elif o == ("-p"):
        worker = "workerd.py"
        workerState = int(a)
    elif o == ("-k"):
        KILL = True
    elif o == ("-c"):
        print("Checking server status only")
        ALLCHECK = False
    else:
        Usage()
        sys.exit(0)

killfileName = "killfile"
killfileDir = "/tmp/render"
killfile = killfileDir+"/"+killfileName

#### Check de l'existance sur log file
if logFile != "":
    if not os.path.exists(os.path.dirname(logFile)):
        print("Specified logFile directory doesn't exists. Abort")
        sys.exit(1)
    else:
        LOG = True

#### Creation du repOut pour le killFile
if not os.path.exists(killfileDir):
    os.umask(0)
    os.mkdir(killfileDir, 0777)
    if DEBUG: print("%s created" % killfileDir)

###################################################
### on check si les workers tournent sur la machines
print("Worker status on '%s':" % os.environ['HOSTNAME'])
for renderw in workers:
    if pidExists(renderw):
        print("\t'%s' is UP" % renderw)
        running[renderw] = True
    else:
        print("\t'%s' is DOWN" % renderw)
        running[renderw] = False

### Check killFile
if os.path.exists(killfile):
    killFile = True
else:
    killFile = False
    

### Check user desired status
try:
    okToRender = int(args[0])
    CHANGE = True
except:
    CHANGE = False
    

#### Change killfile
if CHANGE:
    if DEBUG: print("Change Killfile status...")
    if killFile != okToRender:
        setRender(okToRender, killfile)

#### On affiche le status du killfileDir
### s'il existe render disabled
### sinon  render enabled
if os.path.exists(killfile):
    if DEBUG: print("\tkillfile '%s' exists" % killfile)
    print("\trender DISABLED")
else:
    if DEBUG: print("\tkillfile '%s' does not exists" % killfile)
    print("\trender ENABLED")

### If we ask only to check the status of the servers (-c) we exit now
if not ALLCHECK:
    sys.exit()
    
### On check sil y a des render locaux en cours
processes = ["kick", "Nuke", "maya"]
RENDERING = checkRender(processes)
if DEBUG: print("RENDERING= %s" % RENDERING)
#### Si c'est demande on tue les render locaux en cours (pour le login render only)
if KILL and RENDERING:
    os.system("sudo pkill -U render maya*")
    os.system("sudo pkill -U render Nuke*")
    os.system("sudo pkill -U render kick")
    ## on le met deux fois car la commande ne tue que le premier proccess qu'elle trouve
    os.system("sudo pkill -U render maya*")
    print("Kill command have been sent.")
    RENDERING = checkRender(processes)
#if KILL and RENDERING and isPuli:
#	setRender(okToRender, killfile,"-1")

##################################
## Start or Stop worker
## worker state Should be 1 or 0
if workerState !="":
    if workerState:
        workerAction= "start"
    elif not workerState:
        workerAction = "stop"
    else :
        print ("Worker status can be start/stop")
        sys.exit(1)
    ##### We change the worker status only if it is different from its actual status
    if workerState != running[worker]:
        print("Setting %s to %s..." % (worker, workerAction))
        if worker == "workerd.py":
            action = os.popen("sudo /etc/init.d/puliworker %s" % (workerAction )).read()
        elif worker == "alfserver":
            action = os.popen("sudo /etc/init.d/alfserver %s" % (workerAction )).read()
        print("\t%s" % action)
    else:
        print("No action needed on worker '%s'" % worker)
