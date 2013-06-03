#!/usr/bin/python
# coding: utf-8

from optparse import OptionParser
import os
import sys
import time

VERSION = "1.0"
KILLFILE = "/tmp/render/killfile"


class MyLawn(object):
    def process_args(self):
        parser = OptionParser("MyLawn v%s - Commandline to allow or forbid local renders" % VERSION)
        parser.add_option("-k", "--kill", action="store_true", dest="killproc", help="kill all render processes and disable rendering", default=False)
        parser.add_option("-c", "--check", action="store_true", dest="checkstatusonly", help="only check server status and killfile presence", default=False)
        parser.add_option("-s", "--status", action="store", type="int", dest="status", help="'mylawn 0' to enable rendering (deletes killfile)\n'mylawn 1' to disable rendering (creates killfile)")
        return parser.parse_args()

    def setRender(self, activate, killproc=False):
        if activate or killproc:
            if os.path.isfile(KILLFILE):
                os.remove(KILLFILE)
            # FIXME test
            time.sleep(0.5)
            #
            with open(KILLFILE, 'w') as f:
                if killproc:
                    f.write("-1")
            os.chmod(KILLFILE, 0666)
            print "\t--- killfile created! Render is now DISABLED ---\n"
            if killproc:
                print "\t--- all renders have been killed ---\n"
        else:
            os.remove(KILLFILE)
            print "\t--- killfile deleted! Render is now ENABLED ---\n"

    def checkProcess(self, process, login=""):
        cmdline = "ps aux | "
        if len(login):
            cmdline += "grep %s | " % login
        cmdline += "grep -i %s| grep -v grep" % process
        pidSearch = os.popen(cmdline).read()
        if len(pidSearch):
            return True
        return False

    def killProcess(self, process):
        os.system("sudo pkill -U render %s*" % process)


if __name__ == '__main__':
    ml = MyLawn()
    options, args = ml.process_args()
    killFileDir = os.path.dirname(KILLFILE)
    status = options.status
    if status is None and len(args) != 0:
        status = int(args[0])

    #### Creation du repOut pour le killFile
    if not os.path.isdir(killFileDir):
        os.umask(0)
        os.mkdir(killFileDir, 0777)

    ### on check si les workers tournent sur la machine
    #if ml.checkProcess("alfserver"):
    #    print "\nAlfred service :\tUP"
    #else:
    #    print "\nAlfred service :\tDOWN"

    if ml.checkProcess("workerd.py"):
        print "Puli service :\t\tUP"
    else:
        print "Puli service :\t\tDOWN"

    ### print current status
    if os.path.isfile(KILLFILE):
        print "\n\t--- Render currently DISABLED ---"
    else:
        print "\n\t--- Render currently ENABLED ---"

    ### check of rendering processes
    processes = ["MtoaRunner", "NukeRunner", "MentalrayRunner"]
    rendering = False
    print ""
    for process in processes:
        if ml.checkProcess(process, 'render'):
            rendering = True
            print process.replace("Runner", "") + " rendering"
    if not rendering:
        print "no active rendering process for login 'render'"
    print ""

    ### end process if check only
    if options.checkstatusonly:
        sys.exit()

    #### create or remove killfile
    if status is not None or options.killproc:
        print "Setting killfile status..."
        if status != os.path.isfile(KILLFILE):
            ml.setRender(status, options.killproc)
        else:
            print "Nothing to do.\n"

    #### Si c'est demande on tue les render locaux en cours (pour le login render only)
    #if options.killproc and rendering:
    #    for process in processes:
    #        ml.killProcess(process)
        ## on le met deux fois car la commande ne tue que le premier proccess qu'elle trouve
    #    ml.killProcess("maya")
    #    print "Kill commands have been sent."

