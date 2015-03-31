#!/usr/bin/python
# -*- coding: utf8 -*-
"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import site
import os
import sys
import traceback
import pickle

import threading
import subprocess
import shlex

try:
    import simplejson as json
except ImportError:
    import json

from puliclient.jobs import JobTypeImportError
from puliclient.jobs import CommandError
from puliclient.jobs import TimeoutError

from puliclient.jobs import CommandRunner
from puliclient.jobs import StringParameter


class CallableRunner(CommandRunner):
    execType = StringParameter(mandatory=True)

    def execute(self, arguments, updateCompletion, updateMessage, updateStats, updateLicense):
        self.log.debug("Callable runner arguments: %s" % arguments)

        execType = arguments['execType']

        #
        # Add locations to site path if needed
        #
        # TODO instead, we should start rez with a script to load the callable
        try:
            self.log.info("Add %d site dirs" % len(arguments.get('sysPath', 0)))
            for path in arguments.get('sysPath', None):
                site.addsitedir(path)
        except Exception, e:
            raise JobTypeImportError(e)

        #
        # Retrieve user_args and user_kwargs
        #
        try:
            user_args = json.loads(arguments.get("user_args", None))
            user_kwargs = json.loads(arguments.get("user_kwargs", None))
            self.log.info("args = %s" % user_args)
            self.log.info("kwargs = %s" % user_kwargs)
        except Exception, e:
            print("Problem retrieving args and kwargs: %s)" % e)
            raise CommandError("Problem retrieving args and kwargs: (%s, %s)" % (user_args, user_kwargs))

        #
        # Execute simple function
        #
        if execType == 'function':

            try:
                moduleName = arguments['moduleName']
                funcName = arguments['funcName']
            except Exception:
                raise JobTypeImportError("Missing function or module name in callable arguments: %s", arguments)

            self.log.info("Preparing to load: %s from %s" % (funcName, moduleName))

            #
            # Import module with given moduleName and funcName
            # NB: replace with import_lib.import in python2.7
            #
            try:
                module = __import__(moduleName, fromlist=funcName)
            except ImportError, error:
                traceback.print_exc()
                raise JobTypeImportError("No module '%s' on PYTHONPATH:\n%s. (%s)" % (moduleName, "\n".join(sys.path), error))

            #
            # Load target function in module
            #
            try:
                func = getattr(module, funcName)
            except Exception, e:
                raise JobTypeImportError("No function '%s' defined in module %s (%s)" % (funcName, moduleName, e))

            #
            # Go!
            #
            try:
                func(*user_args, **user_kwargs)
            except Exception, e:
                print "Problem when executing: %s (msg: %s)" % (func, e)
                raise CommandError("Problem when executing: %s (msg: %s)" % (func, e))

        #
        # Execute instance method
        #
        elif execType == 'method':
            self.log.debug("input: %s" % arguments)
            try:
                moduleName = arguments['moduleName']
                className = arguments['className']
                methodName = arguments['methodName']
            except Exception:
                raise JobTypeImportError("Missing function or module name in callable arguments: %s", arguments)

            self.log.info("Preparing to load: %s.%s from %s" % (methodName, className, moduleName))

            module = __import__(moduleName, fromlist=[className])
            jobtype = getattr(module, className)
            func = jobtype()

            #
            # Go!
            #
            try:
                # params = json.loads(arguments.get("params", None))
                # self.log.info("params: %s" % params)
                getattr(func, methodName)(*user_args, **user_kwargs)
            except Exception, e:
                print "Problem when executing: %s (error: %s)" % (func, e)
                raise CommandError("Problem when executing: %s" % func)

        else:
            raise CommandError("Callable not supported")


class RunnerToolkit(object):
    """
    """
    def __init__(self):
        self.process = None
        self.stdout = None
        self.stderr = None
        self.cmdArgs = None
        self.callback = None
        self.keepGoing = True

        pass

    def executeWithTimeout(self, command, timeout):
        """
        Add a timeout callback so that user can have a custom handling of timeout error
        """
        self.process = None

        def target():
            os.umask(2)
            self.process = subprocess.Popen(command, stderr=subprocess.STDOUT, shell=True)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()
        retcode = thread.join(timeout)

        if thread.is_alive():
            self.process.terminate()
            thread.join()
            raise TimeoutError("Execution has taken more than allowed time (timeout=%ds)" % timeout)

        return retcode

    def execute(self, command, timeout, outputCallback=None, timeoutCallback=None):
        """
        | Starts a new process with given command arguments. It is started in a separate thread to have
        | the ability to check periodically the time elapsed in the subprocess.
        | We will try to call the timeoutCallback when necessary if the timeout value is not None and positive.
        | The timeoutCallback is called each time the process spend timeout seconds running.
        | The standard input and output are redirected and line buffered, we will try to call the outputCallback
        | for each line received.

        :param command: A string representing the command line to execute
        :param timeout: Integer specifying the number of seconds to spend in the subprocess before starting the callback function
        :param outputCallback: A function called each time a line is written in subprocess stdout or stderr
        :param timeoutCallback: A function to start each time "timeout" second are spent in the subprocess
        """
        self.cmdArgs = shlex.split(command)
        self.callback = outputCallback
        self.keepGoing = True

        def target():
            """
            Internal thread which starts the subprocess and reads output
            """
            os.umask(2)
            self.process = subprocess.Popen(
                self.cmdArgs,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=False,
                bufsize=1,
                universal_newlines=True)

            for line in iter(self.process.stdout.readline, b''):
                if callable(outputCallback):
                    outputCallback(line)
                else:
                    sys.stdout.write(line)
            (self.stdout, self.stderr) = self.process.communicate()

        # Raise error and interrupt command accordingly if callbacks are not valid
        if outputCallback is not None and not callable(outputCallback):
            raise CommandError("Invalid param: outputCallback=%s must be a callable or None" % outputCallback)

        if timeoutCallback is not None and not callable(timeoutCallback):
            raise CommandError("Invalid param: timeoutCallback=%s must be a callable or None" % timeoutCallback)

        # Create a new thread, it will be in charge of starting a subprocess and reading its output
        # The main thread is in charge of watching if the new thread take too long
        # and eventually calls the timeoutCallback function
        thread = threading.Thread(target=target)
        thread.start()

        if timeout <= 0:
            timeout = None

        thread.join(timeout)

        while thread.is_alive():
            if callable(timeoutCallback):
                keepGoing = timeoutCallback(self.process)
            else:
                print("Invalid timeout callback.")

            # if keepGoing is False:
            #     print("back in runnertoolkit")
            #     self.process.kill()
            #     thread.join()
            #     raise TimeoutError("Execution has taken more than allowed time (timeout=%ds)" % timeout)
            thread.join(timeout)

        return self.process.returncode

    @classmethod
    def executeWithOutput(cls, command, outputCallback=None):
        """
        | Starts a subprocess with given command string. The subprocess is started without shell
        | for safety reason. stderr is send to stdout and stdout is either send to a callback if given or
        | printed on stdout again.
        | If a callback is given for output, it will be called each time a line is printed.

        :param command: a string holding any command line
        :param outputCallback: any callable that we be able to parse line and retrieve useful info from it (usually in the runner)

        :raise CommandError: When any error occurred that should end the command with ERROR status
                             When a subprocess error is raised (OSError or ValueError usually)
        """

        if outputCallback is not None and not callable(outputCallback):
            raise CommandError("Invalid param: outputCallback=%s must be a callable or None" % outputCallback)

        shlex.split(command)
        try:
            p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False, bufsize=1)

            for line in iter(p.stdout.readline, b''):
                if callable(outputCallback):
                    outputCallback(line)
                else:
                    sys.stdout.write(line)

            (stdout, stderr) = p.communicate()
            return p.returncode

        except ValueError as e:
            raise CommandError("%s" % e)

        except OSError as e:
            raise CommandError("Error during process exec: %s" % e)

        except Exception as e:
            raise e

    @classmethod
    def sendMail(cls, subject, body, toAddress, ccAddress='',
                 fromAddress='puliserver', SMTPhost='aspmx.l.google.com', verbose=False):
        """
        | Simple class method to send an email during a workflow.
        | It uses the smtplib an access the MX host: aspmx.l.google.com

        :param subject str: the subject of the mail as a string
        :param body str: the content of the mail as a string
        :param toAddress str: a single target address or a comma separated  list of addresses
        :param ccAddress str: a comma separated list of additionnal recipients
        :param fromAddress str: a single source email address (usually puliserver@yourdomain.com)
        :param SMTPHost str: string representing the SMTP server address to use
        :param verbose bool: if set to true, method prints email summary to stdout

        :raise Exception: any exception raised when starting connection to SMTP server and sending email
        """
        import smtplib
        from email.mime.text import MIMEText

        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = fromAddress
        msg['To'] = toAddress
        msg['Cc'] = ccAddress

        recipients = toAddress.split(',')
        recipients.extend(ccAddress.split(','))

        try:
            s = smtplib.SMTP(SMTPhost)
            s.sendmail(fromAddress, recipients, msg.as_string())
            s.quit()
        except Exception, e:
            print "Error using sendmail, probably malformed email addresses."
            raise Exception("Error using sendmail: %r" % e)

        if verbose:
            print "Email summary info"
            print "  from.....: %r" % fromAddress
            print "  to.......: %r" % toAddress
            print "  cc.......: %r" % ccAddress
            print "  subject..: %r" % subject
            print "  body.....: %r" % body

    @classmethod
    def runGraphInXterm(cls, graph, keepWindowOpen=True):
        """
        Specifically runs a graph locally in a separate xterm windows and subprocess.
        Several possibilities to render locally:
          - use graph.execute() in current thread --> PB: blocks the GUI
          - use graph.execute(detached=True) in a separate thread --> PB: force to quit maya to stop long process
          - pickle graph to dump and reload it in xterm subprocess --> WARN: potential pb if dump is too long
        When using first 2 methods, log output is in reversed order making it difficult to debug

        Method details (pickle graph):
          1. create a data temp file with the pickled graph
          2. create a script temp with a minimal python code to load the data file and "execute" it
          3. Start a command line process to open xterm and execute the script file
             Precisely the command has 3 steps:
             - open xterm with title "Running" and execution command
             - change title to "finished" after command execution
             - eventually starts a new shell in interactive mode to keep xterm opened

        :param graph: a puliclient graph to execute
        :raise CommandError: When any error occurred that should end the command with ERROR status
                             When a subprocess error is raised (OSError or ValueError usually)
        """

        try:
            from tempfile import NamedTemporaryFile

            graphName = ''
            with NamedTemporaryFile(mode='w', prefix='localGraph_', delete=False) as graphFile:
                graphFile.write(pickle.dumps(graph))
                graphName = graphFile.name

            script = """
import pickle
import time

with open("__graphFile__") as file:
  g = pickle.loads(file.read())
  g.execute()
"""
            script = script.replace("__graphFile__", graphName)

            with NamedTemporaryFile(mode='w', prefix='localScript_', delete=False) as scriptFile:
                scriptFile.write(script)
                # args = 'xterm -title "Running" -e "python {script}'.format(script=scriptFile.name)

                if keepWindowOpen:
                    args = 'xterm -title "Running" -e "python {script}' \
                           '; echo -ne \'\033]0;Finished\007\'' \
                           '; sh -i"'.format(script=scriptFile.name)
                else:
                    args = 'xterm -title "Running" -e "python {script}"'.format(script=scriptFile.name)

            print "Start local render, please check your terminal for details."
            process = subprocess.Popen(args, shell=True)
            print "Process started: {pid}".format(pid=process.pid)

        except ValueError as e:
            raise CommandError("%s" % e)

        except OSError as e:
            raise CommandError("Error during process exec: %s" % e)

        except Exception as e:
            raise e
