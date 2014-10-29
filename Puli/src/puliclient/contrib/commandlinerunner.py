'''
Created on Jan 12, 2010

@author: Arnaud Chassagne
'''
import subprocess

from puliclient.jobs import CommandRunner, StringParameter


class CommandLineRunner(CommandRunner):

    args = StringParameter()

    def execute(self, arguments, updateCompletion, updateMessage):
        args = arguments['args']
        print 'Running command "%s"' % args
        updateCompletion(0)
        # subprocess.call(args, close_fds=True, shell=True)
        subprocess.check_call(args, close_fds=True, shell=True)

        updateCompletion(1)
