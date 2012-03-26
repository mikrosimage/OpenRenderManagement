'''
Created on Jan 23, 2012

@author: acs
'''
from puliclient.jobs import CommandRunner
import sys
sys.path.append("/s/apps/common/python")
sys.path.append("\\\\exastore2\\Applis\\common\\python")
import mikrosEnv
mikrosEnv.MikrosEnv().addVersionnedPath(default=True)
import utilities.cleanTempDir.cleanTempDir as cleanLib
import env.templates.mayaVar as mayaVar


class MayaRunner(CommandRunner):

    def execute(self, arguments, updateCompletion, updateMessage):
        # TODO
        pass