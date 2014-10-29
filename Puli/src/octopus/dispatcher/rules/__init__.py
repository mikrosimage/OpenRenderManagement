import re

from octopus.dispatcher.model import Task, TaskNode, FolderNode
from octopus.dispatcher.strategies import loadStrategyClass


class RuleError(Exception):
    pass


class GraphExistsError(RuleError):
    pass

#class FolderNodeTemplate(object):
#
#    parameterPattern = re.compile(r"{graph.(\w*?)}")
#
#    def __init__(self, template):
#        self.template = template
#        self.parameters = self._extractParameters()
#
#    def _extractParameters(self):
#        return tuple(set(parameterPattern.findall(self.template)))
#
#    def build(self, arguments, parent, priority, dispatchKey, maxRN, strategy):
#        # check that no argument is missing
#        #
#        for param in self.parameters:
#            if param not in arguments:
#                raise TypeError, "missing parameter %s" % param
#        # build the node name
#        #
#        name = self.template
#        for param in self.parameters:
#            name = name.replace("{graph."+param+"}", arguments[param])
#        # build and return the node
#        #
#        return FolderNode(None, name, parent, priority, dispatchKey, maxRN, strategy)
#
#class GraphRule(object):
#
#    argPattern = re.compile(r"{graph.(\w*?)}")
#
#    def __init__(self, root, templates, filters):
#        self.root = root
#        self.templates = templates
#        self.filters = filters
#
#    def _parsePatterns(self):
#        for template in self.templates:
#            nodeBuilders = []
#            nodeTemplates = template.split("/")
#            for nodeTemplate in nodeTemplates:
#                nodeParameters = argPattern.findall(nodeTemplate)
#
