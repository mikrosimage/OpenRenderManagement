'''
@file webservicedispatcher.py
@package octopus.dispatcher.webservice
@author Arnaud Chassagne, Jean-Baptiste Spieser, Olivier Derpierre
@date 2008/12/01
@version 0.1
'''
try:
    import simplejson as json
except ImportError:
    import json

import os
import time
import logging
from datetime import timedelta

import tornado
from tornado.web import Application

from octopus.core.communication.http import Http500
from octopus.dispatcher.webservice import commands, rendernodes, graphs, nodes,\
    tasks, poolshares, pools, licenses, \
    query, edit

from octopus.core.enums.command import *
from octopus.dispatcher.webservice import DispatcherBaseResource
from octopus.core import singletonconfig
from octopus.dispatcher import settings


class WebServiceDispatcher(Application):
    '''This class defines the webservice associated with the dispatcher.'''

    def __init__(self, framework, port):
        super(WebServiceDispatcher, self).__init__([
            (r'/stats/?$', StatsResource, dict(framework=framework)),

            (r'/licenses/?$', licenses.LicensesResource, dict(framework=framework)),
            (r'/licenses/([\w.-]+)/?$', licenses.LicenseResource, dict(framework=framework)),

            (r'/commands/?$', commands.CommandsResource, dict(framework=framework)),
            (r'/commands/(\d+)/?$', commands.CommandResource, dict(framework=framework)),

            (r'/rendernodes/?$', rendernodes.RenderNodesResource, dict(framework=framework)),
            (r'/rendernodes/performance/?$', rendernodes.RenderNodesPerfResource, dict(framework=framework)),
            (r'/rendernodes/quarantine/?$', rendernodes.RenderNodeQuarantineResource, dict(framework=framework)),
            (r'/rendernodes/((?:\d+)|(?:[\w.-]+:\d+))/?$', rendernodes.RenderNodeResource, dict(framework=framework)),
            (r'/rendernodes/((?:\d+)|(?:[\w.-]+:\d+))/commands/(\d+)/?$', rendernodes.RenderNodeCommandsResource, dict(framework=framework)),
            (r'/rendernodes/((?:\d+)|(?:[\w.-]+:\d+))/sysinfos/?$', rendernodes.RenderNodeSysInfosResource, dict(framework=framework)),
            (r'/rendernodes/((?:\d+)|(?:[\w.-]+:\d+))/paused/?$', rendernodes.RenderNodePausedResource, dict(framework=framework)),
            (r'/rendernodes/((?:\d+)|(?:[\w.-]+:\d+))/reset/?$', rendernodes.RenderNodeResetResource, dict(framework=framework)),

            (r'^/graphs/?$', graphs.GraphesResource, dict(framework=framework)),

            (r'^/nodes/?$', nodes.NodesResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/?$', nodes.NodeResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/name/?$', nodes.NodeNameResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/status/?$', nodes.NodeStatusResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/cancel/?$', nodes.NodeCancelResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/paused/?$', nodes.NodePausedResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/pauseKill/?$', nodes.NodePauseKillResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/priority/?$', nodes.NodePriorityResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/dispatchKey/?$', nodes.NodeDispatchKeyResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/maxRN/?$', nodes.NodeMaxRNResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/strategy/?$', nodes.NodeStrategyResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/children/?$', nodes.NodeChildrenResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/user/?$', nodes.NodeUserResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/prod/?$', nodes.NodeProdResource, dict(framework=framework)),
            (r'^/nodes/(\d+)/maxAttempt/?$', nodes.NodeMaxAttemptResource, dict(framework=framework)),

            (r'^/tasks/?$', tasks.TasksResource, dict(framework=framework)),
            (r'^/tasks/delete/?$', tasks.DeleteTasksResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/?$', tasks.TaskResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/env/?$', tasks.TaskEnvResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/arguments/?$', tasks.TaskArgumentResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/commands/?$', tasks.TaskCommandResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/tree/?$', tasks.TaskTreeResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/comment/?$', tasks.TaskCommentResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/user/?$', tasks.TaskUserResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/ram/?$', tasks.TaskRamResource, dict(framework=framework)),
            (r'^/tasks/(\d+)/timer/?$', tasks.TaskTimerResource, dict(framework=framework)),

            (r'^/poolshares/?$', poolshares.PoolSharesResource, dict(framework=framework)),
            (r'^/poolshares/(\d+)/?$', poolshares.PoolShareResource, dict(framework=framework)),

            (r'^/pools/?$', pools.PoolsResource, dict(framework=framework)),
            (r'^/pools/([\w.-]+)/?$', pools.PoolResource, dict(framework=framework)),
            (r'^/pools/([\w.-]+)/rendernodes/?$', pools.PoolRenderNodesResource, dict(framework=framework)),

            (r'^/system/?$', SystemResource, dict(framework=framework)),
            (r'^/mobile/?$', MobileResource, dict(framework=framework)),

            # Several WS to get or edit multiple data in a single request.
            # It uses the query mecanism defined in octopus.dispatcher.model.nodequery module
            (r'^/query$', query.QueryResource, dict(framework=framework)),
            (r'^/edit/status$', edit.EditStatusResource, dict(framework=framework)),
            (r'^/edit/maxrn$', edit.EditMaxRnResource, dict(framework=framework)),
            (r'^/edit/prio$', edit.EditPrioResource, dict(framework=framework)),
            (r'^/pause$', edit.PauseResource, dict(framework=framework)),
            (r'^/resume$', edit.ResumeResource, dict(framework=framework)),

            (r'^/query/rn$', query.RenderNodeQueryResource, dict(framework=framework)),
            (r'^/edit/rn$', edit.RenderNodeEditResource, dict(framework=framework)),

            (r'^/query/command$', commands.CommandQueryResource, dict(framework=framework)),

            (r'^/reconfig$', ReconfigResource, dict(framework=framework)),
            (r'^/restart$', RestartResource, dict(framework=framework)),
            (r'^/shutdown$', ShutdownResource, dict(framework=framework)),

            # Standard WS to retrieve log file from server
            (r"/log/(.*)", tornado.web.StaticFileHandler, {"path": settings.LOGDIR, "default_filename": "dispatcher.log"}),
        ])

        self.listen(port, "0.0.0.0")
        self.framework = framework


class StatsResource(DispatcherBaseResource):
    def get(self):
        from octopus.core.enums.rendernode import RN_UNKNOWN, RN_STATUS_NAMES
        from octopus.core.enums.node import NODE_STATUS_NAMES

        tree = self.getDispatchTree()

        #
        # Get info on commands
        #
        commandsByStatus = {}
        for name in CMD_STATUS_NAME:
            commandsByStatus[name] = 0
        for command in tree.commands.values():
            status = CMD_STATUS_NAME[command.status]
            commandsByStatus[status] += 1

        commandsByStatus['TOTAL'] = len(tree.commands)

        #
        # Get info rendernodes
        #
        renderNodeStats = {'totalCores': 0, 'idleCores': 0, 'missingRenderNodes': 0}
        renderNodeByStatus = dict(((status, 0) for status in RN_STATUS_NAMES))

        for node in tree.renderNodes.values():
            if node.status != RN_UNKNOWN:
                renderNodeStats['totalCores'] += node.coresNumber
                renderNodeStats['idleCores'] += node.freeCoresNumber
            else:
                renderNodeStats['missingRenderNodes'] += 1
            renderNodeByStatus[RN_STATUS_NAMES[node.status]] += 1
        renderNodeStats['renderNodesByStatus'] = renderNodeByStatus

        #
        # Get info on jobs (first level of hierarchy)
        #
        jobsByStatus = dict(((status, 0) for status in NODE_STATUS_NAMES))
        for node in tree.nodes[1].children:
            jobsByStatus[NODE_STATUS_NAMES[node.status]] += 1
        jobsByStatus['TOTAL'] = len(tree.nodes[1].children)

        #
        # Final recap
        #
        stats = {
            'date': time.time(),
            'commands': commandsByStatus,
            'rendernodes': renderNodeStats,
            'jobs': jobsByStatus,
            'licenses': repr(self.dispatcher.licenseManager),
            'licensesDict': self.dispatcher.licenseManager.stats()
        }
        self.writeCallback(stats)


class MobileResource(DispatcherBaseResource):
    def get(self):
        from octopus.core.enums.rendernode import RN_STATUS_NAMES
        html = "<meta name = \"viewport\" content = \"width = device-width\">\n<meta name = \"viewport\" content = \"width = 320\">"
        tree = self.getDispatchTree()
        commandsByStatus = {}
        for name in CMD_STATUS_NAME:
            commandsByStatus[name] = 0
        for command in tree.commands.values():
            status = CMD_STATUS_NAME[command.status]
            commandsByStatus[status] += 1
        del commandsByStatus["FINISHING"]
        commandsByStatus['TOTAL'] = len(tree.commands)

        colors = {'BLOCKED': "white",
                  'IDLE': "rgb(190,186,138)",
                  'READY': "rgb(190,186,138)",
                  'ASSIGNED': "white",
                  'WORKING': "green",
                  'RUNNING': "green",
                  'FINISHING': "white",
                  'DONE': "lightgray",
                  'BOOTING': "white",
                  'TIMEOUT': "rgb(217,37,38)",
                  'ERROR': "rgb(217,37,38)",
                  'UNKNOWN': "rgb(217,37,38)",
                  'PAUSED': "rgb(242,195,64)",
                  'CANCELED': "lightgray",
                  'TOTAL': "white"}

        html += "<div><div style=\"width:160px; float:left; display:inline-block;\">"
        html += "<b>Commands Status</b><br><br><table border=1 style=\"border-collapse:collapse;text-align:center;\">"
        for key, value in commandsByStatus.items():
            html += "<tr style=\"background-color:" + colors[key] + "\"><td>" + key + "</td><td>" + str(value) + "</td></tr>"
        html += "</table>"
        html += "</div>"

        html += "<div style=\"margin-left:160px;\">"
        html += "<b>Workers Status</b><br><br><table border=1 style=\"border-collapse:collapse;text-align:center;\">"
        renderNodeByStatus = dict(((status, 0) for status in RN_STATUS_NAMES))
        renderNodeByStatus['TOTAL'] = len(tree.renderNodes.values())
        for node in tree.renderNodes.values():
            renderNodeByStatus[RN_STATUS_NAMES[node.status]] += 1
        for key, value in renderNodeByStatus.items():
            html += "<tr style=\"background-color:" + colors[key.upper()] + "\"><td>" + key.upper() + "</td><td>" + str(value) + "</td></tr>"
        html += "</table>"
        html += "</div></div><br>"

        html += "<div style=\"width:100%;\">"
        html += "<br><b>Licenses</b><br><br><table border=1 style=\"width:100%;border-collapse:collapse;text-align:center;\"><tr>"
        for lic in self.dispatcher.licenseManager.licenses.values():
            html += "<td>" + lic.name + "</td>"
        html += "</tr><tr>"
        for lic in self.dispatcher.licenseManager.licenses.values():
            html += "<td>" + str(lic.used) + " / " + str(lic.maximum) + "</td>"
        html += "</tr></table>"
        html += "</div>"

        self.writeCallback(html)


class SystemResource(DispatcherBaseResource):
    def get(self):

        uptime = 0
        load = (0, 0, 0)
        try:
            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.readline().split()[0])
                uptime = timedelta(seconds=uptime_seconds)
                load = os.getloadavg()
        except Exception, e:
            logging.getLogger('main').warning("Impossible to retrieve server uptime or load average: %s" % e)

        sysinfo = {
            'host': str(os.uname()[1]),
            'port': settings.PORT,
            'user': str(os.getlogin()),

            'version': settings.VERSION,
            'log_dir': settings.LOGDIR,
            'conf_dir': settings.CONFDIR,

            'uptime': str(uptime),
            'load_average_1mn': load[0],
            'load_average_5mn': load[1],
            'load_average_15mn': load[2]
        }
        self.render("template/system.html", title="Puliserver system info", sysinfo=sysinfo, env=sorted(os.environ.items()))


class ReconfigResource(DispatcherBaseResource):
    def post(self):
        '''
        Reload the main config file (using singletonconfig) and reload
        every main loggers.
        '''
        try:
            singletonconfig.reload()
            logLevel = singletonconfig.get('CORE', 'LOG_LEVEL')
            logging.getLogger().setLevel(logLevel)
            logging.getLogger('main').setLevel(logLevel)
            logging.getLogger("worker").setLevel(logLevel)
        except Exception, e:
            raise Http500("Error during server reconfig: %r" % e)

        self.writeCallback("done")


class ShutdownResource(DispatcherBaseResource):
    def post(self):
        '''
        '''
        try:
            result = {'return_code': True}
            self.write(json.dumps(result))
            tornado.ioloop.IOLoop.instance().add_callback(self.framework.application.shutdown)
            tornado.ioloop.IOLoop.instance().add_callback(tornado.ioloop.IOLoop.instance().stop)

        except Exception, e:
            raise Http500("Error during server restart: %s" % e)


class RestartResource(DispatcherBaseResource):
    def post(self):
        '''
        '''
        try:
            self.framework.application.restartService = True

            result = {'return_code': True}
            self.write(json.dumps(result))
            tornado.ioloop.IOLoop.instance().add_callback(self.framework.application.shutdown)
            tornado.ioloop.IOLoop.instance().add_callback(tornado.ioloop.IOLoop.instance().stop)

        except Exception, e:
            raise Http500("Error during server restart: %s" % e)
