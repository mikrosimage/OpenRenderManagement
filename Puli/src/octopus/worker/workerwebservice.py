import os
from Queue import Queue
try:
    import simplejson as json
except ImportError:
    import json
import logging
import subprocess

from octopus.core.communication.http import Http400, Http404
from octopus.worker import settings

from octopus.worker.worker import WorkerInternalException

from tornado.web import Application, RequestHandler

# /commands/ [GET] { commands: [ { id, status, completion } ] }
# /commands/ [POST] { id, jobtype, arguments }
# /commands/{id}/ [GET] { id, status, completion, jobtype, arguments }
# /commands/{id}/ [DELETE] stops the job
# /online/ [GET] { online }
# /online/ [SET] { online }
# /status/ [GET] { status, ncommands, globalcompletion }

LOGGER = logging.getLogger("workerws")


class WorkerWebService(Application):
    '''A tornado application that will communicate with the dispatcher via webservices
    Services are:
    /commands
    /commands/<id command>
    /log
    /log/command/<path>
    /updatesysinfos
    /pause
    /ramInUse
    /reconfig
    '''
    def __init__(self, framework, port):
        super(WorkerWebService, self).__init__([
            (r'/commands/?$', CommandsResource, dict(framework=framework)),
            (r'/commands/(?P<id>\d+)/?$', CommandResource, dict(framework=framework)),
            (r'/debug/?$', DebugResource, dict(framework=framework)),
            (r'/log/?$', WorkerLogResource),
            (r'/log/command/(?P<path>\S+)', CommandLogResource),
            (r'/updatesysinfos/?$', UpdateSysResource, dict(framework=framework)),
            (r'/pause/?$', PauseResource, dict(framework=framework)),
            (r'/ramInUse/?$', RamInUseResource, dict(framework=framework)),
            (r'/reconfig/?$', WorkerReconfig, dict(framework=framework))
        ])
        self.queue = Queue()
        self.listen(port, "0.0.0.0")
        self.framework = framework
        self.port = port




class BaseResource(RequestHandler):
    def initialize(self, framework):
        self.framework = framework
        self.rnId = None

    def setRnId(self, request):
        if self.rnId == None and "rnId" in request.headers:
            self.rnId = request.headers['rnId']

    def getBodyAsJSON(self):
        try:
            return json.loads(self.request.body)
        except:
            return Http400("The HTTP body is not a valid JSON object")


class PauseResource(BaseResource):
    def post(self):
        self.setRnId(self.request)

        try:
            data = self.getBodyAsJSON()

            content = data["content"]
            killfile = settings.KILLFILE
            if os.path.isfile(killfile):
                os.remove(killfile)
            # if 0, unpause the worker
            if content != "0":
                if not os.path.isdir(os.path.dirname(killfile)):
                    os.makedirs(os.path.dirname(killfile))
                f = open(killfile, 'w')
                # if -1, kill all current rendering processes
                # if -2, schedule the worker for a restart
                # if -3, kill all and schedule for restart
                if content in ["-1", "-2", "-3"]:
                    f.write(content)
                f.close()
                os.chmod(killfile, 0666)
        except Exception,e:
            LOGGER.error("Error when pausing RN (%r)" % e)
            self.set_status(500)
        else:
            self.set_status(202)


class RamInUseResource(BaseResource):
    """
    TO FIX: the method for retrieving mem used is not really correct. 
    We should use "free -m" or directly /proc/meminfo -> use = memtotal - (memfree + membuffer + memcache)

    Par ex, pour calculer la memoire libre (en prenant en compte les buffers et le swap): 
    awk '/MemFree|Buffers|^Cached/ {free+=$2} END {print  free}' /proc/meminfo

    Pour avoir la memoire utilisee, soit memtotal-memlibre:
    awk '/MemTotal/ {tot=$2} /MemFree|Buffers|^Cached/ {free+=$2} END {print tot-free}' /proc/meminfo
    """
    def get(self):
        process = subprocess.Popen("ps -e -o rss | awk '{sum+=$1} END {print sum/1024}'",
                                   shell=True,
                                   stdout=subprocess.PIPE)
        stdout_list = process.communicate()[0].split('\n')
        self.write(stdout_list[0])


class CommandsResource(BaseResource):
    def get(self):
        '''Lists the commands running on this worker.'''
        commands = [{
            'id': command.id,
            'status': command.status,
            'completion': command.completion,
            'message': command.message,
        } for command in self.framework.application.commands.values()]
        self.write({'commands': commands})

    def post(self):
        # @todo this setRnId call may be just in doOnline necessary
        self.setRnId(self.request)
        data = self.getBodyAsJSON()
        dct = {}
        for key, value in data.items():
            dct[str(key)] = value
        dct['commandId'] = int(dct['id'])
        del dct['id']

        try:
            # self.framework.addOrder(self.framework.application.addCommandApply, **dct)
            ret = self.framework.application.addCommandApply( None,
                    dct['commandId'], 
                    dct['runner'], 
                    dct['arguments'],
                    dct['validationExpression'],
                    dct['taskName'],
                    dct['relativePathToLogDir'],
                    dct['environment']
                )
        except WorkerInternalException, e:
            LOGGER.error("Impossible to add command %r, the RN status is 'paused' (%r)" % (dct['commandId'],e) )
            self.set_status(500)
        except Exception, e:
            LOGGER.error("Impossible to add command %r (%r)" % (dct['commandId'],e) )
            self.set_status(500)
        else:
            self.set_status(202)



class CommandResource(BaseResource):
    def put(self, id):
        """
        | Usually called from a commandwatcher to set new values relative to a command.
        | Only called when a value has changed or and long delay has been reached (see commandwatcher).
        |
        | URL: PUT http://host:port/commands/<id>
        |
        | Several kind of updates are handled:
        | - validation: validation process when a command starts (to be defined, might not be necessary nor used)
        | - update info of the command:
        |     - status: integer indicating the command status
        |     - completion: a float indicating command progress
        |     - message: information string (only used for display but not in pulback)
        |     - stats: a custom dict to report useful stats from the command
        """
        #TODO check error and set error response
        self.setRnId(self.request)
        rawArgs = self.getBodyAsJSON()

        if 'status' in rawArgs \
            or 'completion' in rawArgs \
            or 'message' in rawArgs \
            or 'stats' in rawArgs:
            args = {
                'commandId': int(id),
                'status': rawArgs.get('status', None),
                'message': rawArgs.get('message', None),
                'completion': rawArgs.get('completion', None),
                'stats': rawArgs.get('stats', None)
            }
            self.framework.addOrder(self.framework.application.updateCommandApply, **args)

        elif 'validatorMessage' in rawArgs or 'errorInfos' in rawArgs:
            # validator message case
            args = {
                'commandId': int(id),
                'validatorMessage': rawArgs.get('validatorMessage', None),
                'errorInfos': rawArgs.get('errorInfos', None)
            }
            self.framework.addOrder(self.framework.application.updateCommandValidationApply, **args)

        # Success
        self.set_status(202)

    def delete(self, id):
        #TODO check error and set error response
        dct = {'commandId': int(id)}
        self.framework.addOrder(self.framework.application.stopCommandApply, **dct)
        self.set_status(202)


class DebugResource(BaseResource):
    def get(self):
        watchers = self.framework.application.commandWatchers.values()
        content = [{'id': watcher.command.id} for watcher in watchers]
        self.write(content)


class WorkerLogResource(RequestHandler):
    def get(self):
        logFileName = "worker%d.log" % settings.PORT
        logFilePath = os.path.join(settings.LOGDIR, logFileName)
        if os.path.isfile(logFilePath):
            logFile = open(logFilePath, 'r')
            logFileContent = logFile.read()
            logFile.close()
            self.set_header('Content-Type', 'text/plain')
            self.write(logFileContent)
        return Http404('no log file')


class CommandLogResource(RequestHandler):
    def get(self, path):
        logFilePath = os.path.join(settings.LOGDIR, path)
        if os.path.isfile(logFilePath):
            logFile = open(logFilePath, 'r')
            logFileContent = logFile.read()
            logFile.close()
            self.set_header('Content-Type', 'text/plain')
            self.write(logFileContent)
        return Http404('no log file')


class UpdateSysResource(BaseResource):
    def get(self):
        args = {}
        self.framework.addOrder(self.framework.application.updateSysInfos, **args)


class WorkerReconfig(BaseResource):
    def post(self):
        #TODO check error and set error response
        self.framework.application.reloadConfig()


        