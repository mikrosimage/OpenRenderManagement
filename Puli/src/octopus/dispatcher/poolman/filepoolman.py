'''
Created on Aug 17, 2009

@author: Olivier Derpierre
'''
import logging
import re
import os

from octopus.dispatcher.poolman import IPoolManager, PoolDescription, ComputerDescription, ComputerNotFoundError, PoolNotFoundError
from octopus.dispatcher import settings

LOGGER = logging.getLogger("dispatcher")
WORKER_DEFINITION_PATTERN = re.compile(r'(?P<hostname>[a-zA-Z0-9-.]+) (?P<port>\d+) (?P<cpucount>\d+) (?P<cpucorecount>\d+) (?P<cpufreq>\d+(?:\.\d+)*) (?P<ramsize>\d+) (?P<properties>.*)$')
WORKER_CAPABILITY_PATTERN = re.compile(r'(?P<name>\w+)="(?P<value>\w+)"(?:,|$)')
POOL_WORKER_DEFINITION_PATTERN = re.compile(r'(?P<hostname>[\w.-]+) +(?P<port>\d+)$')


class ParsingError(Exception):
    '''Base class for parsing errors in the FileBasedPoolManager-related methods.'''

    def __init__(self, filename, lineno):
        super(ParsingError, self).__init__(filename, lineno)
        self.filename = filename
        self.lineno = lineno


class WorkerListFileParsingError(ParsingError):
    '''Raised when the parsing of a worker list file fails.'''


class PoolDefinitionFileParsingError(ParsingError):
    '''Raised when the parsing of a pool definition file fails.'''


class FilePoolManager(IPoolManager):

    def __init__(self):
        if not os.path.exists(settings.FILE_BACKEND_POOL_PATH):
            raise Exception()
        if not os.path.exists(settings.FILE_BACKEND_RN_PATH):
            raise Exception()
        self.defaultPool = PoolDescription(1, u'default', None)
        self.pools = [self.defaultPool]
        self.computers = []

    def getPool(self, poolName):
        for pool in self.pools:
            if pool.name == poolName:
                return pool
        raise PoolNotFoundError(poolName)

    def listPools(self):
        id = 1  # FIXME this should be defined in the file
        for name, computers in self.parsePoolDirectory(settings.FILE_BACKEND_POOL_PATH):
            id += 1
            version = None
            poolDesc = PoolDescription(id, name, version)
            # add this pool to each of its computers' pool list
            for computerDesc in computers:
                computerName = "%(hostname)s" % computerDesc
                port = "%(port)s" % computerDesc
                try:
                    computer = self.getComputer(computerName, port)
                    computer.pools.append(poolDesc)
                except ComputerNotFoundError:
                    continue
            self.pools.append(poolDesc)
        return self.pools

    def getComputer(self, computerName, port):
        for computer in self.computers:
            if computer.name == computerName and int(computer.port) == int(port):
                return computer
        raise ComputerNotFoundError(computerName)

    def listComputers(self):
        id = 0  # FIXME this should be defined in the file
        for desc in self.parseWorkerListFile(settings.FILE_BACKEND_RN_PATH):
            id += 1
            name = desc['hostname']
            version = None
            pools = []
            pools.append(self.defaultPool)
            cpucount = desc['cpucount']
            cpufreq = desc['cpufreq']
            cpucores = desc['cpucorecount']
            ramsize = desc['ramsize']
            port = desc['port']
            properties = desc['properties']
            computerDesc = ComputerDescription(id, name, version, pools, cpucount, cpufreq, cpucores, ramsize, port, properties)
            self.computers.append(computerDesc)
        return self.computers

    def listComputersForPool(self, poolName):
        raise PoolNotFoundError(poolName)

    def setComputerWorkingFlag(self, computer, workingFlag):
        raise ComputerNotFoundError(computer)

    def parseWorkerListFile(self, filename):
        def enumerateWorkers():
            for lineno, line in enumerate(file(filename)):
                import locale
                line = line.strip().decode(locale.getpreferredencoding(), 'replace')
                if not line or line[0] == '#':
                    continue
                match = WORKER_DEFINITION_PATTERN.match(line)
                if match is None:
                    raise WorkerListFileParsingError(filename, lineno)
                workerDef = match.groupdict()
                workerDef['port'] = int(workerDef['port'])
                workerDef['cpucount'] = int(workerDef['cpucount'])
                workerDef['cpucorecount'] = int(workerDef['cpucorecount'])
                workerDef['cpufreq'] = float(workerDef['cpufreq'])
                workerDef['ramsize'] = int(workerDef['ramsize'])
                workerDef['properties'] = str(workerDef['properties'])
                yield workerDef
        return list(enumerateWorkers())

    def parsePoolFile(self, filename):
        def enumerateWorkers():
            for lineno, line in enumerate(file(filename)):
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                match = POOL_WORKER_DEFINITION_PATTERN.match(line)
                if match is None:
                    raise PoolDefinitionFileParsingError(filename, lineno)
                workerDef = match.groupdict()
                workerDef['port'] = int(workerDef['port'])
                yield(workerDef)
        return list(enumerateWorkers())

    def parsePoolDirectory(self, pooldirname):
        def enumeratePools():
            import glob
            dirname = os.path.abspath(pooldirname)
            globpattern = os.path.join(dirname, "*.pool")
            for pool in glob.glob(globpattern):
                try:
                    poolname = os.path.basename(pool)[:-len(".pool")]
                    if not poolname:
                        continue
                    if poolname == "default":
                        LOGGER.error('Pool name "default" is reserved for internal dispatcher use.')
                        continue
                    yield(poolname, self.parsePoolFile(pool))
                except ParsingError:
                    LOGGER.exception("Parsing of pool file '%s' failed.", pool)
        return enumeratePools()
