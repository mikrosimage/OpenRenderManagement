'''
Created on Oct 7, 2009

@author: Arnaud Chassagne
'''

import socket

from octopus.core.communication.requestmanager import RequestManager
try:
    import simplejson as json
except ImportError:
    import json
from octopus.dispatcher import settings
from octopus.dispatcher.poolman import IPoolManager, PoolDescription, ComputerDescription, ComputerNotFoundError, PoolManagerError, PoolNotFoundError


class WebServicePoolManager(IPoolManager):

    def __init__(self):
        self.requestManager = RequestManager(settings.WS_BACKEND_URL, settings.WS_BACKEND_PORT)
        self.defaultPool = PoolDescription(1, u'default', None)
        self.pools = [self.defaultPool]
        self.computers = []

    def getPool(self, poolName):
        for pool in self.pools:
            if pool.name == poolName:
                return pool
        raise PoolNotFoundError(poolName)

    def listPools(self):
        poolsDict = json.loads(self.requestManager.get("/dev-Hd3dServices/v1/pools"))
        for pool in poolsDict['records']:
            id = int(pool['id'])
            name = str(pool['name'])
            version = str(pool['version'])
            poolDesc = PoolDescription(id, name, version)
            computerIds = pool['computerIDs']
            for computerId in computerIds:
                try:
                    computer = self.getComputerById(int(computerId))
                    computer.pools.append(poolDesc)
                except ComputerNotFoundError:
                    continue
            self.pools.append(poolDesc)
        return self.pools

    def getComputer(self, computerName):
        computerName = socket.getfqdn(computerName)
        for computer in self.computers:
            if computer.name == computerName:
                return computer
        raise ComputerNotFoundError(computerName)

    def getComputerById(self, id):
        for computer in self.computers:
            if computer.id == id:
                return computer
        raise ComputerNotFoundError(id)

    def listComputers(self):
        computersDict = json.loads(self.requestManager.get("/dev-Hd3dServices/v1/computers"))
        for computer in computersDict['records']:
            id = int(computer['id'])
            name = str(computer['dnsName'])
            version = str(computer['version'])
            pools = []
            pools.append(self.defaultPool)
            cpucount = computer['numberOfProcs']
            cpufreq = computer['procFrequency']
            cpucores = computer['numberOfCores']
            ramsize = computer['ramQuantity']
            port = 8000
            properties = {}
            computerDesc = ComputerDescription(id, name, version, pools, cpucount, cpufreq, cpucores, ramsize, port, properties)
            self.computers.append(computerDesc)
        return self.computers

    def listComputersForPool(self, poolName):
        raise PoolNotFoundError(poolName)

    def setComputerWorkingFlag(self, computer, workingFlag):
        raise ComputerNotFoundError(computer)
