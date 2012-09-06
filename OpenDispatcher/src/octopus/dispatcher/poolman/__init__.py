'''
Created on Aug 17, 2009

@author: Olivier Derpierre
'''

import socket


class PoolManagerError(Exception):
    '''Base class for errors raised by a PoolManager.'''


class PoolNotFoundError(PoolManagerError):
    '''Raised when a method tries to access a pool that does not exist.'''

    def __init__(self, poolName):
        PoolManagerError.__init__(self, poolName)
        self.poolName = poolName


class ComputerNotFoundError(PoolManagerError):
    '''Raised when a method tries to access a computer that does not exist.'''

    def __init__(self, computerName):
        PoolManagerError.__init__(self, computerName)
        self.computerName = computerName


class PoolDescription():

    def __init__(self, id, name, version):
        '''
        Initializes a PoolDescription object.

        @param name the name of the pool
        '''
        self.id = id
        self.name = name
        self.version = version
#        self.computers = dict(computers)


class ComputerDescription(object):

    _name = None

    def getName(self):
        return self._name

    def setName(self, name):
        self._name = socket.getfqdn(name)

    name = property(getName, setName)

    def __init__(self, id, name, version, pools, cpucount, cpufreq, cpucores, ramsize, port, properties):
        '''
        Initializes a  ComputerDescription object.

        @param name the name of the computer
        @param pools a list of PoolDescription objects that contain this computer
        '''
        self.id = id
        self.name = name
        self.version = version
        self.pools = list(pools)
        self.cpucount = cpucount
        self.cpufreq = cpufreq
        self.cpucores = cpucores
        self.ramsize = ramsize
        self.port = port
        self.properties = properties


class IPoolManager():
    '''
    Describes the programming interface to implement to access a pool & computer discovery service usable by the HD3D/Dispatcher.
    '''

    def getPool(self, poolName):
        '''Returns a PoolDescription object for pool "poolName".

        Raises a PoolNotFoundError if the pool does not exist.
        '''
        raise PoolNotFoundError(poolName)

    def listPools(self):
        '''Returns a list of PoolDescription objects of all the pools known to this manager.'''
        return []

    def getComputer(self, computerName):
        '''Returns a ComputerDescription object for computer "computerName".

        Raises a ComputerNotFoundError
        '''
        raise ComputerNotFoundError(computerName)

    def listComputers(self):
        '''Returns the list of all ComputerDescription objects.
        '''
        return []

    def listComputersForPool(self, poolName):
        '''Returns a list of ComputerDescription objects of all the computers belonging to pool "poolName"

        Raises a PoolNotFoundError if the pool does not exist.
        '''
        raise PoolNotFoundError(poolName)

    def setComputerWorkingFlag(self, computer, workingFlag):
        '''Sets the working flag for a given computer.

        Raises a ComputerNotFoundError if the given computer does not exist.
        '''
        raise ComputerNotFoundError(computer)
