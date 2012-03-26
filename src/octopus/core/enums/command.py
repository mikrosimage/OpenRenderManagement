__all__ = ('CMD_STATUS', 'CMD_STATUS_NAME', 'CMD_BLOCKED', 'CMD_READY',
           'CMD_ASSIGNED', 'CMD_RUNNING', 'CMD_FINISHING', 'CMD_DONE',
           'CMD_ERROR', 'CMD_CANCELED', 'CMD_TIMEOUT',
           'isDoneStatus', 'isErrorStatus', 'isFinalStatus', 'isRunningStatus')

CMD_BLOCKED = 0
CMD_READY = 1
CMD_ASSIGNED = 2 
CMD_RUNNING = 3
CMD_FINISHING = 4
CMD_DONE = 5
CMD_TIMEOUT = 6
CMD_ERROR = 7
CMD_CANCELED = 8

CMD_STATUS = ( CMD_BLOCKED, 
               CMD_READY, 
               CMD_ASSIGNED, 
               CMD_RUNNING, 
               CMD_FINISHING, 
               CMD_DONE, 
               CMD_TIMEOUT,
               CMD_ERROR, 
               CMD_CANCELED )

CMD_STATUS_NAME = ( 'BLOCKED', 
                    'READY', 
                    'ASSIGNED', 
                    'RUNNING',
                    'FINISHING', 
                    'DONE', 
                    'TIMEOUT',
                    'ERROR', 
                    'CANCELED' )

def isDoneStatus(status):
    return status == CMD_DONE

def isErrorStatus(status):
    return status in (CMD_ERROR, CMD_CANCELED)

def isFinalStatus(status):
    return status in (CMD_DONE, CMD_ERROR, CMD_CANCELED, CMD_TIMEOUT)

def isRunningStatus(status):
    return status in (CMD_RUNNING, CMD_FINISHING, CMD_ASSIGNED)

