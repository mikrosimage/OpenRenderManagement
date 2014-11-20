__all__ = ('CMD_STATUS', 'CMD_STATUS_NAME', 'CMD_BLOCKED', 'CMD_READY',
           'CMD_ASSIGNED', 'CMD_RUNNING', 'CMD_FINISHING', 'CMD_DONE',
           'CMD_ERROR', 'CMD_CANCELED', 'CMD_TIMEOUT',
           'isFinalStatus', 'isRunningStatus')

CMD_STATUS = (CMD_BLOCKED,
              CMD_READY,
              CMD_ASSIGNED,
              CMD_RUNNING,
              CMD_FINISHING,
              CMD_DONE,
              CMD_TIMEOUT,
              CMD_ERROR,
              CMD_CANCELED) = range(9)

CMD_STATUS_NAME = ('BLOCKED',
                   'READY',
                   'ASSIGNED',
                   'RUNNING',
                   'FINISHING',
                   'DONE',
                   'TIMEOUT',
                   'ERROR',
                   'CANCELED')

CMD_STATUS_SHORT_NAMES = ("B", "I", "A", "R", "F", "D", "T", "E", "C")


def isFinalStatus(status):
    return status in (CMD_DONE, CMD_ERROR, CMD_CANCELED, CMD_TIMEOUT)


def isRunningStatus(status):
    return status in (CMD_RUNNING, CMD_FINISHING, CMD_ASSIGNED)
