NODE_BLOCKED = 0
NODE_READY = 1
NODE_RUNNING = 2
NODE_DONE = 3
NODE_ERROR = 4
NODE_CANCELED = 5
NODE_PAUSED = 6

NODE_STATUS = ( NODE_BLOCKED,
                NODE_READY,
                NODE_RUNNING,
                NODE_DONE,
                NODE_ERROR,
                NODE_CANCELED,
                NODE_PAUSED )
                
NODE_STATUS_NAMES = ( "BLOCKED",
                      "READY",
                      "RUNNING",
                      "DONE",
                      "ERROR",
                      "CANCELED",
                      "PAUSED" )

def isFinalNodeStatus(status):
    return status in [NODE_DONE, NODE_ERROR, NODE_CANCELED]
