NODE_STATUS = (NODE_BLOCKED,
                NODE_READY,
                NODE_RUNNING,
                NODE_DONE,
                NODE_ERROR,
                NODE_CANCELED,
                NODE_PAUSED) = range(7)

NODE_STATUS_NAMES = ("BLOCKED",
                      "READY",
                      "RUNNING",
                      "DONE",
                      "ERROR",
                      "CANCELED",
                      "PAUSED")

NODE_STATUS_SHORT_NAMES = ("B", "I", "R", "D", "E", "C", "P")


def isFinalNodeStatus(status):
    return status in [NODE_DONE, NODE_ERROR, NODE_CANCELED]
