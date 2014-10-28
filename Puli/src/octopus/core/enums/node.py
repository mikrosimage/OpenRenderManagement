NODE_STATUS = (NODE_BLOCKED,
               NODE_READY,
               NODE_RUNNING,
               NODE_DONE,
               NODE_ERROR,
               NODE_CANCELED,
               NODE_PAUSED) = range(7)

# TODO dependencies should be set for restricted node statutes only: DONE, ERROR and CANCELED

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
