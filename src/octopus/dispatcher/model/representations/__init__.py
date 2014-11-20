def TicketRepresentation(ticket):
    ticketRepr = {
        'id': ticket.id,  # uuid.uuid4() generated
        'status': ticket.status,  # ACCEPTED, DONE, FAILED
        'message': ticket.message,
        'resultURL': ticket.resultURL,  # None or url to the modified resource
    }
    return ticketRepr


def ShortRenderNodeRepresentation(renderNode):
    return renderNode.name


def PoolShareRepresentation(poolShare):
    return {
        'id': poolShare.id,
        'poolId': poolShare.pool.id,
        'nodeId': poolShare.node.id,
        'allocatedRN': poolShare.allocatedRN,
        'maxRN': poolShare.maxRN,
    }


def PoolRepresentation(pool):
    return {
        'name': pool.name,
        'renderNodes': [ShortRenderNodeRepresentation(worker) for worker in pool.renderNodes],
        'poolShares': [PoolShareRepresentation(poolShare) for poolShare in pool.poolShares.values()],
    }

TASK_ID = "taskId"
TASK_NAME = "taskName"
TASK_USER = "user"
TASK_PRIORITY = "priority"
TASK_DISPATCH_KEY = "dispatchKey"
TASK_COMMANDS = "commands"
TASK_JOBTYPE = "runner"
TASK_ARGUMENTS = "arguments"
TASK_REQUIREMENTS = "requirements"
TASK_ENVIRONMENT = "environment"


def TaskRepresentation(task):
    return {
        TASK_ID: task.id,
        TASK_NAME: task.name,
        TASK_USER: task.user,
        TASK_PRIORITY: task.priority,
        TASK_DISPATCH_KEY: task.dispatchKey,
        TASK_COMMANDS: [command.id for command in task.commands],
        TASK_JOBTYPE: task.runner,
        TASK_ARGUMENTS: task.arguments.copy(),
        TASK_REQUIREMENTS: task.requirements.copy(),
        TASK_ENVIRONMENT: task.environment.copy(),
    }
