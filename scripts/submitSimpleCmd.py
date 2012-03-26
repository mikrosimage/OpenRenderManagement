from puliclient import Task, Graph

if __name__ == '__main__':
    simpleTask = Task(name="simpleTask",
                      arguments={},
                      decomposer="puliclient.contrib.generic.GenericDecomposer")
    
    graph = Graph('simpleGraph', simpleTask)
    
    host, port = ("127.0.0.1", 8004)
    graph.submit(host, port)