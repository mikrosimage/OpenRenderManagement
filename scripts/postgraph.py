import simplejson as json
import httplib

if __name__ == '__main__':
    
    graph = {
        'name': 'test graph',
        'tasks': [{
            'name': 'test task',
            'type': 'PrintInFile',
            'arguments': { 'text': 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.' },
            'dependencies': [], 
            'subtasks': [],
            'maxrn': 1,
            'validator': '',
        }]
    }
    
    graph = json.dumps(graph)
    
    conn = httplib.HTTPConnection("localhost", 8004)
    conn.request('POST', '/graphs/', graph, {'Content-Length': len(graph)})
    response = conn.getresponse()
    print response.status, response.reason
    print "\n".join("%s: %s" % (header, value) for header, value in response.getheaders())
    print response.read()
