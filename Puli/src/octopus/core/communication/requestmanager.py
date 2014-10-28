import socket
import httplib as http
import time
import logging

# def connect(self):
#     """Connect to the host and port specified in __init__."""
#     msg = "getaddrinfo returns an empty list"
#     for res in socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM):
#         af, socktype, proto, canonname, sa = res
#         try:
#             self.sock = socket.socket(af, socktype, proto)
#             self.sock.settimeout(getattr(self, "timeout", None))
#             if self.debuglevel > 0:
#                 print "connect: (%s, %s)" % (self.host, self.port)
#             self.sock.connect(sa)
#         except socket.error, msg:
#             if self.debuglevel > 0:
#                 print 'connect fail:', (self.host, self.port)
#             if self.sock:
#                 self.sock.close()
#             self.sock = None
#             continue
#         break
#     if not self.sock:
#         raise socket.error, msg
#     self.sock.settimeout(None)


class RequestManager(object):

    class RequestError(Exception):
        def __init__(self, message, status=0):
            super(RequestManager.RequestError, self).__init__(message)
            self.status = status

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.requestId = 1

    def request(self, method, path, data=None, headers=None):
        logger = logging.getLogger('main')

        if not headers:
            headers = {}
        headers["requestId"] = str(self.requestId)
        self.requestId += 1
        maxi = 10
        while maxi:
            try:
                conn = http.HTTPConnection(self.host, self.port)
                conn.timeout = 2.0
                if not headers:
                    conn.request(method, path, data)
                else:
                    conn.request(method, path, data, headers)
                r = conn.getresponse()

                if r.status not in (200, 201, 202, 303):
                    raise self.RequestError(r.reason + " for request %s %s" % (method, path), r.status)
                rdata = r.read()
                conn.close()
                break
            except socket.error, e:
                logger.error("Warning: a socket error occured in requestmanager '%s %s'" % (method, path))
                try:
                    conn.close()
                except:
                    pass
                try:
                    errno, msg = e
                except ValueError:
                    errno = 0
                logger.error("Error: %r (errno=%r)", e, errno)

                if errno == 111 or errno == 0:
                    return "ERROR"

                # When socket.error occur it is usally a bad adress famaily when the server is done. In this case we simply return
                # an "ERROR" message without retrying. retry will occur if the caller needs to.
                maxi -= 1
                time.sleep(.1)

            except http.BadStatusLine, e:
                logger.error("Warning: unrecognized HTTP code was returned '%s %s' (reties lieft:%d)" % (method, path, maxi))
                try:
                    conn.close()
                except:
                    pass
                # dispatcher server is down ?
                # let's retry
                maxi -= 1
                time.sleep(.1)
                if maxi == 1:
                    # enough retry
                    print("Maximum number of retries reached in request manager (%s - %s)" % (method, path))
                    raise

        else:
            rdata = "ERROR"
        return rdata

    def get(self, path, data=None, headers={}):
        return self.request("GET", path, data, headers=headers)

    def post(self, path, data, headers={}):
        return self.request("POST", path, data, headers)

    def put(self, path, data, headers={}):
        return self.request("PUT", path, data, headers=headers)

    def delete(self, path, data, headers={}):
        return self.request("DELETE", path, data, headers=headers)
