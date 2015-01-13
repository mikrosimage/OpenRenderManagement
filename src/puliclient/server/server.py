#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import logging
try:
    import simplejson as json
except ImportError:
    import json

import requests


class RequestTimeoutError(Exception):
    """ Raised when helper execution is too long. """


class RequestError(Exception):
    """"""


def request(host, port, url, method="get", *args, **kwargs):
    """
    | General wrapper around the "Request" methods
    | Used by Server object when sending request to the main server, can also
    | be used by any worker/specific requests.

    :param host: hostname to reach
    :param port: port to use
    :param url: end part of the url to reach
    :param method: a string indicating wich method to use [get,put,post,delete]

    :return: a json or text data depending of the webservice response
    :raise RequestError: for any error that occured related to the network
    :raise RequestTimeoutError: when a request timeout occur
    """
    try:
        baseUrl = "http://%s:%d" % (host, port)
        url = '/'.join([baseUrl, url])

        if method == "get":
            r = requests.get(url, *args, **kwargs)
        elif method == "post":
            r = requests.post(url, *args, **kwargs)
        elif method == "put":
            r = requests.put(url, *args, **kwargs)
        elif method == "delete":
            r = requests.delete(url, *args, **kwargs)
        else:
            logging.error("Unkown HTTP method called: %s" % method)
            raise RequestError

        if r.status_code in [requests.codes.ok,
                             requests.codes.created,
                             requests.codes.accepted]:
            #
            # Request returned successfully
            #
            try:
                result = r.json()
            except ValueError, e:
                result = r.text
            return result

        elif r.status_code in [requests.codes.bad,
                               requests.codes.unauthorized,
                               requests.codes.forbidden,
                               requests.codes.not_found,
                               requests.codes.not_allowed,
                               requests.codes.not_acceptable,
                               requests.codes.internal_server_error,
                               requests.codes.not_implemented,
                               requests.codes.unavailable,
                               requests.codes.conflict]:
            try:
                msg = r.text
            except:
                msg = ""

            logging.error("Error return code: %s, response message: '%s'" % (
                r.status_code, msg))
            raise RequestError(msg)
        else:
            raise RequestError

    except requests.exceptions.Timeout:
        logging.error("Timeout: %s" % e)
        raise RequestTimeoutError

    except requests.exceptions.ConnectionError, e:
        logging.error("Network problem occured: the host you're trying to reach is probably down (%s)" % baseUrl)
        # logging.error("Network problem occured: %s" % e.args[0].reason)
        raise RequestError

    except requests.exceptions.RequestException, e:
        logging.error("Unhandled request exception: %s" % e)
        raise RequestError

    except RequestError:
        raise

    except Exception, e:
        logging.error("Unhandled exception: %s" % e)
        raise


class Server(object):
    __host = "vfxpc64"
    __port = 8004

    __baseUrl = "http://%s:%d" % (__host, __port)
    __query = ""

    @classmethod
    def getBaseUrl(cls):
        return cls.__baseUrl

    @classmethod
    def request(cls, url, method, *args, **kwargs):
        return request(cls.__host, cls.__port, url, method, *args, **kwargs)

    @classmethod
    def get(cls, url, *args, **kwargs):
        return cls.request(url, "get", *args, **kwargs)

    @classmethod
    def post(cls, url, *args, **kwargs):
        return cls.request(url, "post", *args, **kwargs)

    @classmethod
    def put(cls, url, *args, **kwargs):
        return cls.request(url, "put", *args, **kwargs)

    @classmethod
    def delete(cls, url, *args, **kwargs):
        return cls.request(url, "delete", *args, **kwargs)

