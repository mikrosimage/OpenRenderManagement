#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__      = "Jerome Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import sys
import os
import time

import requests

from tornado.web import HTTPError

class RequestTimeoutError ( Exception ):
    ''' Raised when helper execution is too long. '''

class RequestError( Exception ):
	''''''

class InvalidQueryError(Exception):
    '''Raised to manually end a command execution.'''

class ServerRequest(object):
	__host = "puliserver"
	__port = 8004
	
	__baseUrl = "%s:%d" % (__host, __port)
	__query = ""

	def __init__():
		'''
		'''

    # r=requests.delete(url, data=body)
    # if r.status_code in [200,202]: 
    #     logger.info("License released successfully, response = %s" % r.text)
    #     res = True
    #     break
    # else:
    #     logger.error("Error releasing license, response = %s" % r.text)
    #     res = False


class RenderNode( object ):
	'''
	'''

	def __init__( self ):
		pass


class RenderNodeHandler( object ):
	'''
	'''

	def __init__( self ):
		pass

	def renderNode( self, name ):
		'''
		'''
		rn = None

		try:

			rn = requests.get(url,body)

		except HTTPError,e:
			logging.getLogger().error("oops HTTPError: %s" % e)

		except RequestError,e:
			logging.getLogger().error("oops %s" % e)

		except Exception,e:
			logging.getLogger().error("WTF ?? %s" % e)

		return rn
		pass