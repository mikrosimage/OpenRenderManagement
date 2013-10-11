#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
name: config.py

Module holding param values that might be reloaded without restarting the dispatcher
The dispatcher handles a "reconfig" request wich ask the worker applicaiton to reload this class

Basic usage:
globalconf = SingletonConfig()
globalconf.load( settings.CONFDIR + "/config.ini" )

print "one_value = " + globalconf.conf.get('ONE_SECTION','ONE_FIELD')
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


import ConfigParser


class SingletonConfig(object):
	class __SingletonConfig:
		def __init__(self):
			self.confPath = None
			self.conf = None

		def __str__(self):
			return "SingletonConfig(%r) --> %r" % (self.confPath, self.conf)

		def load( self, pFilePath ):
			self.confPath = pFilePath

			self.conf = ConfigParser.ConfigParser()
			self.conf.read(pFilePath)

		def reload( self ):
			self.conf.read(self.confPath)


	instance = None

	def __new__(c): # _new_ est toujours une méthode de classe
		if not SingletonConfig.instance:
		  SingletonConfig.instance = SingletonConfig.__SingletonConfig()
		return SingletonConfig.instance

	def __getattr__(self, attr):
		return getattr(self.instance, attr)

	def __setattr__(self, attr, val):
		return setattr(self.instance, attr, val)
