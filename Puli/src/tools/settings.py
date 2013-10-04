#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
settings.py: Config des utilitaires pul_* permettant la consultation et l'edition des jobs en batch
"""
__author__		= "Jérôme Samson"
__copyright__	= "Copyright 2013, Mikros Image"


class Settings(object):
	# Global tools attributes
	verbose=False

	# Initial server/port config
	hostname="localhost"
	port="8004"

	# Default formating & enums
	date_format = '%m/%d %H:%M'
	status_short_name = ("B", "I", "R", "D", "E", "C", "P")
	status_long_name = ("Blocked", "Ready", "Running", "Done", "Error", "Canceled", "Paused")
