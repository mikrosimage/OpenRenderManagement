#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
settings.py: Config des utilitaires pul_* permettant la consultation et l'edition des jobs en batch
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2013, Mikros Image"


class Settings(object):
    # Global tools attributes
    verbose = False

    # Initial server/port config
    hostname = "puliserver"
    port = "8004"

    # Default formating & enums
    date_format = '%m/%d %H:%M'
    time_format = '%H:%M'
    precise_date_format = '%m/%d %H:%M:%S'
    precise_time_format = '%H:%M:%S'
