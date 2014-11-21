#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
name: config.py

Module holding param values that might be reloaded without restarting the dispatcher
The dispatcher handles a "reconfig" request wich ask the worker application to reload the source config file and recreate the conf object

Basic usage:

    import singletonconfig
    singletonconfig.load( settings.CONFDIR + "/config.ini" )

    # Access the options with module's get() method
    print "one_value = " + singletonconfig.get('ONE_SECTION','ONE_FIELD')

    # Or with direct access to the key/value dict for faster access
    print "one_value = " + singletonconfig.conf.['ONE_SECTION']['ONE_FIELD']

"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2013, Mikros Image"


import ConfigParser
import ast

confPath = None
confWithString = None
conf = {}


def get(section, option, default=None):
    """
    Simple accessor to hide/protect the multiple depth dict access: conf["SECTION"]["OPTION"]
    """
    if section in conf.keys():
        if option in conf[section].keys():
            return conf[section][option]
    return default


def parse():
    """
    Load a conf object with evaluated options intead of strings (as returned by the ConfigParser)
    We use the "ast" lib to eval the attributes more safely than with classic python eval()
    """
    global confWithString, conf

    if confWithString is None:
        return

    conf = {}
    for section in confWithString.sections():
        conf[section] = {}
        for item in confWithString.items(section):
            optName, optValue = item
            optValue = ast.literal_eval(optValue)

            conf[section][optName.upper()] = optValue


def load(pFilePath):
    """
    Load ini file for later use in application
    """
    global confPath, confWithString

    confPath = pFilePath

    confWithString = ConfigParser.ConfigParser()
    confWithString.read(pFilePath)

    # parse pour creer cleanconf
    parse()


def reload():
    """
    Reload conf file for later use in application
    """
    global confPath, confWithString

    confWithString.read(confPath)
    parse()
