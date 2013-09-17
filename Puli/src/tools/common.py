#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
commons.py: Utility classes and static methods used in every tools
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


from settings import Settings

import urllib

VERBOSE=Settings.verbose

class ConstraintFactory:
    """
    Can parse arguments and options received as command line and create a proper http query string, i.e. :
    With command line args and options: "<tools> --constraint user=jsa 152 156 188"
    We create a useful query: "&constraint_user=jsa&constraint_id=152&constraint_id=156&constraint_id=188"
    """
    @staticmethod
    def makeQuery( pUserArguments, pUserOptions ):

        #
        # Creating corresponding query
        #
        query = ""

        # Applying restriction arguments
        for arg in pUserArguments:
            if arg.isdigit():
                if VERBOSE: print "int as arg, consider an id"
                query += "&constraint_id=%s" % urllib.quote(arg)
            else:
                query += "&constraint_user=%s" % urllib.quote(arg)

            #  TODO: user can specify a date to retrieve/update information on a specific job

        # Applying display attributes
        if hasattr(pUserOptions, 'attribute') and pUserOptions.attribute is not None:
            for attr in pUserOptions.attribute:
                query += "&attr=%s" % urllib.quote(attr)

        # Applying constraints
        if hasattr(pUserOptions, 'constraint') and pUserOptions.constraint is not None:
            for currConst in pUserOptions.constraint:
                constraint = currConst.split("=",1)
                if len(constraint) < 2:
                    print "Error: constraint is not valid, it must have the following format: -C field=value"
                    continue
                constField = str(constraint[0])
                constVal = str(constraint[1])
                query += "&constraint_%s=%s" % (constField , urllib.quote(constVal))

        # Applying updates
        if hasattr(pUserOptions, 'update') and pUserOptions.update is not None:
            for currUpdate in pUserOptions.update:
                updateExpression = currUpdate.split("=",1)
                if len(updateExpression) < 2:
                    print "Error: update info is not valid, it must have the following format: -U field=value"
                    continue
                field = str(updateExpression[0])
                value = str(updateExpression[1])
                query += "&update_%s=%s" % (field , urllib.quote(value))

        return query
