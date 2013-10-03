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

    Returns None if a constraint or update is not valid.
    """
    @staticmethod
    def makeQuery( pUserArguments, pUserOptions ):

        #
        # Creating corresponding query
        #
        query = ""


        #
        # Common preprocess
        #
        # Applying restriction arguments
        for arg in pUserArguments:
            if arg.isdigit():
                query += "&constraint_id=%s" % urllib.quote(arg)
            else:
                query += "&constraint_user=%s" % urllib.quote(arg)
            #  TODO: user might want to specify a date to retrieve/update information on a specific job

        # Applying constraints
        if hasattr(pUserOptions, 'constraint') and pUserOptions.constraint is not None:
            for currConst in pUserOptions.constraint:
                constraint = currConst.split("=",1)
                if len(constraint) < 2:
                    print "Error: constraint is not valid, it must have the following format: -C field=value"
                    return None
                    
                constField = str(constraint[0]).lower()
                constVal = str(constraint[1])

                # Handling regexp for "name" constraint, we would like to hide the python regex complexity to have something
                # close to unix expression syntaxe...
                # TO IMPROVE
                if constField in ['name']:
                    constVal = '^'+constVal.replace('*', '.*')+'$'

                query += "&constraint_%s=%s" % (constField , urllib.quote(constVal))



        #
        # Specific for query requests
        #
        # Applying display attributes
        if hasattr(pUserOptions, 'attribute') and pUserOptions.attribute is not None:
            for attr in pUserOptions.attribute:
                query += "&attr=%s" % urllib.quote(attr)


        #
        # Specific for edit request
        #
        # Applying updates
        if hasattr(pUserOptions, 'update') and pUserOptions.update is not None:
            for currUpdate in pUserOptions.update:
                updateExpression = currUpdate.split("=",1)
                if len(updateExpression) < 2:
                    print "Error: update info is not valid, it must have the following format: -U field=value"
                    return None

                field = str(updateExpression[0])
                value = str(updateExpression[1])
                query += "&update_%s=%s" % (field , urllib.quote(value))

        return query
