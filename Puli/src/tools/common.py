#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
commons.py: Utility classes and static methods used in every tools
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


from settings import Settings
from optparse import IndentedHelpFormatter
from datetime import datetime

import urllib, sys

VERBOSE=Settings.verbose


class CustomTable:
    """
    Utility class to display a table with field data. 
    Use in pul_query/pul_rn tools
    """

    @staticmethod
    def dateToStr(pValue):
        return str(datetime.strftime( datetime.fromtimestamp( pValue ), Settings.date_format ))

    @staticmethod
    def percentToFloat(pValue):
        return float(pValue) * 100.0

    @staticmethod
    def nodeStatusToStr(pValue):
        from octopus.core.enums.rendernode import RN_STATUS_SHORT_NAMES
        return str( RN_STATUS_SHORT_NAMES[pValue] )

    @staticmethod
    def jobStatusToStr(pValue):
        from octopus.core.enums.node import NODE_STATUS_SHORT_NAMES
        return str( NODE_STATUS_SHORT_NAMES[pValue] )

    @staticmethod
    def displayHeader( pDescription ):
        '''
        Print header line for a particular table description.
        '''
        line=""
        for column in pDescription.columns:
            if column['visible'] == True:
                line += column['labelFormat'] % column['label']

        print ""
        print line
        print "-" * ( len(line)+2 )


    @staticmethod
    def displayRow( pRow, pDescription ):
        '''
        Print content row for a particular table description and data.
        '''

        line=""
        for column in pDescription.columns:
            if column['visible'] == True:

                if 'transform' in column.keys():
                    try:
                        data = column['transform'](pRow[column['field']])
                    except Exception, e:
                        print "Invalid transformation for column %r: %r" % (column['field'], e)
                        sys.exit()
                else:
                    data = pRow[column['field']]

                try:
                    line += column['dataFormat'] % data
                except KeyError, e:
                    print "Error: Invalid field specified --> %r" % e
                    sys.exit()

        print line

    @staticmethod
    def displayFooter( pSummary, pDescription ):
        '''
        Print footer for a particular table description and data.
        '''

        print ""
        print "Summary: %d of %d items retrieved in %.2f s." % (pSummary['count'], pSummary['totalInDispatcher'], pSummary['requestTime']*1000)
        print ""




class RenderNodeTable( CustomTable ):
    """
    Definition of a table representation for rendernodes.
    
    Usage:
        - Field: the data to display, supported fields are those defined in rendernode object
        - Label: a text used for table header
        - Visible: a flag indicating if the column will be printed
        - dataFormat: a format for the corresponding field, it uses the 'print' (and similar to POSIX print) function
        - labelFormat: idem for label info
        - transform: Optionnal attribute, the name of a static method of the parent CustomTable class.
                     It will preprocess the value before displaying it at a string (example: date format, status short name)
    """
    columns = [
            {
                "field":        "id", 
                "label":        "ID", 
                "visible":      True, 
                "dataFormat":   " %-5d",
                "labelFormat":  " %-5s"
            },
            {
                "field":        "status", 
                "label":        "ST", 
                "visible":      True, 
                "dataFormat":   " %-2s",
                "labelFormat":  " %-2s",
                "transform":    CustomTable.nodeStatusToStr
            },
            {
                "field":        "name", 
                "label":        "NAME", 
                "visible":      True, 
                "dataFormat":   " %-25s",
                "labelFormat":  " %-25s"
            },
            {
                "field":        "prod", 
                "label":        "PROD", 
                "visible":      True, 
                "dataFormat":   " %-10s",
                "labelFormat":  " %-10s"
            },
            {
                "field":        "shot", 
                "label":        "SHOT", 
                "visible":      True, 
                "dataFormat":   " %-10s",
                "labelFormat":  " %-10s"
            },
            {
                "field":        "endTime", 
                "label":        "END", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s",
                "transform":    CustomTable.dateToStr
            },

        ]





class JobTable( CustomTable ):
    """
    Definition of a table representation for jobs.
    
    Usage:
        - Field: the data to display, supported fields are those defined in job object
        - Label: a text used for table header
        - Visible: a flag indicating if the column will be printed
        - dataFormat: a format for the corresponding field, it uses the 'print' (and similar to POSIX print) function
        - labelFormat: idem for label info
        - transform: Optionnal attribute, the name of a static method of the parent CustomTable class.
                     It will preprocess the value before displaying it at a string (example: date format, status short name)
    """
    columns = [
            {
                "field":        "id", 
                "label":        "ID", 
                "visible":      True, 
                "dataFormat":   " %-5d",
                "labelFormat":  " %-5s"
            },
            {
                "field":        "status", 
                "label":        "ST", 
                "visible":      True, 
                "dataFormat":   " %-2s",
                "labelFormat":  " %-2s",
                "transform":    CustomTable.jobStatusToStr
            },
            {
                "field":        "name", 
                "label":        "HOST", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s"
            },
            {
                "field":        "ramSize", 
                "label":        "RAM", 
                "visible":      True, 
                "dataFormat":   " %-8d",
                "labelFormat":  " %-8s"
            },
            {
                "field":        "coresNumber", 
                "label":        "CPU", 
                "visible":      True, 
                "dataFormat":   " %-3d",
                "labelFormat":  " %-3s",
            },
            {
                "field":        "speed", 
                "label":        "GHZ", 
                "visible":      True, 
                "dataFormat":   " %-.2f ",
                "labelFormat":  " %-4s "
            },
            {
                "field":        "lastAliveTime", 
                "label":        "UPDATE", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s",
                "transform":    CustomTable.dateToStr
            },

        ]






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

        # Indicating to retrieve subnodes hierarchy
        if hasattr(pUserOptions, 'tree') and pUserOptions.tree is True:
            query += "&tree=1"


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



class PlainHelpFormatter(IndentedHelpFormatter): 
    '''
    Subclass of OptParse format handler, will allow to have a raw text formatting in usage and desc fields.
    '''
    def format_description(self, description):
        if description:
            return description + "\n"
        else:
            return ""