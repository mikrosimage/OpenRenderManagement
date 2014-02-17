#!/usr/bin/python2.6
# -*- coding: utf8 -*-
"""
.. module:: Common
   :platform: Unix
   :synopsis: Utility module, classes and static methods used in every command line tools.

The custom table is a generic definition

"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


from settings import Settings
from optparse import IndentedHelpFormatter
from datetime import datetime,timedelta

import urllib, sys, types

VERBOSE=Settings.verbose


class CustomTable:
    """
    | Utility class to display a table with field data. 
    | Use in pul_query/pul_rn tools
    | CustomTable and descendant are a generic way to display information returned by a query.
    | Usual query result is of the form::
    |   {
    |       "item": 
    |       [
    |           {
    |               "field1": val1, 
    |               "field2": val2, 
    |               ...
    |           },
    |           ...
    |       ]
    |   
    |       "summary":
    |       {
    |           "count": xx, 
    |           "totalInDispatcher": xx, 
    |           "requestTime": xx, 
    |           "requestDate": xx
    |       }
    |   }
    | 
    | Any class inheriting CustomTable can define a list of column, each column handling the representation of a field and its header.
    | From outside the class, a user can then delcare a table representation and call the main functions:
    |  :displayHeader: to print header on stdout
    |  :displayRow: to print each individual row on stdout
    |  :displayFooter: to print the footer (i.e. summary info)
    | 
    | The representation class will allow several attribute for each column:
    |  :field: a data field (from query result) or a formula (any CustomTable formula method)
    |  :label: a text used for table header
    |  :visible: a flag indicating if the column will be printed
    |  :dataFormat: a format for the corresponding field, it uses the 'print' (and similar to POSIX print) function
    |  :labelFormat: idem for label info
    |  :truncate: Optionnal attribute, the max length that should be displayed (to avoid messing with columns alignment)
    |  :transform: Optionnal attribute, the name of a static method of the parent CustomTable class. It will preprocess the value before displaying it at a string (example: date format, status short name)
    | 
    | The mecanism to calcultate a data or transform a data, is based on python's ability to store function addresses.
    | Formulas and Transforms are defined as CustomTable static method.
    |   - A formula will be defined in the "field" of the column, it is a tuple : the first item is the formula function, the remaining items are the parameters
    |   - A transformation is defined as "transform" column, it is the address of a transform function
    """
    # @staticmethod
    # def truncateStr(pValue, pLen):
    #     return str(datetime.strftime( datetime.fromtimestamp( pValue ), Settings.date_format ))

    #
    # Formula methods
    #

    @staticmethod
    def formulaDiff( pValue1, pValue2 ):
        if pValue1 is None or pValue2 is None:
            return 0
        return float(pValue1) - float(pValue2)

    @staticmethod
    def formulaRuntime( pEndTime, pStartTime ):
        result = "-"
        if pStartTime is not None:
            if pEndTime is not None:
                result = datetime.fromtimestamp( pEndTime ) - datetime.fromtimestamp( pStartTime )
            else:
                result = datetime.now() - datetime.fromtimestamp( pStartTime )
        return result


    #
    # Transformation methods
    #
    @staticmethod
    def timeToStr(pValue):
        return str(timedelta (seconds=pValue))


    @staticmethod
    def dateToStr(pValue):
        return str(datetime.strftime( datetime.fromtimestamp( float(pValue) ), Settings.date_format ))

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
    def displayRow( pRow, pDescription, pDepth=0 ):
        '''
        Print content row for a particular table description and data.
        '''
        line=""
        for column in pDescription.columns:

            if column['visible'] == True:

                try:
                    if type(column['field']) is tuple:
                        # The field is a formula
                        formula = column['field'][0]
                        args = [ pRow[curField] for curField in column['field'][1:] ]

                        try:
                            data = str(formula( *args ))
                        except Exception, e:
                            print "Invalid formula execution for %r --> %r" % (column['field'], e)
                            sys.exit()

                        # Apply transformation if specified, this process take a data and calls a method to retrieve a string
                        if 'transform' in column.keys():
                            try:
                                data = column['transform']( data )
                            except Exception, e:
                                print "Invalid transformation for column %r --> %r" % (data, e)
                                sys.exit()

                    elif pRow[column['field']] is None:
                        # The field is None, use replacement text
                        data = "-"
                    else:
                        data = pRow[column['field']]

                        # Apply transformation if specified, this process take a data and calls a method to retrieve a string
                        if 'transform' in column.keys():
                            try:
                                data = column['transform']( data )
                            except Exception, e:
                                print "Invalid transformation for column %r --> %r" % (column['field'], e)
                                sys.exit()

                    # Limit length of the data if specified
                    if 'truncate' in column.keys() and len(data) > column['truncate']:
                        data = data[:(column['truncate']-3)]+"..."

                    line += column['dataFormat'] % data

                except KeyError, e:
                    print "Error displaying column %r --> %r" % ( column, e )
                    sys.exit()

        # Once all columns are processed, display the full line
        if pDepth == 0:
            print line
        else:
            print (" "*pDepth)+"`"+line

        if 'items' in pRow:
            pDepth += 1
            for child in pRow['items']:
                CustomTable.displayRow( child, pDescription, pDepth )

    @staticmethod
    def displayFooter( pSummary, pDescription ):
        '''
        Print footer for a particular table description and data.
        '''

        print ""
        print "Summary: %d of %d items retrieved in %.3f s." % (pSummary['count'], pSummary['totalInDispatcher'], pSummary['requestTime'])
        print ""




class JobTable( CustomTable ):
    """
    | Definition of a table representation for jobs.
    | 
    | Usage:
    |     - field:        the data to display, supported fields are those defined in job object
    |     - label:        a text used for table header
    |     - visible:      a flag indicating if the column will be printed
    |     - dataFormat:   a format for the corresponding field, it uses the 'print' (and similar to POSIX print) function
    |     - labelFormat:  idem for label info
    |     - truncate:     Optionnal attribute, the max length that should be displayed (to avoid messing with columns alignment)
    |     - transform:    Optionnal attribute, the name of a static method of the parent CustomTable class.
    |                     It will preprocess the value before displaying it at a string (example: date format, status short name)
    """

    columns = [
            {
                "field":        "id", 
                "label":        "ID", 
                "visible":      True, 
                "dataFormat":   "%-6d",
                "labelFormat":  "%-6s",
            },
            {
                "field":        "status", 
                "label":        "ST", 
                "visible":      True, 
                "dataFormat":   " %-2s",
                "labelFormat":  " %-2s",
                "transform":    CustomTable.jobStatusToStr,
            },
            {
                "field":        "name", 
                "label":        "NAME", 
                "visible":      True, 
                "dataFormat":   " %-30s",
                "labelFormat":  " %-30s",
                "truncate":     50,
            },
            {
                "field":        "prod", 
                "label":        "PROD", 
                "visible":      True, 
                "dataFormat":   " %-10s",
                "labelFormat":  " %-10s",
                "truncate":     10,
            },
            {
                "field":        "shot", 
                "label":        "SHOT", 
                "visible":      True, 
                "dataFormat":   " %-10s",
                "labelFormat":  " %-10s",
                "truncate":     10,
            },
            {
                "field":        "", 
                "label":        "IMG", 
                "visible":      False, 
                "dataFormat":   " %-10s",
                "labelFormat":  " %-10s",
            },
            {
                "field":        "user", 
                "label":        "OWNER", 
                "visible":      True, 
                "dataFormat":   " %-5s",
                "labelFormat":  " %-5s",
                "truncate":     5,
            },
            {
                "field":        "dispatchKey", 
                "label":        "PRIO", 
                "visible":      True, 
                "dataFormat":   " %4d",
                "labelFormat":  " %4s",
            },
            {
                "field":        "completion", 
                "label":        "%", 
                "visible":      True, 
                "dataFormat":   " %3.f",
                "labelFormat":  " %3s",
                "transform":    CustomTable.percentToFloat,
            },
            {
                "field":        "maxRN", 
                "label":        "MAX", 
                "visible":      True, 
                "dataFormat":   " %3d",
                "labelFormat":  " %3s",
            },
            {
                "field":        "allocatedRN", 
                "label":        "ALLOC", 
                "visible":      True, 
                "dataFormat":   " %5d",
                "labelFormat":  " %5s",
            },
            {
                "field":        "creationTime", 
                "label":        "SUBMITTED", 
                "visible":      True, 
                "dataFormat":   " %-12s",
                "labelFormat":  " %-12s",
                "transform":    CustomTable.dateToStr,
            },
            {
                "field":        "startTime", 
                "label":        "START", 
                "visible":      True, 
                "dataFormat":   " %-12s",
                "labelFormat":  " %-12s",
                "transform":    CustomTable.dateToStr,
            },
            {
                "field":        "endTime", 
                "label":        "END", 
                "visible":      True, 
                "dataFormat":   " %-12s",
                "labelFormat":  " %-12s",
                "transform":    CustomTable.dateToStr,
            },

            {
                "field":        ( CustomTable.formulaRuntime, "endTime", "startTime" ),
                "label":        "RUN TIME", 
                "visible":      True, 
                "dataFormat":   " %10s",
                "labelFormat":  " %10s",
            },

            {
                "field":        "averageTimeByFrame",
                "label":        "AVG TIME", 
                "visible":      True, 
                "dataFormat":   " %10s",
                "labelFormat":  " %10s",
                "transform":    CustomTable.timeToStr,

            },
        ]





class RenderNodeTable( CustomTable ):
    """
    | Definition of a table representation for RN.
    | 
    | Usage:
    |     - Field: the data to display, supported fields are those defined in RN object
    |     - Label: a text used for table header
    |     - Visible: a flag indicating if the column will be printed
    |     - dataFormat: a format for the corresponding field, it uses the 'print' (and similar to POSIX print) function
    |     - labelFormat: idem for label info
    |     - transform: Optionnal attribute, the name of a static method of the parent CustomTable class.
    |                  It will preprocess the value before displaying it at a string (example: date format, status short name)
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
                "field":        "createDate", 
                "label":        "CREATE", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s",
                "transform":    CustomTable.dateToStr
            },
            {
                "field":        "registerDate", 
                "label":        "REGISTER", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s",
                "transform":    CustomTable.dateToStr
            },
            {
                "field":        "lastAliveTime", 
                "label":        "LAST PING", 
                "visible":      True, 
                "dataFormat":   " %-15s",
                "labelFormat":  " %-15s",
                "transform":    CustomTable.dateToStr
            },

        ]






class ConstraintFactory:
    """
    | Can parse arguments and options received as command line and create a proper http query string, i.e.:
    | With command line args and options: "<tools> --constraint user=jsa 152 156 188"
    | We create a useful query: "&constraint_user=jsa&constraint_id=152&constraint_id=156&constraint_id=188"
    | 
    | Returns None if a constraint or update is not valid.
    """

    @staticmethod
    def makeQuery( pUserArguments, pUserOptions ):
        '''
        | Parse arguments and options to create a proper http query string:
        | "constraint_user=jsa&constraint_id=152"
        | With command line args and options: "--constraint user=jsa -c id=152 -c id=156 -c id=188"
        | We create a useful query: "&constraint_user=jsa&constraint_id=152&constraint_id=156&constraint_id=188"
        
        :param pUserArguments: A list of strings representing user arguments
        :param pUserOptions: A dic of strings representing user options
        :return: A string query or none if a constraint or update is not valid.
        '''
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
            