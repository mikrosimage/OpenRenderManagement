#!/usr/bin/python
# -*- coding: latin-1 -*-

'''

'''

try:
    import simplejson as json
except ImportError:
    import json

import logging
import time
import re
from datetime import datetime
from tornado.web import HTTPError

from octopus.dispatcher.model import FolderNode
from octopus.core.framework import BaseResource, queue

__all__ = []

logger = logging.getLogger('dispatcher.webservice')

class IQueryNode:

    def filterNodes( self, pFilterArgs, pNodes ):
        """
        Returns a reduced list of nodes according to the given filter arguments (pFilterArgs)
        Filtering works on direct attributes of every nodes: status, user, name, creationtime
        It also works on few optionnals attributes (stored in "tags" dict): prod, shot

        For a single attribute, if several values are given, the resulting list represents the union of every value for this attribute
        However each seperate attribute will "restrict" the result list i.e. it means we operate the intersection between attributes.
        
        For instance, regarding the following filter:
          - constraint_user=['jsa','render']
          - constraint_status=['1','2']

          The resulting list will contain all jobs from user 'jsa' or 'render', having the status '1' or '2'
          i.e.: (user == jsa OR user == render) AND (status == 1 OR status == 2)
        """

        if 'constraint_id' in pFilterArgs:
            filteredIds = [int(id) for id in pFilterArgs['constraint_id']]
            pNodes = [child for child in pNodes if child.id in filteredIds]
            logger.info( "-- Filtering on id list %s, nb remaining nodes: %d", pFilterArgs['constraint_id'], len(pNodes) )

        if 'constraint_status' in pFilterArgs:
            statusList = [int(status) for status in pFilterArgs['constraint_status']]
            pNodes = [child for child in pNodes if child.status in statusList]
            logger.info( "-- Filtering on status %s, nb remaining nodes: %d", pFilterArgs['constraint_status'], len(pNodes) )

        if 'constraint_user' in pFilterArgs:
            pNodes = [child for child in pNodes if child.user in pFilterArgs['constraint_user']]
            logger.info( "-- Filtering on user %s, nb remaining nodes: %d", pFilterArgs['constraint_user'], len(pNodes) )

        if 'constraint_prod' in pFilterArgs:
            filteredNodes = []
            for child in pNodes:
             	if child.tags.get('prod') in pFilterArgs['constraint_prod']:
             		filteredNodes += [child]
            pNodes = filteredNodes
            logger.info( "-- Filtering on prod %s, nb remaining nodes: %d", pFilterArgs['constraint_prod'], len(pNodes) )

        # WARNING: regexp matching constraint can take some time
        # TO IMPROVE
        if 'constraint_name' in pFilterArgs:
            nameRegex = '|'.join( pFilterArgs['constraint_name'] )
            pNodes = [child for child in pNodes if re.match( nameRegex, child.name ) ]
            logger.info( "-- Filtering on name %s, nb remaining nodes: %d", pFilterArgs['constraint_name'], len(pNodes) )

        if 'constraint_creationtime' in pFilterArgs:
            if len(pFilterArgs['constraint_creationtime']) > 1:
                logger.info( "More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_creationtime'][0]) )
            try:
                filterTimestamp = datetime.strptime( pFilterArgs['constraint_creationtime'][0], "%Y-%m-%d %H:%M:%S" ).strftime('%s')
                pNodes = [child for child in pNodes if child.creationTime >= int(filterTimestamp)]

                logger.info( "-- Filtering on date %s (e.g. timestamp=%d), nb remaining nodes: %d", pFilterArgs['constraint_creationtime'][0], 
                    int(filterTimestamp), len(pNodes) )

            except ValueError:
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS"' )
                raise HTTPError(400, 'Invalid date format')
            except Exception:
                logger.warning('Error parsing date constraint')
                raise HTTPError(400, 'Error when parsing date constraint')

        return pNodes
        pass
