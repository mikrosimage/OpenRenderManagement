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
from datetime import datetime

from octopus.dispatcher.model import FolderNode
from octopus.core.framework import BaseResource, queue

__all__ = []

logger = logging.getLogger('dispatcher.webservice')

class IQueryNode:

    def filterNodes( self, pFilterArgs, pNodes ):
        """
        Permet de retourner une liste reduite de noeud en fonction des criteres de filtrage passés en argument (pFilterArgs)
        Le filtrage prend en compte des attributs direct de chaque noeud: status,user,name,creationtime, ainsi que certains
        attributs optionnels du noeud, stockes dans le dictionnaire "tags": prod, shot

        Pour un attribut donne, si plusieurs valeurs possibles sont definies, la liste resultantes est l'union des noeuds correspondant a chaque valeurs.
        En revanche chaque nouvel attribut "restreint" la liste des resultats, i.e. cela induit l'intersection des attributs entre eux.

        par exemple avec les filtres:
          - constraint_user=['jsa','render']
          - constraint_status=['1']
          La liste de resultat contiendra tous les jobs de 'jsa' et de 'render' qui ont le status 1
          soit: (user == jsa OR user == render) AND (status == 1)
        """

        # if 'constraint_id' in pFilterArgs:
        #     filteredIds = [int(id) for id in pFilterArgs['constraint_id']]
        #     nodes = [child for child in nodes if child.id in filteredIds]
        #     for nodeId in filteredIds:
        #         if not any([node.id == nodeId for node in nodes]):
        #             return Http404("Node not found", "Node %d not found." % nodeId, "text/plain")
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

        if 'constraint_name' in pFilterArgs:
            # TODO: Faire evoluer cette contrainte pour pouvoir utiliser une expression reguliere
            # AATENTION au temps d'execution
            pNodes = [child for child in pNodes if child.name in pFilterArgs['constraint_name']]
            logger.info( "-- Filtering on name %s, nb remaining nodes: %d", pFilterArgs['constraint_name'], len(pNodes) )

        if 'constraint_creationtime' in pFilterArgs:
            if len(pFilterArgs['constraint_creationtime']) > 1:
                logger.info( "More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_creationtime'][0]) )
            try:
                filterTimestamp = datetime.strptime( pFilterArgs['constraint_creationtime'][0], "%Y-%m-%d %H:%M:%S" ).strftime('%s')
                # logger.info( "Filtered date: %s (e.g. timestamp=%d)", str(pFilterArgs['constraint_creationtime'][0]), int(filterTimestamp) )

                logger.info( "-- Filtering on date %s (e.g. timestamp=%d), nb remaining nodes: %d", pFilterArgs['constraint_creationtime'][0], 
                    int(filterTimestamp), len(pNodes) )

                pNodes = [child for child in pNodes if child.creationTime >= int(filterTimestamp)]

            except ValueError:
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS"' )
                return Http404('Invalid date format')
            except Exception:
                logger.warning('Error parsing date constraint')
                return Http404('Error when parsing date constraint')

        return pNodes
        pass
