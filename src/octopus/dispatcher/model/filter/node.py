#!/usr/bin/python
# -*- coding: latin-1 -*-

"""
"""

import logging
import re

__all__ = []


class FilterError(Exception):
    pass


class IFilterNode:
    """
    Class helping to filter on node objects.
    Supported filters are:
    - id
    - name
    """
    def __init__(self):
        self.currFilter = None

    def matchKeyValue(self):
        raise NotImplementedError

    def matchDatetime(self):
        raise NotImplementedError

    def matchFloat(self):
        raise NotImplementedError

    def matchString(self):
        raise NotImplementedError

    def matchId(self, elem):
        return True if elem.id in self.currFilter else False

    def matchName(self, elem):
        regex = '|'.join(self.currFilter)
        return re.match(regex, elem.name)

    def matchStatus(self, elem):
        return True if elem.status in self.currFilter else False

    def matchUser(self, elem):
        regex = '|'.join(self.currFilter)
        return re.match(regex, elem.user)

    def matchNodes(self, filters, nodes):

        if 'id' in filters and filters.get('id') is not []:
            self.currFilter = [int(id) for id in filters['id']]
            nodes = filter(self.matchId, nodes)
            logging.getLogger('main.dispatcher.filter').info("-- Filtering on id list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'name' in filters and filters.get('name') is not []:
            self.currFilter = filters['name']
            nodes = filter(self.matchName, nodes)
            logging.getLogger('main.dispatcher.filter').info("-- Filtering on names list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'user' in filters and filters.get('user') is not []:
            self.currFilter = filters['user']
            nodes = filter(self.matchName, nodes)
            logging.getLogger('main.dispatcher.filter').info("-- Filtering on users list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'status' in filters and filters.get('status') is not []:
            self.currFilter = filters['status']
            nodes = filter(self.matchStatus, nodes)
            logging.getLogger('main.dispatcher.filter').info("-- Filtering on status list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        return nodes
