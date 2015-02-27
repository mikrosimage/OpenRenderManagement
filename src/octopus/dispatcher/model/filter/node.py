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

    def matchPool(self, elem):
        for pool in elem.poolShares:
            if pool.name in self.currFilter:
                return True
        return False

    def matchStatus(self, elem):
        return True if elem.status in self.currFilter else False

    def matchTags(self, elem):
        """
        Check current elem against the tags filter stored (self.currentFilter). Current filter stores one or several
        each of them having one or several values. The current behavior is to match any tags or value given.
        i.e. with the following filter:
        {
          'prod':['prodA','prodB'],
          'shot':['shot1']
        }
        We will match any job having either prod=prodA OR prod=prodB OR shot=shot1.
        TODO: create a more flexible request on tags with OR/AND operators ?
        :param elem: a job node
        :return: boolean indicating if the current element is matched
        """
        for name, value in self.currFilter.items():
            res = self._matchSingleTag(name, value, elem.tags)

            if res is True:
                return True
        return False

        #
        # Implementing a AND condition btw tags.
        # It is not really interesting if _matchSingleTag is working as OR btw tag possible values
        # We need to find a cleverer way to do so

        #     if res is False:
        #         return False
        # return True

    def _matchSingleTag(self, name, valuesList, jobTags):
        if name not in jobTags.keys():
            return False
        if jobTags.get(name) not in valuesList:
            return False
        return True

    def matchUser(self, elem):
        # regex = '|'.join(self.currFilter)
        # return re.match(regex, elem.user)
        return True if elem.user in self.currFilter else False

    def matchNodes(self, filters, nodes):

        if 'id' in filters and filters.get('id') is not []:
            self.currFilter = [int(id) for id in filters['id']]
            nodes = filter(self.matchId, nodes)
            logging.getLogger('main.filter').info("-- Filtering on id list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'name' in filters and filters.get('name') is not []:
            self.currFilter = filters['name']
            nodes = filter(self.matchName, nodes)
            logging.getLogger('main.filter').info("-- Filtering on names list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'pool' in filters and filters.get('pool') is not []:
            self.currFilter = filters['pool']
            nodes = filter(self.matchPool, nodes)
            logging.getLogger('main.filter').info("-- Filtering on pool list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'status' in filters and filters.get('status') is not []:
            self.currFilter = filters['status']
            nodes = filter(self.matchStatus, nodes)
            logging.getLogger('main.filter').info("-- Filtering on status list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'tags' in filters and filters.get('tags') is not []:
            self.currFilter = filters['tags']
            nodes = filter(self.matchTags, nodes)
            logging.getLogger('main.filter').info("-- Filtering on tags %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'user' in filters and filters.get('user') is not []:
            self.currFilter = filters['user']
            nodes = filter(self.matchUser, nodes)
            logging.getLogger('main.filter').info("-- Filtering on users list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        return nodes
