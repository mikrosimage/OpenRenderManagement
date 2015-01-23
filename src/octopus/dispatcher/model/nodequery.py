#!/usr/bin/python
# -*- coding: latin-1 -*-

'''

'''

import logging
import re
from datetime import datetime
from tornado.web import HTTPError

__all__ = []

logger = logging.getLogger('main.dispatcher.webservice')


class QueryError(Exception):
    pass


class IQueryNode:

    def matchKeyValue(self):
        raise NotImplementedError

    def matchDatetime(self):
        raise NotImplementedError

    def matchString(self):
        raise NotImplementedError

    def matchName(self, elem):
        regex = '|'.join(self.currFilter)
        return re.match(regex, elem.name)

    def matchId(self, elem):
        return True if elem.id in self.currFilter else False

    def matchFloat(self):
        raise NotImplementedError

    def matchNodes(self, filters, nodes):

        if 'id' in filters:
            self.currFilter = [int(id) for id in filters['id']]
            nodes = filter(self.matchId, nodes)
            logger.info("-- Filtering on id list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        if 'name' in filters:
            self.currFilter = filters['name']
            nodes = filter(self.matchName, nodes)
            logger.info("-- Filtering on id list %s, nb remaining nodes: %d", self.currFilter, len(nodes))

        return nodes

    def filterNodes(self, pFilterArgs, pNodes):
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
            logger.info("-- Filtering on id list %s, nb remaining nodes: %d", pFilterArgs['constraint_id'], len(pNodes))

        if 'constraint_status' in pFilterArgs:
            statusList = [int(status) for status in pFilterArgs['constraint_status']]
            pNodes = [child for child in pNodes if child.status in statusList]
            logger.info("-- Filtering on status %s, nb remaining nodes: %d", pFilterArgs['constraint_status'], len(pNodes))

        if 'constraint_user' in pFilterArgs:
            pNodes = [child for child in pNodes if child.user in pFilterArgs['constraint_user']]
            logger.info("-- Filtering on user %s, nb remaining nodes: %d", pFilterArgs['constraint_user'], len(pNodes))

        if 'constraint_prod' in pFilterArgs:
            filteredNodes = []
            for child in pNodes:
                if child.tags.get('prod') in pFilterArgs['constraint_prod']:
                    filteredNodes += [child]
            pNodes = filteredNodes
            logger.info("-- Filtering on prod %s, nb remaining nodes: %d", pFilterArgs['constraint_prod'], len(pNodes))

        # WARNING: regexp matching constraint can take some time
        # TO IMPROVE
        if 'constraint_name' in pFilterArgs:
            nameRegex = '|'.join(pFilterArgs['constraint_name'])
            pNodes = [child for child in pNodes if re.match(nameRegex, child.name)]
            logger.info("-- Filtering on name %s, nb remaining nodes: %d", pFilterArgs['constraint_name'], len(pNodes))

        if 'constraint_creationtime' in pFilterArgs:
            if len(pFilterArgs['constraint_creationtime']) > 1:
                logger.info("More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_creationtime'][0]))
            try:
                filterTimestamp = datetime.strptime(pFilterArgs['constraint_creationtime'][0], "%Y-%m-%d %H:%M:%S").strftime('%s')
                pNodes = [child for child in pNodes if child.creationTime >= int(filterTimestamp)]

                logger.info("-- Filtering on date %s (e.g. timestamp=%d), nb remaining nodes: %d", pFilterArgs['constraint_creationtime'][0], int(filterTimestamp), len(pNodes))

            except ValueError:
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS"')
                raise HTTPError(400, 'Invalid date format')
            except Exception:
                logger.warning('Error parsing date constraint')
                raise HTTPError(400, 'Error when parsing date constraint')

        if 'constraint_starttime' in pFilterArgs:
            if len(pFilterArgs['constraint_starttime']) > 1:
                logger.info("More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_starttime'][0]))
            try:
                filterTimestamp = datetime.strptime(pFilterArgs['constraint_starttime'][0], "%Y-%m-%d %H:%M:%S").strftime('%s')
                pNodes = [child for child in pNodes if child.startTime >= int(filterTimestamp)]

                logger.info("-- Filtering on date %s (e.g. timestamp=%d), nb remaining nodes: %d", pFilterArgs['constraint_starttime'][0], int(filterTimestamp), len(pNodes))

            except ValueError:
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS"')
                raise HTTPError(400, 'Invalid date format')
            except Exception:
                logger.warning('Error parsing date constraint')
                raise HTTPError(400, 'Error when parsing date constraint')

        if 'constraint_endtime' in pFilterArgs:
            if len(pFilterArgs['constraint_endtime']) > 1:
                logger.info("More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_endtime'][0]))
            try:
                filterTimestamp = datetime.strptime(pFilterArgs['constraint_endtime'][0], "%Y-%m-%d %H:%M:%S").strftime('%s')
                pNodes = [child for child in pNodes if child.endTime >= int(filterTimestamp)]

                logger.info("-- Filtering on date %s (e.g. timestamp=%d), nb remaining nodes: %d", pFilterArgs['constraint_endtime'][0], int(filterTimestamp), len(pNodes))

            except ValueError:
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS"')
                raise HTTPError(400, 'Invalid date format')
            except Exception:
                logger.warning('Error parsing date constraint')
                raise HTTPError(400, 'Error when parsing date constraint')

        return pNodes

    def filterRenderNodes(self, pFilterArgs, pNodes):
        """
        Returns a reduced list of rn according to the given filter arguments (pFilterArgs)
        Filtering works on direct attributes of every items: status, name, host, port

        For a single attribute, if several values are given, the resulting list represents the union of every value for this attribute
        However each seperate attribute will "restrict" the result list i.e. it means we operate the intersection between attributes.

        For instance, regarding the following filter:
          - constraint_host=['vfxpc64','rndlin100']
          - constraint_status=['1','2']

          The resulting list will contain all rn from user 'jsa' or 'render', having the status '1' or '2'
          i.e.: (user == jsa OR user == render) AND (status == 1 OR status == 2)
        """

        # if 'constraint_id' in pFilterArgs:
        #     filteredIds = [int(id) for id in pFilterArgs['constraint_id']]
        #     pNodes = [child for child in pNodes if child.id in filteredIds]
        #     logger.info( "-- Filtering on id list %s, nb remaining nodes: %d", pFilterArgs['constraint_id'], len(pNodes) )

        if 'constraint_status' in pFilterArgs:
            statusList = [int(status) for status in pFilterArgs['constraint_status']]
            pNodes = [child for child in pNodes if child.status in statusList]
            logger.info("-- Filtering on status %s, nb remaining render nodes: %d", pFilterArgs['constraint_status'], len(pNodes))

        # if 'constraint_user' in pFilterArgs:
        #     pNodes = [child for child in pNodes if child.user in pFilterArgs['constraint_user']]
        #     logger.info( "-- Filtering on user %s, nb remaining nodes: %d", pFilterArgs['constraint_user'], len(pNodes) )

        # WARNING: regexp matching constraint can take some time
        # TO IMPROVE
        if 'constraint_name' in pFilterArgs:
            nameRegex = '|'.join(pFilterArgs['constraint_name'])
            pNodes = [child for child in pNodes if re.match(nameRegex, child.name)]
            logger.info("-- Filtering on name %s, nb remaining render nodes: %d", pFilterArgs['constraint_name'], len(pNodes))

        if 'constraint_speed' in pFilterArgs:
            cpuspeedFilters = pFilterArgs['constraint_speed']
            for cpuspeed in cpuspeedFilters:
                if cpuspeed[0] == '+':
                    cpuspeed = float(cpuspeed[1:])
                    pNodes = [child for child in pNodes if cpuspeed < child.speed]
                elif cpuspeed[0] == '-':
                    cpuspeed = float(cpuspeed[1:])
                    pNodes = [child for child in pNodes if child.speed < cpuspeed]
                else:
                    pNodes = [child for child in pNodes if child.speed == float(cpuspeed)]
            logger.info("-- Filtering on cpu speed %s, nb remaining render nodes: %d", pFilterArgs['constraint_speed'], len(pNodes))

        if 'constraint_ramsize' in pFilterArgs:
            ramFilters = pFilterArgs['constraint_ramsize']

            for ram in ramFilters:
                if ram[0] == '+':
                    ram = int(ram[1:])
                    pNodes = [child for child in pNodes if ram < child.ramSize]
                elif ram[0] == '-':
                    ram = int(ram[1:])
                    pNodes = [child for child in pNodes if child.ramSize < ram]
                else:
                    pNodes = [child for child in pNodes if child.ramSize == int(ram)]
            logger.info("-- Filtering on ramsize %s, nb remaining render nodes: %d", pFilterArgs['constraint_ramsize'], len(pNodes))

        if 'constraint_coresnumber' in pFilterArgs:
            coreFilters = pFilterArgs['constraint_coresnumber']

            for core in coreFilters:
                if core[0] == '+':
                    core = int(core[1:])
                    pNodes = [child for child in pNodes if core < child.coresNumber]
                elif core[0] == '-':
                    core = int(core[1:])
                    pNodes = [child for child in pNodes if child.coresNumber < core]
                else:
                    pNodes = [child for child in pNodes if child.coresNumber == int(core)]
            logger.info("-- Filtering on coresNumber %s, nb remaining render nodes: %d", pFilterArgs['constraint_coresnumber'], len(pNodes))

        return pNodes

    def compareTS(self, operator, date1, date2):
        """
        Use a string operator (<,>) to return a value between to timestamps
        :operator: a single charater '<' or '>'
        :date1: first date to compare
        :date2: second date to compare
        :return: a boolean indicating the result of "date1 OPERATOR date2"
        """
        if operator == '<':
            return True if date1 < date2 else False
        elif operator == '>':
            return True if date1 > date2 else False
        else:
            raise QueryError('Invalid operator')

    def filterCommands(self, pFilterArgs, pItems):
        """
        Parse several given constraint to reduce the number of items in list.
        The resulting items will be sent back as a request response.
        """

        # Creating filter from constraints given in request
        filterId = False
        filterStatus = False
        filterStartTime = False

        if 'constraint_id' in pFilterArgs:
            filterId = True
            filteredIds = [int(id) for id in pFilterArgs['constraint_id']]

        if 'constraint_status' in pFilterArgs:
            filterStatus = True
            statusList = [int(status) for status in pFilterArgs['constraint_status']]

        if 'constraint_starttime' in pFilterArgs:
            filterStartTime = True
            if len(pFilterArgs['constraint_starttime']) > 1:
                logger.info("More than one date specified, first occurence is used: %s" % str(pFilterArgs['constraint_starttime'][0]))
            try:
                if pFilterArgs['constraint_starttime'][0][0] in ['>', '<']:
                    operator, dateValue = pFilterArgs['constraint_starttime'][0][:1], pFilterArgs['constraint_starttime'][0][1:]
                else:
                    operator, dateValue = ('>', pFilterArgs['constraint_starttime'][0])

                filterTimestamp = int(datetime.strptime(dateValue, "%Y-%m-%d %H:%M:%S").strftime('%s'))
            except (ValueError, QueryError):
                logger.warning('Error: invalid date format, the format definition is "YYYY-mm-dd HH:MM:SS", optionnal operator can be added as first character "<" or ">"')
                raise HTTPError(400, 'Invalid date format or invalid operator')
            except Exception:
                logger.warning('Error parsing date constraint')
                raise HTTPError(400, 'Error when parsing date constraint')

        # Loop over elem to yiel result
        for item in pItems:
            if filterId and item.id not in filteredIds:
                continue

            if filterStatus and item.status not in statusList:
                continue

            if filterStartTime and not self.compareTS(operator, item.startTime, filterTimestamp):
                continue

            yield item
