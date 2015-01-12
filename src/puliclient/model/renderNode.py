# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2015, Mikros Image"


try:
    import simplejson as json
except Exception:
    import json

# # Sys infos
# id
# name
# coresNumber
# ramSize
# speed
# puliversion
#
# # Dynamic sys infos
# # freeCoresNumber
# # freeRam
# systemFreeRam
# systemSwapPercentage
#
# # Worker state
# commands
# status
# host
# port
# pools
# caracteristics
# performance
# excluded
#
# # Timers
# createDate
# registerDate
# lastAliveTime
#
#
# # new fields suggestion
# stateHistory
# commandHistory
# userCommandHistory