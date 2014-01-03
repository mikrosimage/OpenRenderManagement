#!/usr/bin/python2.6
# -*- coding: utf8 -*-

'''
Generic description of the module
'''
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"

class MyTool:
    """
    Generic description of the class and usage presentation
    """
    def __init__( self, pParam1 ... ):
        pass

    def _privateMethod( self, pParam1 ... ):
        """
        Description on a single line

        :param pPram1: describe me
        :type pParam1: type if necessary
        :return:
        :raise:
        """
        pass

    def publicMethod( self, pFirstParam, pSecondParam ):
        """
        | Generic description of the method
        | On multiple lines for readibility

        :param pPram1: describe me
        :type pParam1: type if necessary
        :return:
        :raise:
        """
        return "myresult"
   