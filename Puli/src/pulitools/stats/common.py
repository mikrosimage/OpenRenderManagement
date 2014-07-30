
# -*- coding: utf8 -*-

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

from optparse import OptionParser
from pygal.style import *

import pygal
import os
import time

from pulitools.common import roundTime

def createCommonParser():
    """
    Create a OptionParser object to handle common params for stats commands
    """
    parser = OptionParser()

    parser.add_option( "-f", action="store", dest="sourceFile", help="Source file" )
    parser.add_option( "-o", action="store", dest="outputFile", help="Target output file." )
    parser.add_option( "--render-mode", action="store", dest="renderMode", type="string", help="render destination: inline, svg or png", default="svg" )

    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )
    parser.add_option( "-s", action="store", dest="rangeIn", type="int", help="Start range is N hours in past", default=3 )
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )

    parser.add_option( "--startTime", action="store", dest="timeIn", type="int", help="Start range is at timestamp", default=0 )
    parser.add_option( "--endTime",   action="store", dest="timeOut", type="int", help="End range is at timestamp", default=time.time() )

    parser.add_option( "-t", "--title", action="store", dest="title", help="Indicates a title", default="")
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates ", default=60 )
    parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )
    parser.add_option( "--scaleRound", action="store", dest="scaleRound", type="int", default=3600 )

    parser.add_option( "--stack", action="store_true", dest="stacked", default=False)
    parser.add_option( "--line", action="store_true", dest="line", default=True)
    parser.add_option( "--interpolate", action="store_true", dest="interpolate", default=False )
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--style", action="store", dest="style", help="Set a specific style name (BlueStyle, RedBlueStyle ...)", default="RedBlue" )
    parser.add_option( "--scaleRotation", action="store", dest="scaleRotation", type="int", default=45 )
    parser.add_option( "--width", action="store", dest="width", type="int" )
    parser.add_option( "--height", action="store", dest="height", type="int" )
    # parser.add_option( "--hide-x", action="store_false", dest="showX", default=True)
    # parser.add_option( "--hide-y", action="store_false", dest="showY", default=True)


    return parser


def getRangeDates( options ):
    """
    Interprete several program options to set a proper date range
    Range is a tuple of timestamp: startDate/endDate
    """
    if options.timeIn is not 0:
        if options.verbose:
            print "Using precise time range: [%s - %s]" % (options.timeIn,options.timeOut)
        startDate = options.timeIn
        endDate = options.timeOut

        if options.timeOut < options.timeIn:
            print "Invalid start/end range"
            sys.exit()
    else:
        if options.rangeIn < options.rangeOut:
            print "Invalid start/end range"
            sys.exit()
        startDate = time.time() - 3600 * options.rangeIn
        endDate = time.time() - 3600 * options.rangeOut

    return startDate, endDate


def prepareScale( npArrScale, options ):

    result = [''] * options.resolution
    tmpscale = []

    for i, date in enumerate(npArrScale):
        tmpscale.append(date[0])
    
    # print tmpscale

    options.scaleEvery = min(options.scaleEvery, options.resolution )

    #########################################################################################
    # Methode 2
    padding = len(npArrScale)/options.scaleEvery
    dayChanged = False
    for i, date in enumerate(tmpscale):
        # print "i: %d - %s " % (i, date)

        
        if i==0:
            result[i] = date.strftime("%m-%d %H:%M")
        else:

            if date.date() != tmpscale[i-1].date():
                # print "change date at index: %d = %s" % (i,date.strftime("%m-%d %H:%M"))
                dayChanged = True

            if i%padding == 0:
                rounded = roundTime(date,options.scaleRound)

                if dayChanged:
                    result[i] = rounded.strftime('%m-%d %H:%M')
                else:
                    result[i] = rounded.strftime('%H:%M')

                dayChanged = False

    result[-1] = tmpscale[-1].strftime("%m-%d %H:%M")

    return result

def prepareGraph( options ):
    """
    Prepare a pygal graph regarding the options given.
    Supported program options are:
    --style
    --width
    --height
    --stack / --line
    --scaleRotation
    --log
    --interpolate
    """

    style = { "RedBlue": RedBlueStyle,
              "Blue" : BlueStyle,
              "Light" : LightStyle,
              "Default": DefaultStyle,
              "Clean": CleanStyle,
              "DarkColorized": DarkColorizedStyle,
              "DarkGreenBlue": DarkGreenBlueStyle,
            }

    if options.style not in style.keys():
        print "Error: Style is not recognised, using \"default\" instead"
        options.style = "Default"

    #
    # Prepare optionnal graph arguments
    #
    kwargs = {"truncate_label":50}
    if options.interpolate:
        kwargs["interpolate"]="hermite"
        kwargs["interpolation_parameters"]={'type': 'cardinal', 'c': .5}
        kwargs["interpolation_precision"]=5

    if options.width:
        kwargs["width"]=options.width

    if options.height:
        kwargs["height"]=options.height

    if options.stacked:
        graph = pygal.StackedLine( x_label_rotation=options.scaleRotation,
                                show_dots=False,
                                fill=True,
                                title_font_size=12,
                                label_font_size=12,
                                legend_font_size=12,
                                spacing=0,
                                logarithmic=options.logarithmic, 
                                # show_x_labels=options.showX,
                                # show_y_labels=options.showY,
                                style=style[options.style],
                                **kwargs
                                )
    else:
        graph = pygal.Line( x_label_rotation=options.scaleRotation,
                                show_dots=True,
                                fill=False,
                                title_font_size=12,
                                label_font_size=12,
                                legend_font_size=12,
                                spacing=0,
                                # margin=2,
                                logarithmic=options.logarithmic, 
                                # show_x_labels=options.showX,
                                # show_y_labels=options.showY,
                                style=style[options.style],
                                **kwargs
                                )

    return graph


def renderGraph( graph, options ):
    """
    Handle graph creation with input params
    """

    if options.renderMode == 'svg':
        graph.render_to_file( options.outputFile )
    elif options.renderMode == 'png':
        graph.render_to_png( options.outputFile )
    elif options.renderMode == 'inline':
        print graph.render()
