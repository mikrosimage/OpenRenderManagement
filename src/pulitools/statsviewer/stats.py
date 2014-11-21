#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"


from PyQt4.QtGui import QApplication, QMainWindow, QColor, QTextCursor, QIcon
from PyQt4.QtWebKit import QWebSettings
from PyQt4.QtCore import QDateTime, QUrl, QProcess, Qt
from PyQt4.QtCore import QObject, pyqtSignal
from PyQt4.QtCore import qDebug


from pulitools.statsviewer.widget.mainwindow import Ui_MainWindow
from pulitools.common import XLogger, OutLog

import sys
import subprocess
import time
import logging
from logging import StreamHandler
from tempfile import NamedTemporaryFile
import codecs
import collections




class StatsMainWindow(QMainWindow):
    """
    """

    LOGDIR = "/s/apps/lin/vfx_test_apps/pulistats/"
    # SRCDIR = "/datas/jsa/OpenRenderManagement/"
    BASEDIR = "/s/apps/lin/puli/"

    reportDict={ 
        "RN usage": { 
            "cmd": BASEDIR + "scripts/util/update_usage_stats",
            "source":"/s/apps/lin/vfx_test_apps/pulistats/logs/usage_stats.log"
            },

        "Job usage" : { 
            "cmd": BASEDIR + "scripts/util/update_queue_stats",
            "source":"/s/apps/lin/vfx_test_apps/pulistats/logs/queue_stats.log"
        },
    }

    queueTrackParam={
        "Num errors":"err",
        "Num jobs":"jobs",
        "Num paused":"paused",
        "Num ready or blocked":"ready",
        "Num rn allocated":"allocatedRN",
        "Num running":"running",
    }

    queueGroupByParam={
        "Prod":"prod",
        "User":"user",
        "Step":"step",
        "Type":"type"
    }

    def __init__(self):
        QMainWindow.__init__(self)

        # Set up the user interface from Designer.
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.ui.actionGenerate.setIcon( QIcon( StatsMainWindow.BASEDIR+"tools/statsviewer/rsrc/refresh.png") )

        sys.stdout = OutLog( self.ui.log , sys.stdout)
        xlogger.info("Starting log")


        # Create tmp file for svg
        self.svgFile = NamedTemporaryFile( suffix=".svg", delete=True )
        xlogger.info("Creating temp file: %s"%self.svgFile.name)

        #
        # Set UI values
        #

        # Set report choices
        for key, elem in self.reportDict.items():
            self.ui.cbReport.addItem(key)

        # Set queue param choices
        for key, elem in sorted(self.queueTrackParam.items()):
            self.ui.cbTrack.addItem(key)
        self.ui.cbTrack.setCurrentIndex(1)

        for key, elem in sorted(self.queueGroupByParam.items()):
            self.ui.cbGroupBy.addItem(key)
        self.ui.cbGroupBy.setCurrentIndex(0)


        #
        # Connect UI
        #

        # Update end date value
        self.ui.dtEndDate.setDateTime( QDateTime.currentDateTime() )
        
        # Connect up the buttons.
        self.ui.actionGenerate.triggered.connect(self.generateGraph)
        self.ui.actionReset.triggered.connect(self.resetParams)
        self.ui.cbReport.currentIndexChanged.connect( self.updateParamsVisibility )
        self.ui.cbReport.currentIndexChanged.connect( self.ui.actionGenerate.trigger )

        # Connect Now/Enddate widgets
        self.ui.chkNow.toggled.connect(lambda toggled: self.ui.dtEndDate.setDateTime( QDateTime.currentDateTime() ))
        self.ui.chkNow.toggled[bool].connect( self.ui.dtEndDate.setDisabled )
        self.ui.chkNow.toggled[bool].connect( self.ui.actionGenerate.trigger )
        self.ui.dtEndDate.editingFinished.connect( self.ui.actionGenerate.trigger )

        # RN usage params
        self.ui.chkWorking.toggled.connect( self.ui.actionGenerate.trigger )
        self.ui.chkPaused.toggled.connect( self.ui.actionGenerate.trigger )
        self.ui.chkOffline.toggled.connect( self.ui.actionGenerate.trigger )
        self.ui.chkIdle.toggled.connect( self.ui.actionGenerate.trigger )

        # Jobs params
        self.ui.cbTrack.currentIndexChanged.connect( self.ui.actionGenerate.trigger )
        self.ui.cbGroupBy.currentIndexChanged.connect( self.ui.actionGenerate.trigger )


        # Handle length slider and spinbox
        self.ui.slLength.sliderReleased.connect(self.ui.actionGenerate.trigger)
        self.ui.slLength.valueChanged.connect(self.ui.spLength.setValue)
        self.ui.spLength.valueChanged.connect(self.ui.slLength.setValue)
        self.ui.spLength.editingFinished.connect(self.ui.actionGenerate.trigger)
        self.ui.slLength.sliderReleased.connect(self.ui.actionGenerate.trigger)

        # Handle display options
        self.ui.slResolution.sliderReleased.connect(self.ui.actionGenerate.trigger)
        self.ui.slResolution.valueChanged.connect(self.ui.spResolution.setValue)
        self.ui.spResolution.valueChanged.connect(self.ui.slResolution.setValue)
        self.ui.spResolution.editingFinished.connect(self.ui.actionGenerate.trigger)
        self.ui.cbGraphStyle.currentIndexChanged.connect( self.ui.actionGenerate.trigger )
        self.ui.cbGraphType.currentIndexChanged.connect( self.ui.actionGenerate.trigger )
        self.ui.cbScaleType.currentIndexChanged.connect( self.ui.actionGenerate.trigger )
        self.ui.cbScaleRound.currentIndexChanged.connect( self.ui.actionGenerate.trigger )
        self.ui.spScaleResolution.editingFinished.connect(self.ui.actionGenerate.trigger)


        # Hide/Show report param
        self.updateParamsVisibility()

        self.currentCmd = self.reportDict[ str(self.ui.cbReport.currentText()) ]["cmd"]
        self.currentSource = self.reportDict[ str(self.ui.cbReport.currentText()) ]["source"]

        xlogger.debug("current command is: %s" % self.currentCmd)
        xlogger.debug("current source is: %s" % self.currentSource)

        # self.ui.webView.loadFinished.connect( self.initSvgFile )
        # self.ui.webView.settings().setAttribute( QWebSettings.JavascriptEnabled, True)
        # self.ui.webView.load(QUrl("rsrc/www/display.html"))

        self.ui.webView.load(QUrl(self.svgFile.name))
        self.generateGraph()

        # self.ui.webView.load(QUrl("/tmp/graph.svg"))
        # self.ui.webView.load(QUrl("/tmp/test.html"))

    # def initSvgFile(self):
    #     doc = self.ui.webView.page().mainFrame().documentElement()
    #     em = doc.findFirst("embed")
    #     em.setAttribute("src", self.svgFile.name)

    #     print "RESULT=%r" % self.ui.webView.page().mainFrame().toHtml()
    #     self.ui.webView.show()
    #     pass

    def updateParamsVisibility(self):

        if self.ui.cbReport.currentText() == "RN usage":
            self.ui.gbJobUsage.setVisible( False )
            self.ui.gbRnUsage.setVisible( True )
        else:
            self.ui.gbJobUsage.setVisible( True )
            self.ui.gbRnUsage.setVisible( False )

    def closeEvent(self, pEvent):
        """
        """
        xlogger.info("Removing temp file: %s"%self.svgFile.name)
        del(self.svgFile)
        pEvent.accept()


    def createBaseArgs(self):
        """
        """
        startDate = self.ui.dtEndDate.dateTime().addSecs( -1*self.ui.slLength.value()*3600 )
        endDate = self.ui.dtEndDate.dateTime()
        
        title = "%s - %s" % (startDate.toString("MM/dd hh:mm"), endDate.toString("MM/dd hh:mm"))
        startTime = startDate.toTime_t()
        endTime = endDate.toTime_t()

        args = []
        args += ["-t", title]
        args += ["--startTime", str(startTime)]
        args += ["--endTime", str(endTime)]
        args += ["-f", self.currentSource]
        args += ["-o", self.svgFile.name]
        args += ["--render-mode", "svg"]
        args += ["--scale", str(self.ui.spScaleResolution.value())]
        args += ["--scaleRound", str( int(self.ui.cbScaleRound.currentText()) * 60 )]

        #
        # Display params
        #
        args += ["--res", str(self.ui.slResolution.value())]
        args += ["--style", str(self.ui.cbGraphStyle.currentText())]
        args += ["--width", str(800)]
        args += ["--height", str(300)]
        
        if self.ui.cbGraphType.currentText() == "Stacked":
            args.append("--stack")
        if self.ui.cbScaleType.currentText() == "Logarithmic":
            args.append("--log")

        return args

    def createRnUsageArgs(self):
        """
        """
        args = []

        if not self.ui.chkOffline.isChecked():
            args.append("--hide-offline")
        if not self.ui.chkPaused.isChecked():
            args.append("--hide-paused")
        if not self.ui.chkWorking.isChecked():
            args.append("--hide-working")
        if not self.ui.chkIdle.isChecked():
            args.append("--hide-idle")

        return args

    def createQueueUsageArgs(self):
        """
        Add 2 fields to graph one value with a specific groupby attribute.
        Warning: the order of the params are important: groupby, value
        """
        args = []
        # args.append("prod")
        # args.append("jobs")

        args.append( self.queueGroupByParam[ str(self.ui.cbGroupBy.currentText()) ] )
        args.append( self.queueTrackParam[ str(self.ui.cbTrack.currentText()) ] )
        xlogger.info( "Tracking %s grouped by %s" % (self.queueTrackParam[ str(self.ui.cbTrack.currentText()) ],
                                                    self.queueGroupByParam[ str(self.ui.cbGroupBy.currentText()) ]) )
        return args

    def resetParams(self):
        """
        Set default values for all widget elements that might have been changed by user.
        Signals are blocked for actionGenerate during the reset, a call to grapheGenerate is done to refresh the view at the end.
        """

        xlogger.info( "Reset all params" )
        self.ui.actionGenerate.blockSignals(True)

        self.ui.cbReport.setCurrentIndex(0)
        self.ui.slLength.setValue(24)
        self.ui.chkNow.setChecked(True)

        self.ui.chkWorking.setChecked(True)
        self.ui.chkPaused.setChecked(True)
        self.ui.chkIdle.setChecked(True)
        self.ui.chkOffline.setChecked(True)

        self.ui.cbTrack.setCurrentIndex(1)
        self.ui.cbGroupBy.setCurrentIndex(0)

        self.ui.slResolution.setValue(30)
        self.ui.cbGraphType.setCurrentIndex(0)
        self.ui.cbScaleType.setCurrentIndex(0)
        self.ui.cbGraphStyle.setCurrentIndex(0)
        self.ui.cbScaleRound.setCurrentIndex(3)
        self.ui.spScaleResolution.setValue(20)

        self.ui.actionGenerate.blockSignals(False)
        self.generateGraph()

    def generateGraph(self):

        #
        # Disable GUI
        #
        self.ui.actionGenerate.setEnabled( False )


        #
        # Prepare process params
        #
        startDate = self.ui.dtEndDate.dateTime().addSecs( -1*self.ui.slLength.value()*3600 )
        endDate = self.ui.dtEndDate.dateTime()


        self.currentCmd = self.reportDict[ str(self.ui.cbReport.currentText()) ]["cmd"]
        self.currentSource = self.reportDict[ str(self.ui.cbReport.currentText()) ]["source"]
        
        args = self.createBaseArgs()
        if self.ui.cbReport.currentText() == "RN usage":
            args += self.createRnUsageArgs()
        else:
            args += self.createQueueUsageArgs()


        # Start thread
        self.p = QProcess(self)
        self.p.finished.connect( self.renderFinished )
        self.p.started.connect( self.renderStarted )


        xlogger.debug("starting command: %s %s" % (self.currentCmd, " ".join(args)) )
        res = self.p.start( self.currentCmd, args )
        if res==False:
            xlogger.info("Error starting subprocess: code=%s" % str(self.p.error()))

        self.ui.statusbar.showMessage("Graph update in progress...", 10000)
        self.ui.actionGenerate.setEnabled( False )
        

    def renderStarted(self):
        
        self.creationStartTime = time.time()
        QApplication.setOverrideCursor(Qt.WaitCursor)

        # Activate webview display
        # self.ui.webView.page().mainFrame().findFirstElement("#wait").setAttribute("class", "visible")
        # self.ui.webView.page().mainFrame().findFirstElement("#graph").setAttribute("class", "loading")


    def renderFinished(self, retCode):
        """
        Refresh result in webview
        """
        self.ui.actionGenerate.setEnabled( True )
        self.ui.statusbar.clearMessage()

        QApplication.restoreOverrideCursor()

        # # Activate webview display
        # self.ui.webView.page().mainFrame().findFirstElement("#wait").removeAttribute("class")
        # self.ui.webView.page().mainFrame().findFirstElement("#graph").removeAttribute("class")
        
        # svg = self.ui.webView.page().mainFrame().documentElement().findFirst("svg")
        # svg.setAttribute("style", "opacity: 1.0;")

        if retCode == 0:
            # # try to replace svg inline html
            # result = str(self.p.readAllStandardOutput())
            # svg = self.ui.webView.page().mainFrame().findFirstElement("#svg")
            # svg.setInnerXml(svgMarkup)
            # svg.replace(svgMarkup)
            
            self.ui.webView.reload()
            xlogger.info("graph updated in %.2fs" % (time.time() - self.creationStartTime ))
        else:
            xlogger.info("An error occured")

        # xlogger.debug("subprocess output=%s" % str(self.p.readAllStandardOutput()))



app = QApplication(sys.argv)

# Used to create/maintain application settings
app.setOrganizationName("Mikros")
app.setApplicationName("StatsViewer")
app.setWindowIcon( QIcon(StatsMainWindow.BASEDIR+"tools/statsviewer/rsrc/charts.png"))

#
# Define logs
# - user log streamed to a widget and stdout
# - dev log streamed to a stdout
xlogger = XLogger()

stats = StatsMainWindow()
stats.show()

app.exec_()