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


from tools.statsviewer.widget.mainwindow import Ui_MainWindow
from tools.common import XLogger, OutLog

import sys
import subprocess
import time
import logging
from logging import StreamHandler
from tempfile import NamedTemporaryFile
import codecs



class StatsMainWindow(QMainWindow):
    """
    """

    def __init__(self):
        QMainWindow.__init__(self)

        # Set up the user interface from Designer.
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.ui.actionGenerate.setIcon( QIcon("rsrc/refresh.png") )

        sys.stdout = OutLog( self.ui.log , sys.stdout)
        xlogger.info("Starting log")


        # Create tmp file for svg
        self.svgFile = NamedTemporaryFile( suffix=".svg", delete=True )
        xlogger.info("Creating temp file: %s"%self.svgFile.name)

        # Update end date value
        self.ui.dtEndDate.setDateTime( QDateTime.currentDateTime() )
        
        # # Connect up the buttons.
        self.ui.actionGenerate.triggered.connect(self.generateGraph)
        self.ui.chkNow.toggled.connect(lambda toggled: self.ui.dtEndDate.setDateTime( QDateTime.currentDateTime() ))


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

    def closeEvent(self, pEvent):
        """
        """
        xlogger.info("Removing temp file: %s"%self.svgFile.name)
        del(self.svgFile)
        pEvent.accept()


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

        prog = "/datas/jsa/OpenRenderManagement/Puli/scripts/util/update_usage_stats"
        sourceFile = "/s/apps/lin/vfx_test_apps/OpenRenderManagement/stats/FR/logs/usage_stats.log"
        title = "%s - %s" % (startDate.toString("MM/dd hh:mm"), endDate.toString("MM/dd hh:mm"))

        startTime = startDate.toTime_t()
        endTime = endDate.toTime_t()

        args = []
        args += ["-t", title]
        args += ["--startTime", str(startTime)]
        args += ["--endTime", str(endTime)]
        args += ["-f", sourceFile]
        # args += ["--render-mode", "inline"]

        args += ["-o", self.svgFile.name]
        args += ["--render-mode", "svg"]

        #
        # Optionnal params
        #
        args += ["--res", str(self.ui.slResolution.value())]
        args += ["--style", str(self.ui.cbGraphStyle.currentText())]
        
        if self.ui.cbGraphType.currentText() == "Stacked":
            args.append("--stack")
        if self.ui.cbScaleType.currentText() == "Logarithmic":
            args.append("--log")

        if not self.ui.chkOffline.isChecked():
            args.append("--hide-offline")
        if not self.ui.chkPaused.isChecked():
            args.append("--hide-paused")
        if not self.ui.chkWorking.isChecked():
            args.append("--hide-working")
        if not self.ui.chkIdle.isChecked():
            args.append("--hide-idle")


        # Start thread
        self.p = QProcess(self)
        self.p.finished.connect( self.renderFinished )
        self.p.started.connect( self.renderStarted )

        env = self.p.systemEnvironment()
        env.replaceInStrings("PYTHONPATH=", "PYTHONPATH=/usr/lib64/python2.6/site-packages/:")
        self.p.setEnvironment( env )

        # xlogger.debug("prog=%s" % prog)
        # xlogger.debug("args=%s" % " ".join(args) )
        res = self.p.start( prog, args )
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

        xlogger.debug("subprocess output=%s" % str(self.p.readAllStandardOutput()))



app = QApplication(sys.argv)

# Used to create/maintain application settings
app.setOrganizationName("Mikros")
app.setApplicationName("StatsViewer")
app.setWindowIcon( QIcon("rsrc/charts.png"))

#
# Define logs
# - user log streamed to a widget and stdout
# - dev log streamed to a stdout
xlogger = XLogger()

stats = StatsMainWindow()
stats.show()

app.exec_()