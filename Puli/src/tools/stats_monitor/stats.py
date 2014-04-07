from PyQt4.QtGui import QMainWindow
from PyQt4.QtGui import QApplication

from widget.mainwindow import Ui_MainWindow

import sys

class StatsMainWindow(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)

        # Set up the user interface from Designer.
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        # Make some local modifications.
        self.ui.cbReport.addItem("RN usage")
        # self.ui.cbReport.addItem("Jobs usage")

        self.ui.btnRefresh.setText("Generate")
        
        # Connect up the buttons.
        self.ui.btnRefresh.clicked.connect(self.generateGraph)
        # self.ui.cancelButton.clicked.connect(self.reject)

    def generateGraph(self):

        # Disable dedicated ui+cursor
        # Start thread
        # When thread end, refresh



        pass


app = QApplication(sys.argv)

# Used to create/maintain application settings
app.setOrganizationName("Mikros");
app.setApplicationName("StatsMonitor");

#
# Define logs
# - user log streamed to a widget and stdout
# - dev log streamed to a stdout

stats = StatsMainWindow()
stats.show()

app.exec_()