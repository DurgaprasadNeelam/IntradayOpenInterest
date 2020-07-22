import time as t
import sys
import pyqtgraph as pg
import Help as h
import DataThread as dt
from DataThread import log
from pyqtgraph.Qt import QtCore, QtGui


class TimeAxisItem(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        return [QtCore.QTime(0, 0, 0).addSecs(int(value)).toString("hh:mm:ss") for value in values]

class ApplicationWindow(QtCore.QObject):
    #Read thread signals
    expiryChngSgnl      = QtCore.pyqtSignal(str)
    timePeriodChngSgnl  = QtCore.pyqtSignal(int)
    otms2ShowSgnl       = QtCore.pyqtSignal('PyQt_PyObject')
    updateOtms2ShowSgnl = QtCore.pyqtSignal('PyQt_PyObject')
    readDataSgnl        = QtCore.pyqtSignal()

    #write thread signals 
    writeDataStateSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.currentPrice       = 0.0
        self.expiry             = ''
        self.otms2Read          = 2
        self.otmsToShow         = []
        self.graphs             = []
        self.init_data_thread   = None
        self.init_nifty_thread  = None
        self.write_thread       = None
        self.read_thread        = None 
        self.ShowNiftyOnOI      = True

    def InitDataAvailble(self, expiryList):
        self.expiry = expiryList[0]
        self.expirySelction.addItems(expiryList) 
        
        #Start pulling data only when we get expiry 
        self.write_thread = dt.DataWriteThread()  
        self.writeDataStateSgnl.connect(self.write_thread.SetWriteDataState)
        self.write_thread.start()

        self.read_thread.SetExpiry(self.expiry)
        self.read_thread.start()
        self.expirySelction.currentIndexChanged.connect(self.ExpirySelectionChanged)
        self.AddEmptyGraphs2View()

    def DeleteExistingGraphs(self):
        self.graphsWidget.clear()
        self.graphs.clear()

    def AddEmptyGraphs2View(self):
        self.otmsToShow.sort()
        for i in range(self.otms2Read):
            strk = str(self.otmsToShow[i])
            stringaxis = TimeAxisItem(orientation='bottom')
            plt = self.graphsWidget.addPlot(title="STRIKE - "+str(strk), axisItems={'bottom': stringaxis})
            plt.showGrid(x=True,y=True)
            plt.addLegend()
            item = plt.plot(name='CE')
            item.setObjectName(strk)
            self.graphs.append(item)
            item1 = plt.plot(name='PE')
            item1.setObjectName(str(strk)+'PE')
            self.graphs.append(item1)
            item2 = plt.plot(name='Nifty')
            item2.setObjectName(str(strk)+'Nifty')
            self.graphs.append(item2)

        self.graphsWidget.nextRow()    

        middle = int(len(self.otmsToShow)/2)
        strk = self.otmsToShow[middle]
        stringaxis = TimeAxisItem(orientation='bottom')
        plt = self.graphsWidget.addPlot(title="STRIKE - "+str(strk), axisItems={'bottom': stringaxis})
        plt.showGrid(x=True,y=True)
        plt.addLegend()
        item = plt.plot(name='CE')
        item.setObjectName(str(strk))
        self.graphs.append(item)
        item1 = plt.plot(name='PE')
        item1.setObjectName(str(strk)+'PE')
        self.graphs.append(item1)
        item2 = plt.plot(name='Nifty')
        item2.setObjectName(str(strk)+'Nifty')
        self.graphs.append(item2)

        stringaxis = TimeAxisItem(orientation='bottom')
        plt = self.graphsWidget.addPlot(title='NIFTY', axisItems={'bottom': stringaxis})
        plt.showGrid(x=True,y=True)
        plt.addLegend()
        item = plt.plot(name='Nifty')
        item.setObjectName("Nifty")
        self.graphs.append(item)

        self.graphsWidget.nextRow()
        
        for i in range(self.otms2Read):
            strk = str(self.otmsToShow[middle+1+i])
            stringaxis = TimeAxisItem(orientation='bottom')
            plt = self.graphsWidget.addPlot(title="STRIKE - "+str(strk), axisItems={'bottom': stringaxis})
            plt.showGrid(x=True,y=True)
            plt.addLegend()
            item = plt.plot(name='CE')
            item.setObjectName(str(strk))
            self.graphs.append(item)
            item1 = plt.plot(name='PE')
            item1.setObjectName(str(strk)+'PE')
            self.graphs.append(item1)
            item2 = plt.plot(name='Nifty')
            item2.setObjectName(str(strk)+'Nifty')
            self.graphs.append(item2)

    def NiftyDataAvailable(self, niftyPrice):
        self.read_thread = dt.DataReadThread(self.expiry)
        self.read_thread.ceSignal.connect(self.CEOIDataAvailable)
        self.read_thread.peSignal.connect(self.PEOIDataAvailable)
        self.read_thread.niftySignal.connect(self.NiftyPriceAvailable)
        self.expiryChngSgnl.connect(self.read_thread.SetExpiry)
        self.timePeriodChngSgnl.connect(self.read_thread.SetTimePeriod)
        self.otms2ShowSgnl.connect(self.read_thread.SetOtmsToShow)
        self.updateOtms2ShowSgnl.connect(self.read_thread.UpdateOtmsToShow)
        self.readDataSgnl.connect(self.read_thread.ReadData)

        self.niftyPrice.setText(str(niftyPrice))
        self.currentPrice = int(niftyPrice)
        self.CalAtmOtmRangeToRead()

        if self.init_data_thread is None:
            self.init_data_thread = dt.InitDataThread()  
            self.init_data_thread.signal.connect(self.InitDataAvailble)
            self.init_data_thread.start()

    def ExpirySelectionChanged(self):
        self.expiry = self.expirySelction.currentText()
        if None != self.read_thread:
            self.expiryChngSgnl.emit(self.expiry)
        
    def TimePeriodChanged(self):
        period = self.timePeriod.currentText()
        if None != self.read_thread:
            self.timePeriodChngSgnl.emit(int(period))

    def ReadDuringMarketHrs(self):
        state = self.marketHrs.isChecked()
        if None != self.write_thread and None != self.init_nifty_thread:
            self.writeDataStateSgnl.emit(state)

    def ShowNiftyChartOnOIChart(self):
        self.ShowNiftyOnOI = self.niftyChrtChkBx.isChecked()
        if None != self.read_thread:
            self.readDataSgnl.emit()

    def OtmstoShowChanged(self):
        self.otms2Read = int(self.otmsToShowSpn.currentText())
        self.CalAtmOtmRangeToRead()
        self.DeleteExistingGraphs()
        self.AddEmptyGraphs2View()

        if None != self.read_thread:
            self.otms2ShowSgnl.emit(self.otmsToShow)

    def CalAtmOtmRangeToRead(self):
        atm = h.getATM(self.currentPrice)

        self.otmsToShow.clear()
        self.otmsToShow = [atm]
        for i in range(self.otms2Read):
            self.otmsToShow.append(atm + (i+1)*50)
            self.otmsToShow.insert(0, atm - (i+1)*50)

        if None != self.read_thread:
            self.updateOtms2ShowSgnl.emit(self.otmsToShow)

    def TimeStamp(self, t):
        tt = QtCore.QTime(0, 0, 0).secsTo(QtCore.QTime.fromString(t, "hh:mm"))    
        return tt   

    def CEOIDataAvailable(self, ceData):
        strike   = ceData['Strike']
        time_x   = ceData['Time']
        ce_oi_y  = ceData['OI']
        niftyPrc = ceData['Nifty']

        if [] == strike or [] == time_x or [] == ce_oi_y or [] == niftyPrc:
            return

        for plot in self.graphs:
            if plot.objectName() == str(strike):
                plot.setData(x = [self.TimeStamp(time) for time in time_x], y = ce_oi_y, pen='r')
            elif plot.objectName() == str(strike)+"Nifty":
                if True == self.ShowNiftyOnOI:
                    plot.setData(x = [self.TimeStamp(time) for time in time_x], y = niftyPrc, pen='y')
                else: 
                     plot.setData(x = [], y = [], pen='r')
                     
            ######## check plot.hide()


    def PEOIDataAvailable(self, peData):
        strike  = peData['Strike']
        time_x  = peData['Time']
        pe_oi_y = peData['OI']

        if [] == strike or [] == time_x or [] == pe_oi_y:
            return

        for plot in self.graphs:
            if plot.objectName() == str(strike)+"PE":
                plot.setData(x = [self.TimeStamp(time) for time in time_x], y = pe_oi_y, pen='g')

    def NiftyPriceAvailable(self, niftyData):
        price   = niftyData['Price']
        time_x  = niftyData['Time']

        if [] == price or [] == time_x:
            return

        for plot in self.graphs:
            if plot.objectName() == "Nifty":
                plot.setData(x = [self.TimeStamp(time) for time in time_x], y = price, pen='g')

    def GetApplicationWindow(self):
        self.mainWindow = QtGui.QWidget()
        self.mainLayout = QtGui.QGridLayout()
        self.mainWindow.setLayout(self.mainLayout)

        self.optionsWindow = QtGui.QWidget()
        self.optionsLayout = QtGui.QGridLayout()

        self.optionsLayout.addWidget(QtGui.QLabel('Nifty'), 0,0,1,1)
        self.niftyPrice = QtGui.QLabel()
        self.niftyPrice.setStyleSheet("color: green;font-size:20px;margin-top:8px;margin-bottom:8px")
        self.optionsLayout.addWidget(self.niftyPrice, 0,1,1,1)

        self.optionsLayout.addWidget(QtGui.QLabel('Expiry'), 1,0,1,1)
        self.expirySelction = QtGui.QComboBox()
        self.expirySelction.setStyleSheet("margin-bottom:8px")
        self.optionsLayout.addWidget(self.expirySelction, 1,1,1,1)
        self.optionsWindow.setLayout(self.optionsLayout)
        
        self.optionsLayout.addWidget(QtGui.QLabel('OTMsToShow'), 2,0,1,1)
        self.otmsToShowSpn = QtGui.QComboBox()
        self.otmsToShowSpn.setStyleSheet("margin-bottom:8px")
        self.otmsToShowSpn.addItems(['1', '2', '3', '4', '5'])
        self.otmsToShowSpn.setCurrentIndex(1)
        self.otms2Read = 2
        self.otmsToShowSpn.currentIndexChanged.connect(self.OtmstoShowChanged)
        self.optionsLayout.addWidget(self.otmsToShowSpn, 2,1,1,1)

        self.optionsLayout.addWidget(QtGui.QLabel('Period (min)'), 3,0,1,1)
        self.timePeriod = QtGui.QComboBox()
        self.timePeriod.setStyleSheet("margin-bottom:8px")
        self.timePeriod.addItems(['1', '2', '5', '10', '15', '30', '60'])
        self.timePeriod.currentIndexChanged.connect(self.TimePeriodChanged)
        self.optionsLayout.addWidget(self.timePeriod, 3,1,1,1)

        self.niftyChrtChkBx = QtGui.QCheckBox('Show Nifty Chart')
        self.optionsLayout.addWidget(self.niftyChrtChkBx, 4,0,1,2)
        self.niftyChrtChkBx.setStyleSheet("margin-bottom:8px")
        self.niftyChrtChkBx.setChecked(True)
        self.niftyChrtChkBx.stateChanged.connect(self.ShowNiftyChartOnOIChart)

        self.marketHrs = QtGui.QCheckBox('Read Only During Market Hrs')
        self.optionsLayout.addWidget(self.marketHrs, 5,0,1,2)
        self.marketHrs.setStyleSheet("margin-bottom:8px")
        self.marketHrs.setChecked(True)
        self.marketHrs.stateChanged.connect(self.ReadDuringMarketHrs)
        
        verticalSpacer = QtGui.QSpacerItem(20, 40, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.optionsLayout.addItem(verticalSpacer)

        self.graphsWidget = pg.GraphicsLayoutWidget()
        self.mainLayout.addWidget(self.optionsWindow , 0, 0, 1, 1)
        self.mainLayout.addWidget(self.graphsWidget, 0, 1, 1, 1)

        return self.mainWindow

def main():
    app = QtGui.QApplication(sys.argv)
    appWin = ApplicationWindow()
    mainWindow = appWin.GetApplicationWindow()
    mainWindow.showMaximized()

    appWin.init_nifty_thread = dt.NiftyPriceThread()  
    appWin.init_nifty_thread.signal.connect(appWin.NiftyDataAvailable)
    appWin.writeDataStateSgnl.connect(appWin.init_nifty_thread.SetWriteDataState)
    appWin.init_nifty_thread.start()

    app.exec_()

if __name__ == '__main__':
    main()