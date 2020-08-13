from pyqtgraph.Qt import QtCore, QtGui
import pyqtgraph as pg
import ActiveContractsReader as ACT
import ExpiryListReader as ELRT
import NiftyHeavyWeightsWriter as NHWWT
import NiftyLivePriceReader as NLPRT
import NiftyPriceWriter as NPWT
import OIDataReaderWriter as ODRWT

import sys
import time as t
import Help as h
import copy
import random
import datetime
from Help import log

class TimeAxisItem(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        return [QtCore.QTime(0, 0, 0).addSecs(int(value)).toString("hh:mm:ss") for value in values]

class ApplicationWindow(QtCore.QObject):
    expiryChngSgnl       = QtCore.pyqtSignal(str)
    timePeriodChngSgnl   = QtCore.pyqtSignal(int)
    readDataSgnl         = QtCore.pyqtSignal()
    otms2ShowSgnl        = QtCore.pyqtSignal('PyQt_PyObject')
    updateOtms2ShowSgnl  = QtCore.pyqtSignal('PyQt_PyObject')
    writeDataStateSgnl   = QtCore.pyqtSignal('PyQt_PyObject')
    heaveyWeightsChdSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QObject.__init__(self)
        self.currentPrice       = None
        self.expiry             = ''
        self.otms2Read          = 2
        self.otmsToShow         = []
        self.graphs             = []
        self.plotsDict          = {}
        self.init_data_thread   = None
        self.init_nifty_thread  = None
        self.write_thread       = None
        self.read_thread        = None 
        self.ShowNiftyOnOI      = True

    #Need expiry list to read/write data from/to DB
    #once expiry list available then only start OI writer and reader threads 
    def InitDataAvailble(self, expiryList):
        if None == self.write_thread:
            log.log("InitDataAvailble, expiry list = " + str(expiryList))
            self.expiry = expiryList[0]
            self.expirySelction.addItems(expiryList) 
            
            #Start pulling data from NSE & writes to DB 
            self.write_thread = ODRWT.OIDataWriterThread()  
            self.writeDataStateSgnl.connect(self.write_thread.SetWriteDataState)
            self.write_thread.SetExpiryList(expiryList)
            self.write_thread.start()

            #Start reader thread only when expiry list available 
            self.read_thread.SetExpiry(self.expiry)
            self.read_thread.start()
            self.expirySelction.currentIndexChanged.connect(self.ExpirySelectionChanged)
            self.AddEmptyGraphs2View()

    #Delete existing all graphs & references
    def DeleteExistingGraphs(self):        
        for vw in self.plotsDict.keys():
            vw.clear() 
        
        self.graphsWidget.clear()
        self.graphs.clear()
        self.plotsDict.clear()

    #special case for drawing PE volume 
    #use existing CE volume axis to plot PE volume chart
    def PlotVolumeToExistingAxisChart(self, plt, uniqueID, plotOnID):
        for vb in plt.scene().items():
            if isinstance(vb, type(pg.ViewBox())) and vb.objectName() == plotOnID:
                item2 = None
                if uniqueID.find('CE') != -1:
                    item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.5, name='Volume', brush='g', pen='g')
                elif uniqueID.find('PE') != -1:
                    item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.3, name='Volume', brush='r', pen='r')
                else:
                    item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.3, name='Volume', brush='b')

                item2.setOpacity(0.5)
                item2.setObjectName(uniqueID)
                vb.addItem(item2)
                self.graphs.append(item2)
                plt.vb.sigResized.connect(self.updateViews)
                break

    def PlotVolumeChart(self, plt, uniqueID):
        p3 = pg.ViewBox()
        p3.setObjectName(uniqueID)
        ax3 = pg.AxisItem('right')
        plt.layout.addItem(ax3, 2,3)
        plt.scene().addItem(p3)
        ax3.linkToView(p3)
        p3.setXLink(plt)
        ax3.setZValue(-100000)
        

        if uniqueID.find('CE') != -1 or uniqueID.find('PE') != -1:
            ax3.setLabel('Volume', color='blue')
        else:
            ax3.setLabel('Volume', color='yellow')

        item2 = None
        if uniqueID.find('CE') != -1:
            item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.9, name='Volume', brush='b', pen='b')
        elif uniqueID.find('PE') != -1:
            item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.3, name='Volume', brush='r', pen='r')
        else:
            item2 = pg.BarGraphItem(x=range(2), height=[0,0], width=0.5, name='Volume', brush='g', pen='y')

        item2.setOpacity(0.5)
        item2.setObjectName(uniqueID)
        p3.addItem(item2)
        self.plotsDict[p3] = plt
        self.graphs.append(item2)
        plt.vb.sigResized.connect(self.updateViews)

    #Draw EC, PE OI data & plot nifty chart as well
    def PlotOIGraph(self, index):
        #plot graph for strike
        strk = str(self.otmsToShow[index])
        stringaxis = TimeAxisItem(orientation='bottom')
        plt = self.graphsWidget.addPlot(title="STRIKE - "+str(strk), axisItems={'bottom': stringaxis})
        plt.setObjectName("STRIKE - "+str(strk))
        plt.showGrid(x=True,y=True)
        plt.addLegend()
        item = plt.plot(name='CE')
        item.setObjectName(strk)
        self.graphs.append(item)
        item1 = plt.plot(name='PE')
        item1.setObjectName(str(strk)+'PE')
        self.graphs.append(item1)   #Save items to update/SetData 

        #plot Nifty time on same x-axis but use right side y-axis for price 
        plt.setLabels(left='OI')
        plt.showAxis('right')
        p2 = pg.ViewBox()
        plt.scene().addItem(p2)
        plt.getAxis('right').linkToView(p2)
        p2.setXLink(plt)
        plt.getAxis('right').setLabel('Nifty', color='green')            
        self.plotsDict[p2] = plt
        p2.setGeometry(plt.vb.sceneBoundingRect())
        p2.linkedViewChanged(plt.vb, p2.XAxis)

        item2 =  pg.PlotCurveItem(name='Nifty')
        item2.setObjectName(str(strk)+'Nifty')
        p2.addItem(item2)
        self.graphs.append(item2)    #Save items to update/SetData
        plt.vb.sigResized.connect(self.updateViews)

        #initially plot CE volume with 2 dummy values
        self.PlotVolumeChart(plt, strk+"CE"+"Volume")
        self.PlotVolumeToExistingAxisChart(plt, strk+"PE"+"Volume", strk+"CE"+"Volume")

    #plot nifty chart & nifty heavy weigts chart
    def PlotNiftyAndHeavyWeights(self):
        #Plot Nifty char
        #strk = str(self.otmsToShow[middle])
        stringaxis = TimeAxisItem(orientation='bottom')
        plt = self.graphsWidget.addPlot(title='NIFTY', axisItems={'bottom': stringaxis})

        #plt.setObjectName("STRIKE - "+str(strk))
        plt.showGrid(x=True,y=True)
        plt.addLegend()
        item1 = plt.plot(name='Nifty')
        item1.setObjectName("Nifty")
        self.graphs.append(item1)
        
        #Plot heavy weights time on same x-asix but use right side y-axis for price 
        plt.setLabels(left='Nifty')
        plt.showAxis('right')
        p2 = pg.ViewBox()
        plt.scene().addItem(p2)
        plt.getAxis('right').linkToView(p2)
        p2.setXLink(plt)
        plt.getAxis('right').setLabel('Nifty - Heavy Weights', color='yellow')
        self.plotsDict[p2] = plt
        p2.setGeometry(plt.vb.sceneBoundingRect())
        p2.linkedViewChanged(plt.vb, p2.XAxis)

        item =  pg.PlotCurveItem(name='NiftyHeavyWights Movement')
        item.setObjectName("NiftyHeavy")
        p2.addItem(item)
        self.graphs.append(item)
        plt.vb.sigResized.connect(self.updateViews)

        #initially plot volume with 2 dummy values
        self.PlotVolumeChart(plt, "HeavyWeightsVolume")

    #draw all the graphs on graphics widget
    def AddEmptyGraphs2View(self):
        self.otmsToShow.sort()
        
        #draw first row OI grapgs
        for i in range(self.otms2Read):
            self.PlotOIGraph(i)
        self.graphsWidget.nextRow()    

        #draw middle line graphs 
        middle = int(len(self.otmsToShow)/2)
        self.PlotOIGraph(middle)
        self.PlotNiftyAndHeavyWeights()
        self.graphsWidget.nextRow()
        
        #draw 3rd/last line graphs
        for i in range(self.otms2Read):
            self.PlotOIGraph(middle+1+i)

        self.updateViews()

    #Slot - update geometry on windows resizing
    def updateViews(self):
        for key in self.plotsDict.keys():
            p2 = key
            p1 = self.plotsDict[key]
            p2.setGeometry(p1.vb.sceneBoundingRect()) 
            p2.linkedViewChanged(p1.vb, p2.XAxis)

    def LiveNiftyPrice(self, livePrice):
        #log.log('Live price = ' + str(livePrice))
        self.niftyPrice.setText(str(livePrice))

        #sometimes nifty data read thread stucks
        if self.currentPrice == None:
            self.NiftyDataAvailable(livePrice)

    #nifty price available 1st time, calculte ATM & OTMS and start dependent threads         
    def NiftyDataAvailable(self, niftyPrice):
        self.currentPrice = int(niftyPrice)
        self.CalAtmOtmRangeToRead()

        #Start nifty price, ATM & OTMS dependent threads 
        if self.init_data_thread is None:
            log.log("Nifty data vailable : " + str(niftyPrice))
            self.read_thread = ODRWT.OIDataReaderThread(self.expiry, self.heavyWeightsList)
            self.read_thread.ceSignal.connect(self.CEOIDataAvailable)
            self.read_thread.peSignal.connect(self.PEOIDataAvailable)
            self.read_thread.niftySignal.connect(self.NiftyPriceAvailable)
            self.read_thread.niftyHeavyWeightsSgnl.connect(self.HeavyWeightsDataAvailable)

            self.read_thread.UpdateOtmsToShow(self.otmsToShow)
            
            self.expiryChngSgnl.connect(self.read_thread.SetExpiry)
            self.timePeriodChngSgnl.connect(self.read_thread.SetTimePeriod)
            self.otms2ShowSgnl.connect(self.read_thread.SetOtmsToShow)
            self.updateOtms2ShowSgnl.connect(self.read_thread.UpdateOtmsToShow)
            self.readDataSgnl.connect(self.read_thread.ReadData)
            self.heaveyWeightsChdSgnl.connect(self.read_thread.UpdateHeavyWeightsList)

            #launch 1+3 times & let any thread fetch the data for us
            #some times single thread taking log time to get list or stuck
            self.init_data_thread = ELRT.ExpiryListReaderThread()  
            self.init_data_thread.signal.connect(self.InitDataAvailble)
            self.init_data_thread.start()
            
            self.temp_data_thread = []
            for i in range(3):
                self.temp_data_thread.append(ELRT.ExpiryListReaderThread())
            for i in range(3):
                self.temp_data_thread[i].signal.connect(self.InitDataAvailble)
                self.temp_data_thread[i].start()
                        
    #when expiry selection changed from ui, re-draw everything                    
    def ExpirySelectionChanged(self):
        self.expiry = self.expirySelction.currentText()
        log.log("ExpirySelectionChanged : " + self.expiry)
        if None != self.read_thread:
            self.expiryChngSgnl.emit(self.expiry)
    
    #when time period changed from ui, re-draw everything
    def TimePeriodChanged(self):
        period = self.timePeriod.currentText()
        log.log("TimePeriodChanged : " + period)
        if None != self.read_thread:
            self.timePeriodChngSgnl.emit(int(period))

    #Data to be read (& write to DB) only during market hrs
    def ReadDuringMarketHrs(self):        
        state = self.marketHrs.isChecked()
        log.log("readDuringMarketHrs : " + str(state))
        if None != self.write_thread and None != self.init_nifty_thread:
            self.writeDataStateSgnl.emit(state)

    #show or hide nifty chart along with OI chart 
    def ShowNiftyChartOnOIChart(self):
        self.ShowNiftyOnOI = self.niftyChrtChkBx.isChecked()
        log.log("ShowNiftyChartOnOIChart : " + str(self.ShowNiftyOnOI))
        if None != self.read_thread:
            self.readDataSgnl.emit()

    #How many OTMS to show on graph
    def OtmstoShowChanged(self):
        self.otms2Read = int(self.otmsToShowSpn.currentText())
        self.CalAtmOtmRangeToRead()
        self.DeleteExistingGraphs()
        self.AddEmptyGraphs2View()
        log.log("OtmstoShowChanged : " + str(self.otms2Read))

        if None != self.read_thread:
            self.otms2ShowSgnl.emit(self.otmsToShow)

    #calculte ATM, OTMS based on nifty current price 
    #as nifty price keeps on changes ATM will keep on change
    def CalAtmOtmRangeToRead(self):
        atm = h.getATM(self.currentPrice)

        self.otmsToShow.clear()
        self.otmsToShow = [atm]
        for i in range(self.otms2Read):
            self.otmsToShow.append(atm + (i+1)*50)
            self.otmsToShow.insert(0, atm - (i+1)*50)

        if None != self.read_thread:
            self.updateOtms2ShowSgnl.emit(self.otmsToShow)

    #converts DB time to time to plot x-axis
    def TimeStamp(self, t):
        tt = QtCore.QTime(0, 0, 0).secsTo(QtCore.QTime.fromString(t, "hh:mm"))    
        return tt   

    #Reader thread reads OI data from DB every 1 min & post 
    def CEOIDataAvailable(self, ceData):
        try:
            strike   = ceData['Strike']
            time_x   = ceData['Time']
            ce_oi_y  = ceData['OI']
            niftyPrc = ceData['Nifty']
            totVolume= ceData['Volume']

            if [] == strike or [] == time_x or [] == ce_oi_y or [] == niftyPrc:
                return

            #Update existing CE data & Nifty graphs with SetData interface 
            for plot in self.graphs:
                if plot.objectName() == str(strike):
                    plot.setData(x = [self.TimeStamp(time) for time in time_x], y = ce_oi_y, pen='r')
                    self.UpdateVolumeChart(time_x, totVolume, str(strike)+"CE"+"Volume" )
                elif plot.objectName() == str(strike)+"Nifty":
                    if True == self.ShowNiftyOnOI:
                        pen = pg.mkPen(color=(0, 255, 0), width=1, style=QtCore.Qt.DotLine)
                        plot.setData(x = [self.TimeStamp(time) for time in time_x], y = niftyPrc, pen=pen)
                    else: 
                        plot.setData(x = [h.getTime()], y = [0])
                        plot.hide()
        except:
            log.logException("Exception in CEOIDataAvailable")

    #reader threads post PE data every 1 min
    def PEOIDataAvailable(self, peData):
        try:
            strike  = peData['Strike']
            time_x  = peData['Time']
            pe_oi_y = peData['OI']
            totVolume= peData['Volume']

            if [] == strike or [] == time_x or [] == pe_oi_y:
                return

            #Update PE graphs with SetData
            for plot in self.graphs:
                if plot.objectName() == str(strike)+"PE":
                    plot.setData(x = [self.TimeStamp(time) for time in time_x], y = pe_oi_y, pen='b')
                    self.UpdateVolumeChart(time_x, totVolume, str(strike)+"PE"+"Volume" )
        except:
            log.logException("Exception in PEOIDataAvailable")

    #Draw Nifty chart in middle row along with Heavy weights chart (Not in CE/PE OI chart)
    def NiftyPriceAvailable(self, niftyData):
        try:
            price   = niftyData['Price']
            time_x  = niftyData['Time']

            if [] == price or [] == time_x:
                return

            for plot in self.graphs:
                if plot.objectName() == "Nifty":
                    pen = pg.mkPen(color=(0, 255, 0), width=1, style=QtCore.Qt.DotLine)
                    plot.setData(x = [self.TimeStamp(time) for time in time_x], y = price, pen=pen)
        except:
            log.logException("Exception in NiftyPriceAvailable")

    #Nifty heavy weights data read from DB & will be availble for every 1 min
    def HeavyWeightsDataAvailable(self, niftyHeavyWightsData):        
        if [] == self.graphs:
            return
            
        try:
            heavyWeightsTimePrice = {}
            heavyWeightsTimeVol = {}

            for record in niftyHeavyWightsData:
                time = record[0]
                ltp  = record[2]
                vol  = record[3]

                if time in heavyWeightsTimePrice:
                    heavyWeightsTimePrice[time] = heavyWeightsTimePrice[time] + ltp 
                    heavyWeightsTimeVol[time] = heavyWeightsTimeVol[time] + vol
                else:
                    heavyWeightsTimePrice[time] = ltp  
                    heavyWeightsTimeVol[time] = vol    

            #Heavy weights chart
            price   = [] 
            time_x  = []

            for pr in heavyWeightsTimePrice.values():
                price.append(pr)
            for tm in heavyWeightsTimePrice.keys():
                time_x.append(tm)

            for plot in self.graphs:
                if plot.objectName() == "NiftyHeavy":
                    plot.setData(x = [self.TimeStamp(time) for time in time_x], y = price, pen='y')

            #Heavy weights volume chart
            self.UpdateVolumeChart(time_x, heavyWeightsTimeVol.values(), "HeavyWeightsVolume")   
        except:
            log.logException("Exception in HeavyWeightsDataAvailable")

    def UpdateVolumeChart(self, time_x_axis, VolumeList, uniqueID):
        try:
            time_x = copy.deepcopy(time_x_axis)
            if len(time_x) < 2:
                return

            volume  = [] 
            time_x.pop(0)

            volPrv = None
            for vl in VolumeList:
                if None != volPrv:
                    volume.append(vl-volPrv) 
                volPrv = vl
            
            volume_y = volume

            '''
            volume_y = []
            k = random.sample([1000, 1500], 1)
            if uniqueID.find('CE') != -1:
                volume_y = [ x + k[0] for x in volume  ]
            else:
                volume_y = [ x + k[0] for x in volume  ]
            '''

            '''
            log.log("--------------  " + uniqueID + "  ----------------------")
            log.log(time_x)
            log.log(volume)
            log.log(VolumeList)
            log.log("--------------------------------------------------------------")
            '''

            for plot in self.graphs:
                if plot.objectName() == uniqueID:
                    plot.setOpts(x = [self.TimeStamp(time) for time in time_x], height=volume_y, width=0.5)

                    if uniqueID.find('CE') != -1:
                        plot.setZValue(5)
                    else:
                        plot.setZValue(4)

        except:
            log.logException("Exception while plotting volume cart for : " + uniqueID)

    #Predefined heavey weights can be checked (removed/added)
    def heaveyWeightsChanged(self):
        self.heavyWeightsList.clear()
        heavyWightsWdg = self.grpBox.children()
        for chld in heavyWightsWdg:
            if str(type(QtGui.QCheckBox())) == str(type(chld)):
                if chld.isChecked():
                    self.heavyWeightsList.append(chld.text())
        
        self.heaveyWeightsChdSgnl.emit(self.heavyWeightsList)
        log.log("Heavy weights changed : " + str(self.heavyWeightsList))

    #Ui setup
    def GetApplicationWindow(self):
        self.mainWindow = QtGui.QWidget()
        self.mainLayout = QtGui.QGridLayout()
        self.mainWindow.setLayout(self.mainLayout)

        self.optionsWindow = QtGui.QWidget()
        self.optionsLayout = QtGui.QGridLayout()

        self.niftyLbl = QtGui.QLabel('Nifty')
        self.niftyLbl.setStyleSheet("color:blue;font-weight:bold;font-size:15px")
        self.optionsLayout.addWidget(self.niftyLbl, 0,0,1,1)
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

        self.activeCLable = QtGui.QLabel("Nifty Active Contracts")
        self.optionsLayout.addWidget(self.activeCLable, 6,0,1,2)
        self.activeCLable.setStyleSheet("margin-top:20px;color:blue;font-size:20px;font-weight:bold")

        self.activeContactsWidget = QtGui.QTableWidget()
        self.optionsLayout.addWidget(self.activeContactsWidget, 7,0,1,2)
        
        self.heavyWeightsLable = QtGui.QLabel("Nifty Heavy Weigts")
        self.optionsLayout.addWidget(self.heavyWeightsLable, 8,0,1,2)
        self.heavyWeightsLable.setStyleSheet("margin-top:20px;color:blue;font-size:20px;font-weight:bold")
        self.grpBox = QtGui.QGroupBox()
        self.optionsLayout.addWidget(self.grpBox, 9,0,1,2)
        self.grpBxLayout = QtGui.QGridLayout()
        self.grpBox.setLayout(self.grpBxLayout)

        #show 2 items/stocks on each row in group box
        self.heavyWeightsList = ['RELIANCE', 'HDFCBANK', 'HDFC', 'ICICIBANK', 'INFY', 'TCS', 'HINDUNILVR', 'ITC']
        r = 0
        c = 0
        for syb in self.heavyWeightsList:
            self.stkWd = QtGui.QCheckBox(syb)
            self.grpBxLayout.addWidget(self.stkWd, r, c, 1, 1)
            self.stkWd.setChecked(True)
            self.stkWd.stateChanged.connect(self.heaveyWeightsChanged)
            c = c+1
            if 2 == c:
                c = 0
                r = r+1

        #to push things up or align properly
        verticalSpacer = QtGui.QSpacerItem(20, 40, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.optionsLayout.addItem(verticalSpacer)

        #Graphics widget to plot all graphs
        self.graphsWidget = pg.GraphicsLayoutWidget()
        self.mainLayout.addWidget(self.optionsWindow , 0, 0, 1, 1)
        self.mainLayout.addWidget(self.graphsWidget, 0, 1, 1, 1)

        return self.mainWindow

    #Active contacts data is available every min
    def activeContactDataAvailable(self, tableData):
        self.activeContactsWidget.clear()

        try:
            hLabels = ["STRIKE", "TYPE", "%Chng", "OI", "LTP"]
            rowCnt = len(tableData)
            colCnt = len(hLabels)
            self.activeContactsWidget.setRowCount(rowCnt)
            self.activeContactsWidget.setColumnCount(colCnt)
            self.activeContactsWidget.verticalHeader().setVisible(False)
            self.activeContactsWidget.horizontalHeader().setVisible(True)
            self.activeContactsWidget.horizontalHeader().setStyleSheet("font-weight:bold;border:1px")
            self.activeContactsWidget.setHorizontalHeaderLabels(hLabels)
            self.activeContactsWidget.setSortingEnabled(False)

            for r in range(rowCnt):
                for c in range(colCnt): 
                    self.activeContactsWidget.setItem(r,c, QtGui.QTableWidgetItem(str(tableData[r][c])))   
            
            self.activeContactsWidget.resizeColumnsToContents()
            self.activeContactsWidget.setSortingEnabled(True)
            self.activeContactsWidget.sortItems(1, QtCore.Qt.AscendingOrder)
        except:
            log.logException("Exception in activeContactDataAvailable")

def main():
    log.logHeader("------------------------------------------------------------------------------------------")
    log.logHeader("           " + h.getDate() + " " + h.getTime() +"  Day Open Interest")
    log.logHeader("------------------------------------------------------------------------------------------")
    log.changeBaseConfig()
    
    #launch UI
    app = QtGui.QApplication(sys.argv)
    appWin = ApplicationWindow()
    mainWindow = appWin.GetApplicationWindow()
    mainWindow.showMaximized()
    mainWindow.setWindowTitle("Intraday Application")

    #Launch independent threads
    #Reads nifty live price and update UI
    appWin.nifty_live_price_thread = NLPRT.NiftyLivePriceReaderThread()  
    appWin.nifty_live_price_thread.signal.connect(appWin.LiveNiftyPrice)
    appWin.nifty_live_price_thread.start()

    #Reads nifty price every 1 min & writes to DB DB, calculte ATM & OTMS based nifty current price 
    appWin.init_nifty_thread = NPWT.NiftyPriceWriterThread()  
    appWin.init_nifty_thread.signal.connect(appWin.NiftyDataAvailable)
    appWin.writeDataStateSgnl.connect(appWin.init_nifty_thread.SetWriteDataState)
    appWin.init_nifty_thread.start()

    #Active contacts will be shown in UI
    active_contracts_thread = ACT.ActiveContractsReaderThread()
    active_contracts_thread.activeContractsSgnl.connect(appWin.activeContactDataAvailable)
    active_contracts_thread.start()

    #Reads nifty Heavy weights data & writes to DB
    appWin.nifty_data_write_thread = NHWWT.NiftyHeavyWeightsWriterThread(appWin.heavyWeightsList)
    appWin.writeDataStateSgnl.connect(appWin.nifty_data_write_thread.SetWriteDataState)
    appWin.nifty_data_write_thread.start()

    app.exec_()

if __name__ == '__main__':
    main()