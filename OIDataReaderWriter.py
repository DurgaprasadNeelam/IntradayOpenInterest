from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t
import sqlite3

class OIDataWriterThread(QtCore.QThread):
    def __init__(self):
        QtCore.QThread.__init__(self)
        log.log("OIDataWriterThread Started...")
        self.timer = 60
        self.todaydate = str(h.getDate())
        self.writeDuringMrktHrs = True
        self.expiryDateList = []
        self.createDB()
        self.DeleteOldData()
    
    def __del__(self): 
        log.log("OIDataWriterThread - exited") 

    def SetExpiryList(self, exList):
        self.expiryDateList = exList

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs = state
        log.log("OIDataWriterThread - setWriteDataState : " + str(state) )

    def DeleteOldData(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("DELETE from OptionChain WHERE Date != '"+ self.todaydate + "'")
            self.conn.commit()
            self.c.close()
            self.conn.close()
            log.log('Deleted previous days data of OI')
        except:
            log.logException('Exception : can not delete old data of OI')

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists OptionChain(id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Strike int not null, Expiry varchar(20) not null, ContractType varchar(2), OpenInterest integer,ChangeInOI integer, pChangeInOI real, TotVol integer, IV real, LTP real, TotBuyQty integer, TotSellQty integer, Underlying real)"
            self.c.execute(query)
            self.c.close()
        except:
            log.logException('OIDataWriterThread - Exception during CreateDB()')
            log.log('Aborting process...')
            exit()
                    
    def InsertData(self, data, contractType):
        try:
            time        = str(h.getTime())
            strkPrice   = str(data['strikePrice'])
            expiryDate  = str(data['expiryDate'])
            oi          = data[contractType]['openInterest']
            oiChg       = data[contractType]['changeinOpenInterest']
            pOiChg      = data[contractType]['pchangeinOpenInterest']
            totVol      = data[contractType]['totalTradedVolume']
            iv          = data[contractType]['impliedVolatility']
            ltp         = data[contractType]['lastPrice']
            totBuyQty   = data[contractType]['totalBuyQuantity']
            totSellQty  = data[contractType]['totalSellQuantity']
            undelying   = data[contractType]['underlyingValue']
            ContractType= contractType

            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("INSERT INTO OptionChain (Date, Time, Strike, Expiry, ContractType, OpenInterest, ChangeInOI, pChangeInOI, TotVol, IV, LTP, TotBuyQty, TotSellQty, Underlying) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, ?)", (self.todaydate, time, strkPrice, expiryDate, ContractType, oi, oiChg, pOiChg, totVol, iv, ltp, totBuyQty, totSellQty, undelying))
            self.conn.commit()
            self.c.close()
            self.conn.close()
        except:
            log.log("Strike = " + str(data['strikePrice']) + "   expiry date = " + str(data['expiryDate']))
            log.logException('OIDataWriterThread - Exception while inserting CE/PE data')

    def ImportNseDataToDatabase(self):
        try:
            oiData = h.GetOIDataList()
            if oiData == []:
                log.log('empty OI data received')
                return
            
            for data in oiData:
                expiryDate = str(data['expiryDate'])
                if expiryDate in self.expiryDateList:
                    if "CE" in data.keys():
                        self.InsertData(data, 'CE')
                    if "PE" in data.keys():
                        self.InsertData(data, 'PE')        
        except:
            log.logException('OIDataWriterThread - Exception while reading nse data or adding record to database')

    def run(self):
        while True:
            if True == self.writeDuringMrktHrs:
                if h.IsThisMarketHr():
                    self.ImportNseDataToDatabase()
            else:
                self.ImportNseDataToDatabase()

            t.sleep(self.timer)

class OIDataReaderThread(QtCore.QThread):
    signal      = QtCore.pyqtSignal('PyQt_PyObject')
    peSignal    = QtCore.pyqtSignal(dict)
    ceSignal    = QtCore.pyqtSignal(dict)
    niftySignal = QtCore.pyqtSignal(dict)
    niftyHeavyWeightsSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self, expiry, heavyWeightsList):
        QtCore.QThread.__init__(self)
        log.log("OIDataReaderThread Started...")
        self.currentExpiry = expiry
        self.timePeriod    = 1
        self.todayDate = h.getDate()
        self.optionaTable = "OptionChain.db"
        self.NiftyTable = "Nifty50"
        self.heavyWeightsList = heavyWeightsList
        self.otmsToShow = []
        self.timer = 60

    def __del__(self): 
        log.log("OIDataReaderThread - exited") 

    def SetOtmsToShow(self, otms):
        log.log("OIDataReaderThread - SetOtmsToShow = " + str(otms))
        self.otmsToShow = otms.copy()
        self.ReadData()

    def UpdateOtmsToShow(self, otms):
        self.otmsToShow = otms.copy()

    def SetExpiry(self, expiry):
        log.log("OIDataReaderThread - SetExpiry = " + expiry)
        self.currentExpiry = expiry
        self.ReadData()

    def SetTimePeriod(self, timePeriod):
        log.log("OIDataReaderThread - SetTimePeriod = " + str(timePeriod))
        self.timePeriod = timePeriod      
        self.ReadData()

    def UpdateHeavyWeightsList(self, heavyList):
        log.log("OIDataReaderThread - UpdateHeavyWeightsList = " + str(heavyList))
        self.heavyWeightsList = heavyList
        self.ReadData()

    def ReadData(self):
        self.conn = sqlite3.connect("OptionChain.db")
        self.c = self.conn.cursor()
        tPeriod = self.timePeriod

        try:
            for strike in self.otmsToShow:        
                ce_data = {}
                pe_data = {}    
                query = "SELECT Time, OpenInterest, Underlying, TotVol FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
                query = query + " ContractType = 'CE' AND "  + " Strike = " + str(strike) + " AND Date = '" + self.todayDate + "'" 
                result = self.c.execute(query)
                ceRecords = result.fetchall()

                niftyPrc = []
                timeLst  = []
                OI       = []
                TotVolume= []
                i        = 0
                sz       = len(ceRecords)
                
                for record in ceRecords:
                    if 1 == tPeriod or 0 == i or i == sz-1 or 0 == (i % tPeriod):
                        timeLst.append(record[0])
                        OI.append(record[1]) 
                        niftyPrc.append(record[2])
                        TotVolume.append(record[3])

                ce_data['Strike'] = strike
                ce_data['Time'] = timeLst
                ce_data['OI'] = OI
                ce_data['Nifty']  = niftyPrc
                ce_data['Volume'] = TotVolume       
                self.ceSignal.emit(ce_data)

                query = "SELECT Time, OpenInterest, TotVol FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
                query = query + " ContractType = 'PE' AND "  + " Strike = " + str(strike) + " AND Date = '" + self.todayDate + "'"
                result = self.c.execute(query)
                peRecords = result.fetchall()

                timeLst  = []
                OI       = []
                TotVolume= []
                i  = 0
                sz = len(peRecords)
                for record in peRecords:
                    if 1 == tPeriod or 0 == i or i == sz-1 or 0 == (i % tPeriod):
                        timeLst.append(record[0])
                        OI.append(record[1]) 
                        TotVolume.append(record[2])
                    i = i+1

                pe_data['Strike'] = strike
                pe_data['Time'] = timeLst
                pe_data['OI'] = OI       
                pe_data['Volume'] = TotVolume       
                self.peSignal.emit(pe_data)

            query = "SELECT Time, Nifty50 FROM " + self.NiftyTable + " WHERE Date = '" + self.todayDate + "'" 
            result = self.c.execute(query)
            niftyRecords = result.fetchall()

            timeLst = []
            price   = []
            i  = 0
            sz = len(niftyRecords)
            for record in niftyRecords:
                if 1 == tPeriod or 0 == i or i == sz-1 or 0 == (i % tPeriod):
                    timeLst.append(record[0])
                    price.append(record[1]) 

            nifty_data = {}    
            nifty_data['Time'] = timeLst
            nifty_data['Price'] = price       
            self.niftySignal.emit(nifty_data)

            if [] != self.heavyWeightsList:
                subQry = "("
                for stk in self.heavyWeightsList:
                    subQry = subQry + "'" + stk + "',"
                subQry = subQry[:-1]
                subQry = subQry + ")"

                query = "SELECT Time, Symbol, LTP, TotVol, pChange FROM NiftyHeavyWeights WHERE Date = '" + self.todayDate + "'"
                query = query +  " and Symbol in " + subQry 

                result = self.c.execute(query)
                niftyRecords = result.fetchall()

                if [] != niftyRecords:
                    self.niftyHeavyWeightsSgnl.emit(niftyRecords)
            else:
                self.niftyHeavyWeightsSgnl.emit([])

        except:
            log.logException("OIDataReaderThread - exception during read data")

        self.c.close()
        self.conn.close()

    def run(self):
        while True:
            if [] == self.otmsToShow:
                log.log('OIDataReaderThread - OTMS to show is empty, retry')
                t.sleep(5)
                continue

            self.ReadData()
            t.sleep(self.timer)
