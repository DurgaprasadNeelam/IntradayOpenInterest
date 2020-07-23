from pyqtgraph.Qt import QtCore
import datetime
import Help as h
import time as t
import sqlite3

class Logger():
    def log(self, msg):
        print(msg)

def IsThisMarketHr():   
    now = datetime.datetime.now() 
    today930 = now.replace(hour=9, minute=15, second=0, microsecond=0)
    today330 = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if  now >= today930 and now <= today330:
        return True    
    else:
        return False

expiryDateList = []
log = Logger()

class DataWriteThread(QtCore.QThread):
    def __init__(self):
        QtCore.QThread.__init__(self)
        self.timer = 60
        self.time_x_axis        = []
        self.data_to_plot_ce    = {}
        self.data_to_plot_pe    = {}
        self.todaydate          = str(h.getDate())
        self.writeDuringMrktHrs = True
        self.createDB()
        self.DeleteOldData()

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs = True

    def DeleteOldData(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("DELETE from OptionChain WHERE Date != '"+ self.todaydate + "'")
            self.c.execute("DELETE from Nifty50 WHERE Date != '"+ self.todaydate + "'")            
            self.conn.commit()
            self.c.close()
            self.conn.close()
            log.log('Deleted previous days data')
        except:
            log.log('Exception : can not delete old data')

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists OptionChain(id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Strike int not null, Expiry varchar(20) not null, ContractType varchar(2), OpenInterest integer,ChangeInOI integer, pChangeInOI real, TotVol integer, IV real, LTP real, TotBuyQty integer, TotSellQty integer, Underlying real)"
            self.c.execute(query)
            self.c.close()
            log.log('Db created/updated successfully')
        except:
            log.log('Exception during CreateDB()')
            log.log('Aborting process')
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
            a = 1
        #    log.log('Exception while inserting CE/PE data')

    def ImportNseDataToDatabase(self):
        try:
            oiData = h.GetOIDataList()
            if oiData == []:
                log.log('empty OI data received')
                return
            
            for data in oiData:
                expiryDate = str(data['expiryDate'])
                if expiryDate in expiryDateList:
                    self.InsertData(data, 'CE')
                    self.InsertData(data, 'PE')        
        except:
            log.log('Exception while reading nse data or adding record to database')

    def run(self):
        while True:
            if True == self.writeDuringMrktHrs:
                if IsThisMarketHr():
                    self.ImportNseDataToDatabase()
            else:
                self.ImportNseDataToDatabase()

            t.sleep(self.timer)

class DataReadThread(QtCore.QThread):
    signal      = QtCore.pyqtSignal('PyQt_PyObject')
    peSignal    = QtCore.pyqtSignal(dict)
    ceSignal    = QtCore.pyqtSignal(dict)
    niftySignal = QtCore.pyqtSignal(dict)

    def __init__(self, expiry):
        QtCore.QThread.__init__(self)
        self.currentExpiry = expiry
        self.timePeriod    = 1
        self.todayDate = h.getDate()
        self.optionaTable = "OptionChain.db"
        self.NiftyTable = "Nifty50"
        self.otmsToShow = []
        self.timer = 60

    def SetOtmsToShow(self, otms):
        self.otmsToShow = otms.copy()
        self.ReadData()

    def UpdateOtmsToShow(self, otms):
        self.otmsToShow = otms.copy()

    def SetExpiry(self, expiry):
        self.currentExpiry = expiry
        self.ReadData()

    def SetTimePeriod(self, timePeriod):
        self.timePeriod = timePeriod      
        self.ReadData()

    def ReadData(self):
        self.conn = sqlite3.connect("OptionChain.db")
        self.c = self.conn.cursor()
        '''
        log.log("current expry for query = " + self.currentExpiry)
        log.log("Time period = " + str(self.timePeriod))
        log.log("OTMs2Show = " + str(self.otmsToShow))
        '''
        tPeriod = self.timePeriod
        nifty_data = {}    
        for strike in self.otmsToShow:        
            ce_data = {}
            pe_data = {}    
            query = "SELECT Time, OpenInterest, Underlying FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
            query = query + " ContractType = 'CE' AND "  + " Strike = " + str(strike) + " AND Date = '" + self.todayDate + "'" 
            result = self.c.execute(query)
            ceRecords = result.fetchall()

            niftyPrc = []
            timeLst  = []
            OI       = []
            i        = 0
            sz       = len(ceRecords)
            
            for record in ceRecords:
                if 1 == tPeriod or 0 == i or i == sz-1 or 0 == (i % tPeriod):
                    timeLst.append(record[0])
                    OI.append(record[1]) 
                    niftyPrc.append(record[2])

            ce_data['Strike'] = strike
            ce_data['Time'] = timeLst
            ce_data['OI'] = OI
            ce_data['Nifty'] = niftyPrc       
            self.ceSignal.emit(ce_data)

            query = "SELECT Time, OpenInterest FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
            query = query + " ContractType = 'PE' AND "  + " Strike = " + str(strike) + " AND Date = '" + self.todayDate + "'"
            result = self.c.execute(query)
            peRecords = result.fetchall()

            timeLst = []
            OI   = []
            i  = 0
            sz = len(peRecords)
            for record in peRecords:
                if 1 == tPeriod or 0 == i or i == sz-1 or 0 == (i % tPeriod):
                    timeLst.append(record[0])
                    OI.append(record[1]) 
                i = i+1

            pe_data['Strike'] = strike
            pe_data['Time'] = timeLst
            pe_data['OI'] = OI       
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

        nifty_data['Time'] = timeLst
        nifty_data['Price'] = price       
        self.niftySignal.emit(nifty_data)

        self.c.close()
        self.conn.close()

    def run(self):
        while True:
            if [] == self.otmsToShow:
                log.log('OTMS to show is empty in read thread, retry')
                t.sleep(5)
                continue

            self.ReadData()
            t.sleep(self.timer)

class InitDataThread(QtCore.QThread):
    signal = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)

    def run(self):
        global expiryDateList
        expiryList = h.GetExpiryList()        
        #loop untill we get valid data
        while expiryList == []:
            log.log('Exception while getting expiry list, retry again')
            expiryList = h.GetExpiryList()
            t.sleep(5)

        expiryDateList = expiryList
        self.signal.emit(expiryList)

class NiftyPriceThread(QtCore.QThread):
    signal = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        self.table = "Nifty50"
        self.timer = 60
        self.createDB()
        self.writeDuringMrktHrs  = True

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs  = state

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists " + self.table + " (id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Nifty50 real)"
            self.c.execute(query)
            self.c.close()
            log.log('Nifty table created/updated successfully')
        except:
            log.log('Exception during Create Nifty50 table')
            log.log('Aborting process')
            exit()
                
    def InsertNiftyData(self, niftyPrc):        
        try:
            time        = str(h.getTime())
            date        = str(h.getDate())

            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("INSERT INTO " + self.table + " (Date, Time, Nifty50) VALUES (?,?,?)", (date, time, niftyPrc))
            self.conn.commit()
            self.c.close()
            self.conn.close()
        except:
            a = 1

    def run(self):
        while True:
            niftyPrice = h.getNiftyCurrentPrice()
            if niftyPrice == -1:
                log.log('Exception while getting nifty price, retry')
                t.sleep(5)
                continue
            self.signal.emit(niftyPrice)

            if True == self.writeDuringMrktHrs:
                if IsThisMarketHr():
                    self.InsertNiftyData(niftyPrice)
            else:
                self.InsertNiftyData(niftyPrice)

            t.sleep(self.timer)

class ActiveContractsThread(QtCore.QThread):
    activeContractsSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        self.timer = 60

    def run(self):
        while True:
            activeContactsByVolume = h.GetActiveContractsByVolume()    
            try:
                allRecords = []
                if [] != activeContactsByVolume:
                    for record in activeContactsByVolume:
                        if "NIFTY" == record['underlying']:
                            row = [record['strikePrice'], record['optionType'], ("%.2f" % record['pChange']), record['openInterest'], record['lastPrice']]
                            allRecords.append(row)
                    
                    allRecords = allRecords[:5]
                    self.activeContractsSgnl.emit(allRecords)
                else:
                    t.sleep(10)
                    continue        
            except:
                t.sleep(10)
                continue

            t.sleep(self.timer)
    

class Nifty50DataWriteThread(QtCore.QThread):
    def __init__(self, stockList):
        QtCore.QThread.__init__(self)
        self.stockList = stockList
        self.table = "NiftyHeavyWeights"
        self.timer = 60
        self.createDB()
        self.writeDuringMrktHrs  = True

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs  = state

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists " + self.table + " (id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Symbol varchar(20), LTP real, TotVol integer, Open real, DayHigh real, DayLow real, pChange real)"
            self.c.execute(query)
            self.c.close()
            log.log('Created/updated NiftyHeavyWeightsTable')
        except:
            log.log('Exception during Create NiftyHeavyWeights table')
            log.log('Aborting process')
            exit()

    def WriteData(self):
        nifty50Stocks = h.GetNifty50Data()
        try:
            if [] != nifty50Stocks:
                time    = str(h.getTime())
                date    = str(h.getDate())
                for record in nifty50Stocks:
                    if record['symbol'] in self.stockList:
                        symbol  = record['symbol']
                        ltp     = record['lastPrice']
                        TotVol  = record['totalTradedVolume']
                        dOpen   = record['open']
                        dHigh   = record['dayHigh']
                        dLow    = record['dayLow']
                        pChg    = record['pChange']

                        self.conn = sqlite3.connect("OptionChain.db")
                        self.c = self.conn.cursor()
                        self.c.execute("INSERT INTO " + self.table + " (Date, Time, Symbol, LTP, TotVol, Open, DayHigh, DayLow, pChange) VALUES (?,?,?,?,?,?,?,?,?)", (date, time, symbol, ltp, TotVol, dOpen, dHigh, dLow, pChg))
                        self.conn.commit()
                        self.c.close()
                        self.conn.close()
                return True
        except:
            a = 1
        return False

    def run(self):
        while True:
            if True == self.writeDuringMrktHrs:
                if IsThisMarketHr():
                    if False == self.WriteData():
                        t.sleep(10)
                        continue
            else:
                if False == self.WriteData():
                    t.sleep(10)
                    continue
            t.sleep(self.timer)

class Nifty50DataReadThread(QtCore.QThread):
    nifty50StocksSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self, stockList):
        QtCore.QThread.__init__(self)
        self.timer = 61
        self.todayDate = h.getDate()
        self.stockList = stockList
        self.table = "NiftyHeavyWeights"

    def SetStockeList(self, stockList):
        self.stockList = stockList
        self.ReadData()

    def ReadData(self):
        if [] != self.stockList:
            subQry = "("
            for stk in self.stockList:
                subQry = subQry + "'" + stk + "',"
            subQry = subQry[:-1]
            subQry = subQry + ")"

            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "SELECT Time, Symbol, LTP, TotVol, pChange FROM " + self.table + " WHERE Date = '" + self.todayDate + "'"
            query = query +  " and Symbol in " + subQry 

            result = self.c.execute(query)
            niftyRecords = result.fetchall()

            if [] != niftyRecords:
                self.nifty50StocksSgnl.emit(niftyRecords)

    def run(self):
        while True:
            self.ReadData()
            t.sleep(self.timer)
