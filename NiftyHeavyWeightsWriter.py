from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t
import sqlite3

class NiftyHeavyWeightsWriterThread(QtCore.QThread):
    def __init__(self, stockList):
        QtCore.QThread.__init__(self)
        log.log("NiftyHeavyWeightsWriterThread Started...")
        self.stockList = stockList
        self.table = "NiftyHeavyWeights"
        self.todaydate = str(h.getDate())
        self.timer = 61
        self.createDB()
        self.DeleteOldData()
        self.writeDuringMrktHrs  = True

    def __del__(self): 
        log.log("NiftyHeavyWeightsWriterThread - exited") 

    def DeleteOldData(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("DELETE from NiftyHeavyWeights WHERE Date != '"+ self.todaydate + "'")            
            self.conn.commit()
            self.c.close()
            self.conn.close()
            log.log('NiftyHeavyWeightsWriterThread - Deleted previous days data')
        except:
            log.logException('Exception : can not delete NiftyHeavyWeightsWriterThread old data')

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs  = state
        log.log("NiftyHeavyWeightsWriterThread - SetWriteDataState : " + str(state))

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists " + self.table + " (id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Symbol varchar(20), LTP real, TotVol integer, Open real, DayHigh real, DayLow real, pChange real)"
            self.c.execute(query)
            self.c.close()
        except:
            log.logException('Exception during Create NiftyHeavyWeights table')
            log.log('Aborting process...')
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
            log.logException("NiftyHeavyWeightsWriterThread - exception during write data")

        return False

    def run(self):
        while True:
            if True == self.writeDuringMrktHrs:
                if h.IsThisMarketHr():
                    if False == self.WriteData():
                        t.sleep(10)
                        continue
            else:
                if False == self.WriteData():
                    t.sleep(10)
                    continue
            t.sleep(self.timer)