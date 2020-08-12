from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t
import sqlite3

class NiftyPriceWriterThread(QtCore.QThread):
    signal = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        log.log("NiftyPriceWriterThread Started...")
        self.table = "Nifty50"
        self.todaydate = str(h.getDate())
        self.timer = 60
        self.createDB()
        self.DeleteOldData()
        self.writeDuringMrktHrs  = True

    def __del__(self): 
        log.log("NiftyPriceWriterThread - exited") 

    def SetWriteDataState(self, state):
        self.writeDuringMrktHrs  = state
        log.log("NiftyPriceWriterThread - setWriteDataState : " + str(state))

    def createDB(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            query = "create table if not exists " + self.table + " (id integer primary key AUTOINCREMENT, Date Date not null, Time Time not null, Nifty50 real)"
            self.c.execute(query)
            self.c.close()
        except:
            log.logException('Exception during Create Nifty50 table')
            log.log('Aborting process...')
            exit()
    
    def DeleteOldData(self):
        try:
            self.conn = sqlite3.connect("OptionChain.db")
            self.c = self.conn.cursor()
            self.c.execute("DELETE from Nifty50 WHERE Date != '"+ self.todaydate + "'")
            self.conn.commit()
            self.c.close()
            self.conn.close()
            log.log('Deleted previous days data of nifty 50')
        except:
            log.logException('Exception : can not delete old data of nifty 50')

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
            log.logException("NiftyPriceWriterThread - exception during InsertNiftyData")


    def run(self):
        while True:
            niftyPrice = h.getNiftyCurrentPrice()
            if niftyPrice == -1:
                log.log('NiftyPriceWriterThread - Exception while getting nifty price, retry')
                t.sleep(5)
                continue
            self.signal.emit(niftyPrice)

            if True == self.writeDuringMrktHrs:
                if h.IsThisMarketHr():
                    self.InsertNiftyData(niftyPrice)
            else:
                self.InsertNiftyData(niftyPrice)

            t.sleep(self.timer)