from pyqtgraph.Qt import QtCore
from Help import log
import pandas as pd
import Help as h
import time as t
import sqlite3

class OIChangeReaderThread(QtCore.QThread):
    olChgSignal = QtCore.pyqtSignal(pd.DataFrame)

    def __init__(self, expiry, strikesToRead):
        QtCore.QThread.__init__(self)
        log.log("OIChangeReaderThread Started...")
        log.log("expiry = " + expiry)
        log.log("strikes to read = " + str(strikesToRead))
        self.currentExpiry = expiry
        self.strikesToRead = strikesToRead
        self.todayDate = h.getDate()
        self.optionaTable = "OptionChain.db"
        self.timer = 60

    def __del__(self): 
        log.log("OIChangeReaderThread - exited") 

    #future usage
    def SetExpiry(self, expiry):
        log.log("OIChangeReaderThread - SetExpiry = " + expiry)
        self.currentExpiry = expiry
        self.ReadData()

    def ReadData(self):
        self.conn = sqlite3.connect("OptionChain.db")
        self.c = self.conn.cursor()
        
        try:
            if [] == self.strikesToRead:
               log.log("OIChangeReaderThread - strikes to read in null")   
               return

            indexList  = []
            strikeList = []
            ExpiryList = []
            ContractTypeList = []
            OIList = []
            pChgList = []
            ltpList = []

            for stk in self.strikesToRead:
                query = "SELECT Strike, Expiry, ContractType, OpenInterest, pChangeInOI, LTP FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
                query = query + " Strike = " + str(stk) + " AND ContractType = 'CE' AND Date = '" + self.todayDate + "' order by Time desc limit 1" 
                result = self.c.execute(query)
                allRecords = result.fetchall()

                if len(allRecords) == 1:
                    lastRedord = allRecords[0]
                    #dataFrame[str(stk)+"-CE"] = lastRedord
                    #print(lastRedord)
                    indexList.append(str(lastRedord[0])+"-CE")
                    strikeList.append(lastRedord[0])
                    ExpiryList.append(lastRedord[1])
                    ContractTypeList.append(lastRedord[2])
                    OIList.append(lastRedord[3])
                    pChgList.append(lastRedord[4])
                    ltpList.append(lastRedord[5])

                query = "SELECT Strike, Expiry, ContractType, OpenInterest, pChangeInOI, LTP FROM OptionChain WHERE Expiry = '" + self.currentExpiry + "' AND "
                query = query + " Strike = " + str(stk) + " AND ContractType = 'PE' AND Date = '" + self.todayDate + "' order by Time desc limit 1" 
                result = self.c.execute(query)
                allRecords = result.fetchall()

                if len(allRecords) == 1:
                    lastRedord = allRecords[0]
                    #dataFrame[str(stk)+"-PE"] = lastRedord
                    #print(lastRedord)
                    indexList.append(str(lastRedord[0])+"-PE")
                    strikeList.append(lastRedord[0])
                    ExpiryList.append(lastRedord[1])
                    ContractTypeList.append(lastRedord[2])
                    OIList.append(lastRedord[3])
                    pChgList.append(lastRedord[4])
                    ltpList.append(lastRedord[5])

            dataFrame = pd.DataFrame({ "index" : indexList,
                                       "Strike": strikeList,
                                       "Expiry": ExpiryList, 
                                       "ContractType": ContractTypeList,
                                       "OI": OIList,
                                       "PChange": pChgList,
                                       "LTP": ltpList }
                                       )

            dataFrame.set_index('index', inplace = True)
            self.olChgSignal.emit(dataFrame)

        except:
            log.logException("OIChangeReaderThread - exception while reading data")

        self.c.close()
        self.conn.close()

    def run(self):
        while True:
            self.ReadData()
            t.sleep(self.timer)
