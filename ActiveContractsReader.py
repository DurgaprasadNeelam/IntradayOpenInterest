from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t

class ActiveContractsReaderThread(QtCore.QThread):
    activeContractsSgnl = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        log.log("ActiveContractsReaderThread Started...")
        self.timer = 60

    def __del__(self): 
        log.log("ActiveContractsReaderThread - exited") 

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
                    log.log("ActiveContractsReaderThread - active contacts by volume empty")
                    t.sleep(10)
                    continue        
            except:
                log.logException("ActiveContractsReaderThread - Exception while getting active contacts")
                t.sleep(10)
                continue

            t.sleep(self.timer)