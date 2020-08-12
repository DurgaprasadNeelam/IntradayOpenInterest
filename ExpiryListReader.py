from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t

class ExpiryListReaderThread(QtCore.QThread):
    signal = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        log.log("ExpiryListReaderThread Started...")

    def __del__(self): 
        log.log("ExpiryListReaderThread - exited") 

    def run(self):
        expiryList = h.GetExpiryList()        
        #loop untill we get valid data
        while expiryList == []:
            log.log('ExpiryListReaderThread - Exception while getting expiry list, retry again')
            expiryList = h.GetExpiryList()
            t.sleep(1)
        self.signal.emit(expiryList)