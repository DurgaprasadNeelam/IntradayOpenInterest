from pyqtgraph.Qt import QtCore
from Help import log
import Help as h
import time as t 

class NiftyLivePriceReaderThread(QtCore.QThread):
    signal = QtCore.pyqtSignal('PyQt_PyObject')

    def __init__(self):
        QtCore.QThread.__init__(self)
        log.log("NiftyLivePriceReaderThread Started...")
        self.timer = 2

    def __del__(self): 
        log.log("NiftyLivePriceReaderThread - exited") 

    def run(self):
        while True:
            niftyPrice = h.getNiftyCurrentPrice()
            if niftyPrice != -1:
                self.signal.emit(niftyPrice)
            else:
                log.log("Nifty Live price read failed")
            t.sleep(self.timer)