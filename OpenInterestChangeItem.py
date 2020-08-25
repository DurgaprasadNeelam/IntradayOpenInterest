from pyqtgraph.Qt import QtCore, QtGui 
from Help import log

class OpenInterestChangeItem(QtGui.QGraphicsItem):
    def __init__(self, strike):
        super().__init__() 
        self.rect = QtCore.QRectF(0, 0, 330, 30)
        self.strike = strike
        self.ceOI = 1
        self.peOI = 1
        self.cePercent = 1
        self.pePercent = 1
        self.ceLtp = 1
        self.peLtp = 1
        self.maxOI = 1

    def setValues(self, maxOI, ceoi, peoi, cePercent, pePercent, ceLtp, peLtp):
        self.maxOI = maxOI
        self.ceOI  = ceoi
        self.peOI  = peoi
        self.ceLtp = ceLtp
        self.peLtp = peLtp
        self.cePercent = cePercent
        self.pePercent = pePercent

    def drawHeader(self, painter):
        try:
            bgBrush = QtGui.QBrush(QtGui.QColor(255, 255, 153))
            painter.setBrush(bgBrush)

            adjustWidth = 60
            painter.drawRect(QtCore.QRectF(self.rect.x(), self.rect.y(), (self.rect.width()/2)-adjustWidth, self.rect.height()))
            painter.drawText(QtCore.QRectF(self.rect.x()+5, self.rect.y()+5, (self.rect.width()/2)-adjustWidth, self.rect.height()), "CE OI")
            ceLtpX = (self.rect.width()/2)-adjustWidth
            ceLtpW = 35
            painter.drawRect(QtCore.QRectF(ceLtpX, self.rect.y(), ceLtpW, self.rect.height()))
            painter.drawText(QtCore.QRectF(ceLtpX+5, self.rect.y()+5, ceLtpW, self.rect.height()), "LTP")
            strikeX = ceLtpX+ceLtpW
            strikeW = 50
            painter.drawRect(QtCore.QRectF(strikeX, self.rect.y(), strikeW+3, self.rect.height()))
            painter.drawText(QtCore.QRectF(strikeX+5, self.rect.y()+5, strikeW, self.rect.height()), "Strike")
            peLtpX = strikeX+strikeW
            peLtpW = 35
            painter.drawRect(QtCore.QRectF(peLtpX+3, self.rect.y(), peLtpW, self.rect.height()))
            painter.drawText(QtCore.QRectF(peLtpX+5, self.rect.y()+5, peLtpW, self.rect.height()), "LTP")
            painter.drawRect(QtCore.QRectF((self.rect.width()/2)+adjustWidth, self.rect.y(), (self.rect.width()/2)-adjustWidth, self.rect.height()))
            painter.drawText(QtCore.QRectF((self.rect.width()/2)+adjustWidth+5, self.rect.y()+5, (self.rect.width()/2)-adjustWidth, self.rect.height()), "PE OI")
        except:
            log.logException("Exception in OpenInterestChangeItem while paint")

    def paint(self, painter, option, widget):
        try:
            if ("Strike" == self.strike):
                self.drawHeader(painter)
                return

            ceOIColor = QtGui.QBrush(QtGui.QColor(255,127,80))
            peOIColor = QtGui.QBrush(QtGui.QColor(144,238,144))
            grayBrush = QtGui.QBrush(QtGui.QColor(192,192,192))
            
            #draw outer rect & strike
            defBrush = painter.brush()
            painter.setBackground(grayBrush)
            adjustWidth = 60
            adjustHeightForText = 5
            adjustWidthForText  = 5
            adjustWidthForText_sm  = 2
            painter.drawRect(QtCore.QRectF(self.rect.x(), self.rect.y(), (self.rect.width()/2)-adjustWidth, self.rect.height()))
            painter.setBackground(defBrush)
            ceLtpX = (self.rect.width()/2)-adjustWidth
            ceLtpW = 35
            painter.drawText(QtCore.QRectF(ceLtpX+adjustWidthForText_sm, self.rect.y()+adjustHeightForText, ceLtpW, self.rect.height()), str(round(self.ceLtp,1)))
            painter.drawLine(ceLtpX+adjustWidthForText_sm+ceLtpW-1, 0, ceLtpX+adjustWidthForText_sm+ceLtpW-2, self.rect.height())
            painter.setBackground(grayBrush)
            strikeX = ceLtpX+ceLtpW
            strikeW = 50
            painter.drawText(QtCore.QRectF(strikeX+adjustWidthForText, self.rect.y()+adjustHeightForText, strikeW, self.rect.height()), self.strike)
            painter.drawLine(strikeX+adjustWidthForText+strikeW-1, 0, strikeX+adjustWidthForText+strikeW, self.rect.height())
            painter.setBackground(grayBrush)
            peLtpX = strikeX+strikeW
            peLtpW = 35
            painter.drawText(QtCore.QRectF(peLtpX+adjustWidthForText, self.rect.y()+adjustHeightForText, peLtpW, self.rect.height()), str(round(self.peLtp,1)))
            painter.setBackground(grayBrush)
            painter.drawRect(QtCore.QRectF((self.rect.width()/2)+adjustWidth, self.rect.y(), (self.rect.width()/2)-adjustWidth, self.rect.height()))
            painter.setBackground(defBrush)

            #update the OI change
            painter.setBrush(ceOIColor)
            cePercentWidth = ( ((self.ceOI/self.maxOI)*100.0)*((self.rect.width()/2)-adjustWidth) )/100 
            painter.drawRoundedRect(QtCore.QRectF(self.rect.x(), self.rect.y()+5, cePercentWidth, self.rect.height()-10), 5, 0)
            ceChg = str(round(self.ceOI/1000.0, 1)) + "k (" + str(round(self.cePercent,1)) + "%)"
            painter.drawText(QtCore.QRectF(self.rect.x()+adjustWidthForText, self.rect.y()+adjustHeightForText, (self.rect.width()/2), self.rect.height()), ceChg)

            painter.setBrush(peOIColor)
            pePercentWidth = ( ((self.peOI/self.maxOI)*100.0)*((self.rect.width()/2)-adjustWidth) )/100
            painter.drawRoundedRect(QtCore.QRectF(self.rect.width()-pePercentWidth, self.rect.y()+5, pePercentWidth, self.rect.height()-10), 0, 5)
            peChg = str(round(self.peOI/1000.0, 1)) + "k (" + str(round(self.pePercent,1)) + "%)"
            painter.drawText(QtCore.QRectF((self.rect.width()/2)+adjustWidth+adjustWidthForText, self.rect.y()+adjustHeightForText, (self.rect.width()/2)-adjustWidth, self.rect.height()), peChg)

            painter.setBrush(defBrush)
        except:
            log.logException("Exception in OpenInterestChangeItem during paint")

    def boundingRect(self):
        #print(self.rect)
        return self.rect




