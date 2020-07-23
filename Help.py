import time as t 
import requests
import time as t 

url                 = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
urlMarketStatus     = "https://www.nseindia.com/api/marketStatus"
urlActiveContracts  = "https://www.nseindia.com/api/snapshot-derivatives-equity?index=contracts&limit=20"
urlNifty50Stocks    = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"  

payload = {}
headers = {
'Accept': '*/*',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
'Accept-Encoding': 'gzip, deflate, br'}

def getTime(param="%H:%M"):
    now = t.localtime(t.time())
    return (t.strftime(param, now))

def getDate(param="%d-%b-%Y"):
    now = t.localtime(t.time())
    return (t.strftime(param, now))

def GetExpiryList():
    global url, payload, headers
    expiryList = []
    expiryData = []
    try:
        response = requests.request("GET", url, headers=headers, data = payload)
        json = response.json()
        expiryList = json['records']['expiryDates']
    except:
        #print('GetExpiryList() - Exception while sending request to NSE india for Expiry List')
        expiryList = []
        
    #consider only 5 records
    if len(expiryList) > 5:
        for i in range(5):
            expiryData.append(expiryList[i])
    else:
        expiryData = expiryList

    return expiryData

def GetOIDataList():
    global url, payload, headers
    expiryList = []
    try:
        response = requests.request("GET", url, headers=headers, data = payload)
        json = response.json()
        expiryList = json['records']['data']
    except:
        print('GetOIDataList() - Exception while sending request to NSE india for Option chain data.')
    
    return expiryList

def getNiftyCurrentPrice():
    global urlMarketStatus, payload, headers
    niftyPrice = -1
    try:
        response = requests.request("GET", urlMarketStatus, headers=headers, data = payload)
        json = response.json()

        for market in json['marketState']:
            if market['index'] == 'NIFTY 50':
                niftyPrice = market['last']
    except:
        print('getNiftyCurrentPrice() - Exception while sending request to Nifty for Nifty price.')
      
    return niftyPrice

def getATM(niftyCurrentPrice):
    niftyPrice = niftyCurrentPrice
    
    if niftyPrice > 0:
        mod = niftyPrice % 50
        if mod > 25:
            niftyPrice = niftyPrice - mod + 50 
        else:
            niftyPrice = niftyPrice - mod 

    return niftyPrice

def GetActiveContractsByVolume():
    global urlActiveContracts, payload, headers
    activeVolumeList = []
    try:
        response = requests.request("GET", urlActiveContracts, headers=headers, data = payload)
        json = response.json()
        activeVolumeList = json['volume']['data']
    except:
        print('GetActiveContractsByVolume() - Exception while sending request to NSE india.')
    
    return activeVolumeList

def GetActiveContractsByValue():
    global urlActiveContracts, payload, headers
    activeValueList = []
    try:
        response = requests.request("GET", urlActiveContracts, headers=headers, data = payload)
        json = response.json()
        activeValueList = json['value']['data']
    except:
        print('GetActiveContractsByValue() - Exception while sending request to NSE india.')
    
    return activeValueList


def GetNifty50Data():
    global urlNifty50Stocks, payload, headers
    nifty50List = []
    try:
        response = requests.request("GET", urlNifty50Stocks, headers=headers, data = payload)
        json = response.json()
        nifty50List = json['data']
    except:
        print('GetNifty50Data() - Exception while sending request to NSE india.')
    
    return nifty50List