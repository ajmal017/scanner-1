import datetime
import dateutil.tz
import threading
import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

eastern_time_zone = dateutil.tz.gettz('US/Eastern')

class IBapi(EWrapper, EClient):
	def __init__(self):
		EClient.__init__(self, self)
	# historical data
	'''
		tickerId, A unique identifier which will serve to identify the incoming data.
		contract, The IBApi.Contract you are interested in.
		endDateTime, The request's end date and time (the empty string indicates current present moment).
		durationString, The amount of time (or Valid Duration String units) to go back from the request's given end date and time.
		barSizeSetting, The data's granularity or Valid Bar Sizes
		whatToShow, The type of data to retrieve. See Historical Data Types
		useRTH, Whether (1) or not (0) to retrieve data generated only within Regular Trading Hours (RTH)
		formatDate, The format in which the incoming bars' date should be presented. Note that for day bars, only yyyyMMdd format is available.
		keepUpToDate, Whether a subscription is made to return updates of unfinished real time bars as they are available (True), or all data is returned on a one-time basis (False). Available starting with API v973.03+ and TWS v965+. If True, and endDateTime cannot be specified.
		For example, making a request with an end date and time of "20160127 23:59:59", a duration string of "3 D" and a bar size of "1 hour" will return three days worth of 1 hour bars data in which the most recent bar will be the closest possible to 20160127 23:59:59.
	'''
	def historicalData(self, reqId, bar):
		print(reqId, bar)
	def historicalDataEnd(self, reqId, startDate, endDate):
		print(reqId, startDate, endDate)
	def	historicalDataUpdate(self, reqId, bar):
		print(reqId, bar)

	# real time price and bar data
	def tickPrice(self, reqId, tickType, price, attrib):
		if tickType == 2 and reqId == 1:
			print('The current ask price is: ', price, attrib)
	def realtimeBar(self, reqId, time, open, high, low, close, volume, WAP, count):
		print(time, open, high, low, close, volume)

	# real time tick data
	def tickByTickBidAsk(self, reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
		print(reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)
	def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions):
		print(tickType, time, price, size, tickAttribLast, exchange, specialConditions)
	def tickByTickMidPoint(self, reqId, time, midPoint):
		print(time, time, midPoint)

	def symbolSamples(self, reqId, contractDescriptions):
		super().symbolSamples(reqId, contractDescriptions)
		print("Symbol Samples. Request Id: ", reqId)
		
		for contractDescription in contractDescriptions:
			derivSecTypes = ""
			for derivSecType in contractDescription.derivativeSecTypes:
				derivSecTypes += derivSecType
				derivSecTypes += " "
				print("Contract: conId:%s, symbol:%s, secType:%s primExchange:%s, currency:%s, derivativeSecTypes:%s" % (contractDescription.contract.conId, contractDescription.contract.symbol, contractDescription.contract.secType, contractDescription.contract.primaryExchange, contractDescription.contract.currency, derivSecTypes))

def run_loop():
	app.run()

app = IBapi()
app.connect('127.0.0.1', 4001, 123)

#Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

time.sleep(1) #Sleep interval to allow time for connection to server

#Create contract object, currency must be local - GBP:LSE, NYSE:USD, ASX:AUD

contract = Contract()

'''contract.symbol = 'AAPL'
contract.secType = 'STK'
contract.exchange = 'NYSE'
contract.currency = 'USD'''

'''contract.symbol = 'CBA'
contract.secType = 'STK'
contract.currency = 'AUD'
contract.exchange = 'ASX'''

'''contract.symbol = 'BARC'
contract.secType = 'STK'
contract.exchange = 'LSE'
contract.currency = 'GBP'''

'''contract.symbol = '6070'
contract.secType = 'STK'
contract.exchange = 'TSEJ'
contract.currency = 'JPY'''

#app.reqMatchingSymbols(211, "IB")

#Request Market Data
#app.reqMktData(1, apple_contract, '', False, False, [])
#app.reqRealTimeBars(1, apple_contract, 5, "TRADES", True, [])
#app.reqTickByTickData(1, apple_contract, 5, "TRADES", True, [])
#app.reqTickByTickData(1, apple_contract, "AllLast", 0, False)
'query time format - YYYYMMDD hh:mm:ss [TMZ]'
queryTime = datetime.datetime.now(tz=eastern_time_zone).strftime("%Y%m%d %H:%M:%S")
print ('queryTime', queryTime)
#app.reqHistoricalData(1, apple_contract, queryTime, "1 M", "1 day", "MIDPOINT", 1, 1, False, [])
app.reqHistoricalData(1, contract, queryTime, "10 D", "1 min", "TRADES", 1, 1, False, [])

time.sleep(100) #Sleep interval to allow time for incoming price data
app.disconnect()