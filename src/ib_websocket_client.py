import signal
import threading
import json
import datetime
import dateutil.tz
import requests
import websocket
import ssl

from typing import Optional, Callable
from secret_manager_client import SecretManagerClient
from ib_manager import IBManager

eastern_time_zone = dateutil.tz.gettz('US/Eastern')

class IBWebSocketClient:
    DEFAULT_HOST = "ndcdyn.interactivebrokers.com/portal.proxy/v1/portal"
    LOCAL_HOST = "localhost:5000/v1/api"

    def __init__(self, on_message):
        self._host = self.LOCAL_HOST
        self.url = f"wss://{self._host}/ws"
        self.ws: websocket.WebSocketApp = websocket.WebSocketApp(self.url, on_open=self.on_open, on_close=self.on_close,
                                                                 on_error=self.on_error, on_message=on_message)
        self.secretManagerClient = SecretManagerClient('us-east-1')
        self.ib_manager = IBManager()

        print ("IBWebSocketClient - __init__ complete.", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
   
    def run_forever(self):
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def run(self):
        try:
            self._run_thread = threading.Thread(target=self.run_forever)
            self._run_thread.start()
            print ('IBWebSocketClient - run() - completed.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ('IBWebSocketClient - run() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def close_connection(self):
        try:
            self.ws.close()
            if self._run_thread:
                self._run_thread.join()
            print ('IBWebSocketClient - close_connection() - completed.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ('IBWebSocketClient - close_connection() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    # 31 - last price, 55 - symbol, 70 - high, 71 - low, 82 - change price
    # 83 - change percent, 87 - volume, 7295 - open, 7296 - close
    def subscribe(self, code, con_id):
        try:
            # first grab contract id
            #con_id = self.ib_manager.get_contract_id(code)
            print ('IBWebSocketClient - subscribe() - stock code: {} - contract id: {}'.format(code, con_id))
            self.ws.send('smd+%s+{"fields":["31","55","70","71","82","83","87","7295","7296"]}' % con_id)
        except Exception as ex:
            print ('IBWebSocketClient - subscribe() - failed for stock {}'.format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def unsubscribe(self, code):
        try:
            # first grab contract id
            con_id = self.get_stock(code)
            print ('IBWebSocketClient - unsubscribe() - stock code: {} - contract id: {}'.format(code, con_id))
            self.ws.send('umd+%s{}' % con_id)
        except Exception as ex:
            print ('IBWebSocketClient - unsubscribe() - failed for stock {}'.format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_open(ws):
        print("IBWebSocketClient() - on_open()", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_error(ws, error):
        print("IBWebSocketClient() - on_error()", error, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_close(ws):
        print("IBWebSocketClient() - process_ib_on_close()", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))