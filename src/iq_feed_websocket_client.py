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

eastern_time_zone = dateutil.tz.gettz('US/Eastern')

class IBWebSocketClient:
    DEFAULT_HOST = "ndcdyn.interactivebrokers.com/portal.proxy/v1/portal"
    LOCAL_HOST = "localhost:5000/v1/api"
    # TODO: Either an instance of the client couples 1:1 with the cluster or an instance of the Client couples 1:3 with
    #  the 3 possible clusters (I think I like client per, but then a problem is the user can make multiple clients for
    #  the same cluster and that's not desirable behavior,
    #  somehow keeping track with multiple Client instances will be the difficulty)
    def __init__(self, on_message):
        self._host = self.LOCAL_HOST
        self.url = f"wss://{self._host}/ws"
        self.ws: websocket.WebSocketApp = websocket.WebSocketApp(self.url, on_open=self.on_open, on_close=self.on_close,
                                                                 on_error=self.on_error, on_message=on_message)
        self.secretManagerClient = SecretManagerClient('us-east-1')

        # TODO: this probably isn't great design.
        #  If the user defines their own signal handler then this will gets overwritten.
        #  We still need to make sure that killing, terminating, interrupting the program closes the connection
        #signal.signal(signal.SIGINT, self._cleanup_signal_handler())
        #signal.signal(signal.SIGTERM, self._cleanup_signal_handler())
        print ("IBWebSocketClient - __init__ complete.", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def get_stock(self, code):
        try:
            secret = self.secretManagerClient.getSecretString('ib_auth')
            if secret:
                headers = {
                    'Content-Type': 'text/plain',
                    'Cookie': secret['cookie']
                }

                # first we need to get stock contract id
                stocks_response = requests.post('https://localhost:5000/v1/portal/iserver/secdef/search', data=json.dumps({
                    "symbol": code,
                    "name": True,
                    "secType": "STK"
                }), headers=headers, verify=False)
                print (stocks_response)
                print (stocks_response.text)
                stocks = json.loads(stocks_response.text)
                return stocks[0]['conid']
        except Exception as ex:
            print ("IBWebSocketClient - get_stock() - failed for stock {}".format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
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
        
    def subscribe(self, code):
        try:
            # first grab contract id
            con_id = self.get_stock(code)
            print ('IBWebSocketClient - subscribe() - stock code: {} - contract id: {}'.format(code, con_id))
            self.ws.send('smd+%s+{"fields":["31","83"]}' % con_id)
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