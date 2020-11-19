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

STOCKS_CLUSTER = "stocks"
FOREX_CLUSTER = "forex"
CRYPTO_CLUSTER = "crypto"

class PolygonWebSocketClient:
    DEFAULT_HOST = "socket.polygon.io"

    # TODO: Either an instance of the client couples 1:1 with the cluster or an instance of the Client couples 1:3 with
    #  the 3 possible clusters (I think I like client per, but then a problem is the user can make multiple clients for
    #  the same cluster and that's not desirable behavior,
    #  somehow keeping track with multiple Client instances will be the difficulty)
    def __init__(self, on_message, total_stocks_in_group):
        self.host = self.DEFAULT_HOST
        self.url = f"wss://{self.host}/ws"
        self.on_message = on_message
        self.polygon_connecting = False
        self.socket_connected = False
        self.number_of_stocks_subscribed = 0
        self.total_stocks_in_group = total_stocks_in_group
        self.secretManagerClient = SecretManagerClient('us-east-1')

        self.init_socket()

        print ("PolygonWebSocketClient - __init__ complete.", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    # creates a new global socket connection to polygon
    def init_socket():
        print ('PolygonWebSocketClient - init_socket()')
        try:
            if self.socket_connected and self.ws:
                print ('PolygonWebSocketClient - init_socket() - socket previously connected, deleting previous socket.')
                self.ws.on_message = None
                self.ws.on_open = None
                self.ws.close = None    
                del self.ws

            # forcebly set ws to None          
            self.ws = None
            self.ws = websocket.WebSocketApp(self.url, on_open=self.on_open, on_close=self.on_close,
                                                                 on_error=self.on_error, on_message=self.on_message)            
            self.run_thread = threading.Thread(target=ws_ib.run_forever, args=(None, {"cert_reqs": ssl.CERT_NONE}))
            self.run_thread.start()
            self.socket_connected = True
            print ('PolygonWebSocketClient - init_socket() completed.', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print ('PolygonWebSocketClient - init_socket() failed', e)
         
    # web socket
    def check_polygon_socket():
        if not self.polygon_connecting:
            try:
                # if we have not subscribed to all stocks, re connect
                if self.total_stocks_in_group != self.number_of_stocks_subscribed:
                    print ('check_polygon_socket - all stocks not subscribed, reconnecting . . .', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
                    #restart_candle_creation()
                else:
                    ws.send('')
                    print ('check_polygon_socket - polygon socket connected.', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
            except Exception as ex:
                print ("check_polygon_socket failed at", datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"), ex)
                print ('check_polygon_socket - socket not connected, reconnecting . . .')
                #restart_candle_creation()
            
        print ('check_polygon_socket completed.', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def run_forever(self):
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def run(self):
        try:
            self._run_thread = threading.Thread(target=self.run_forever)
            self._run_thread.start()
            print ('PolygonWebSocketClient - run() - completed.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ('PolygonWebSocketClient - run() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def close_connection(self):
        try:
            self.ws.close()
            if self._run_thread:
                self._run_thread.join()
            print ('PolygonWebSocketClient - close_connection() - completed.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ('PolygonWebSocketClient - close_connection() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        
    def subscribe(self, code):
        try:
            # first grab contract id
            con_id = self.get_stock(code)
            print ('PolygonWebSocketClient - subscribe() - stock code: {} - contract id: {}'.format(code, con_id))
            self.ws.send('smd+%s+{"fields":["31","83"]}' % con_id)
        except Exception as ex:
            print ('PolygonWebSocketClient - subscribe() - failed for stock {}'.format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def unsubscribe(self, code):
        try:
            # first grab contract id
            con_id = self.get_stock(code)
            print ('PolygonWebSocketClient - unsubscribe() - stock code: {} - contract id: {}'.format(code, con_id))
            self.ws.send('umd+%s{}' % con_id)
        except Exception as ex:
            print ('PolygonWebSocketClient - unsubscribe() - failed for stock {}'.format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_open(ws):
        print("PolygonWebSocketClient() - on_open()", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_error(ws, error):
        print("PolygonWebSocketClient() - on_error()", error, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    def on_close(ws):
        print("PolygonWebSocketClient() - process_ib_on_close()", datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))