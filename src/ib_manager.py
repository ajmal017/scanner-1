import signal
import threading
import json
import datetime
import dateutil.tz
import requests
import boto3
import urllib3
import websocket

from secret_manager_client import SecretManagerClient 
from typing import Optional, Callable

eastern_time_zone = dateutil.tz.gettz('US/Eastern')
urllib3.disable_warnings()

class IBManager:
    def __init__(self):
        self.host = "54.174.182.208:5001"
        self.secretManagerClient = SecretManagerClient('us-east-1')
        print ("IBManager -  - __init__ complete.")

    # ib functions

    def auth():
        try:
            # TEMP - sometimes it takes two to knock out other users - still investigating
            td_auth_data = requests.post('http://{}/auth'.format(self.host), data={}, headers={'ContentType': 'application/json'})
        except Exception as e:
            print ('ib_auth() failed', e)

        print ('IBManager - ib_auth() completed at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def start():
        try:
            ib_auth_data = requests.post('http://{}/start'.format(self.host), data={}, headers={'ContentType': 'application/json'})
            print ('ib_start()', ib_auth_data.text)
        except Exception as e:
            print ('ib_start() failed', e)
        print ('IBManager - ib_start() completed at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def stop():
        try:
            ib_auth_data = requests.post('http://{}/stop'.format(self.host), data={}, headers={'ContentType': 'application/json'})
            print ('ib_stop()', ib_auth_data.text)
        except Exception as e:
            print ('ib_stop() failed', e)
        print ('IBManager - ib_stop() completed at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def get_contract_id(self, code):
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
                stocks = json.loads(stocks_response.text)
                return stocks[0]['conid']
        except Exception as ex:
            print ("IBManager - get_stock() - failed for stock {}".format(code), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        return -1

    def execute_order(code, action, quantity):
        try:
            trade_action = 'SELL'
            if 'long' in action:
                trade_action = 'BUY'

            order_data = requests.post('http://{}/order'.format(self.host), data=json.dumps({
                "code": code,
                "action": trade_action,
                "ib_action": "entry",
                "secType": "STK",
                "quantity": quantity
            }), headers={
                'ContentType': 'application/json'
            })
            print ('IBManager - ib_execute_order() {}'.format(code), order_data.text)
            print ('IBManager - ib_execute_order() completed at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print ('IBManager - ib_execute_order() failed', e)

    def exit_order(code, action, quantity):
        try:
            # temp just reverse trade action
            trade_action = 'SELL'
            if 'short' in action:
                trade_action = 'BUY'

            order_data = requests.post('http://{}/order'.format(self.host), data=json.dumps({
                "code": code,
                "action": trade_action,
                "ib_action": "exit",
                "secType": "STK",
                "quantity": quantity
            }), headers={'ContentType': 'application/json'})
            print ('IBManager - ib_exit_order() {}'.format(code), order_data.text)
            print ('IBManager - ib_exit_order() completed at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print ('ib_exit_order() failed', e, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def clean_up():
        try:
            requests.get('http://{}/clean'.format(self.host), headers={'ContentType': 'application/json'})
        except Exception as e:
            print ('IBManager - ib_clean_up() failed', e, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))