import signal
import threading
import websocket
import json
import os
import base64 
import boto3 
import http.client
import datetime
import dateutil.tz
import requests
import threading
import json

from signalr_aio import Connection
from base64 import b64decode
from zlib import decompress, MAX_WBITS
from threading import Thread
from requests_aws4auth import AWS4Auth
from typing import Optional, Callable
from boto3 import Session as AWSSession
from gql import gql
from gql.client import Client
from gql.transport.requests import RequestsHTTPTransport
from secret_manager_client import SecretManagerClient

eastern_time_zone = dateutil.tz.gettz('US/Eastern')

'''def test_get():
    # get_appsync_obj is a GraphQL query in string form.
    # You can use the query strings from AppSync schema.
    client = make_client()
    params = {'id': 1235}
    resp = client.execute(gql(get_appsync_obj),
                          variable_values=json.dumps(params))
    return resp

def test_mutation():
    client = make_client()
    params = {'id': 1235, 'state': 'DONE!'}
    resp = client.execute(gql(update_appsync_obj), variable_values=json.dumps({'input': params}))
    return resp'''

class SignalRClient:
    def __init__(self, host: str, apiKey: str, process_message: Optional[Callable[[str], None]] = None,
                 on_close: Optional[Callable[[websocket.WebSocketApp], None]] = None,
                 on_error: Optional[Callable[[websocket.WebSocketApp, str], None]] = None):
        # Create connection
        # Users can optionally pass a session object to the client, e.g a cfscrape session to bypass cloudflare.
        self.connection = Connection('https://beta.bittrex.com/signalr', session=None)
        # Register hub
        self.hub = self.connection.register_hub('c2')
        # Assign debug message handler. It streams unfiltered data, uncomment it to test.
        self.connection.received += self.on_debug
        # Assign error handler
        self.connection.error += self.on_error

        # Assign hub message handler
        self.hub.client.on('uE', self.on_message)
        self.hub.client.on('uS', self.on_message)

        # Send a message
        self.hub.server.invoke('SubscribeToExchangeDeltas', 'BTC-ETH')
        self.hub.server.invoke('SubscribeToSummaryDeltas')
        self.hub.server.invoke('queryExchangeState', 'BTC-NEO')

        # Start the client
        self.connection.start()
        print ("SignalRClient - __init__ complete.")

    def process_message(message):
        deflated_msg = decompress(b64decode(message), -MAX_WBITS)
        return json.loads(deflated_msg.decode())

    # Create debug message handler.
    async def on_debug(**msg):
        # In case of 'queryExchangeState'
        if 'R' in msg and type(msg['R']) is not bool:
            decoded_msg = self.process_message(msg['R'])
            print(decoded_msg)

    # Create error handler
    async def on_error(msg):
        print(msg)

    # Create hub message handler
    async def on_message(msg):
        decoded_msg = self.process_message(msg[0])
        print(decoded_msg)

    # handling batch amounts

    def mutate_batch(self, operation, query, items):
        try:
            batch = []
            for item in items:
                batch.append(item)
                # put has a 25 limit
                if len(batch) == 25:
                    # update momentum stocks in dynamo
                    graphql_mutation = {
                        'operation': operation,
                        'query': query,
                        'variables': '{ "input": ' + str(json.dumps(batch)) + ' }'
                    }
                    self.mutate(graphql_mutation)
                    batch.clear()
            
            graphql_mutation = {
                'operation': operation,
                'query': query,
                'variables': '{ "input": ' + str(json.dumps(batch)) + ' }'
            }
            self.mutate(graphql_mutation)
            batch.clear()
        except Exception as ex:
            print ("SignalRClient - mutate_batch() - {} failed".format(graphql_mutation['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    def mutate(self, graphql_mutation):
        try:
            self.graphql_updates.append(graphql_mutation)
            #response = requests.post(self.API_URL, data=json.dumps(graphql_mutation), headers=self.app_sync_http_header)
            #resp = self.client.execute(gql(graphql_mutation['query']), variable_values=graphql_mutation['variables'])
            #return resp
            print ("SignalRClient - mutate() - {} completed".format(graphql_mutation['operation']), datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ("SignalRClient - mutate() - {} failed".format(graphql_mutation['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    # graph ql update functions

    def start_graphql_updates(self, sched):
        sched.add_job(self.initiate_graphql_updates, 'interval', seconds=1, id='initiate_graphql_updates')

    def stop_graphql_updates(self):
        sched.remove_job('SignalRClient - initiate_graphql_updates')

    def initiate_graphql_updates(self):
        try:
            Thread(target=self.run_graphql_updates).start()
        except Exception as ex:
            print ('SignalRClient - initiate_graphql_updates() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    # updates throttled at 3 request per second
    def run_graphql_updates(self):
        try:
            if not self.graphql_update_processing and len(self.graphql_updates) > 0:
                print ('SignalRClient - run_graphql_updates() - Processing {} updates.'.format(len(self.graphql_updates)))
                self.graphql_update_processing = True
                for graphql_update in self.graphql_updates:
                    self.handle_graphql_update(graphql_update)
                self.graphql_update_processing = False
            else:
                print ('SignalRClient - run_graphql_updates() - currently {} processing updates, waiting to next interval.'.format(len(self.graphql_updates)))
        except Exception as ex:
            print ('SignalRClient - run_graphql_updates() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def handle_graphql_update(self, graphql_update):
        graphql_update_failed = False
        graphql_update_failed_message = ''

        try:
            response = requests.post(self.API_URL, data=json.dumps(graphql_update), headers=self.app_sync_http_header, timeout=10)
            print ('SignalRClient - handle_graphql_update() - completed update {}'.format(graphql_update['operation']), datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as ex:
            graphql_update_failed = True
            print ('SignalRClient - handle_graphql_update() handle_graphql_update failed {}'.format(graphql_update['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        
        self.graphql_updates.remove(graphql_update)
