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

class AppSyncClient:
    # secret manager functions

    # TODO: Either an instance of the client couples 1:1 with the cluster or an instance of the Client couples 1:3 with
    #  the 3 possible clusters (I think I like client per, but then a problem is the user can make multiple clients for
    #  the same cluster and that's not desirable behavior,
    #  somehow keeping track with multiple Client instances will be the difficulty)
    def __init__(self, host: str, apiKey: str, process_message: Optional[Callable[[str], None]] = None,
                 on_close: Optional[Callable[[websocket.WebSocketApp], None]] = None,
                 on_error: Optional[Callable[[websocket.WebSocketApp, str], None]] = None):
        self.API_URL = 'https://{}.appsync-api.us-west-2.amazonaws.com/graphql'.format(host)
        HOST = self.API_URL.replace('https://','').replace('/graphql','')
        WSS_URL = self.API_URL.replace("https","wss").replace("appsync-api","appsync-realtime-api")
        APP_SYNC_WSS_HEADER = {
            "host": HOST,
            "x-api-key": ''
        }
        APP_SYNC_HTTP_HEADER = {
            'Content-type': 'application/graphql', 
            'x-api-key': '',
            'host': HOST
        }

        self.graphql_update_processing = False
        self.graphql_updates = []
        self._host = HOST
        self.secretManagerClient = SecretManagerClient('us-east-1')
        secret = self.secretManagerClient.getSecretString('levitrade-auth')
        if secret:
            aws = AWSSession(aws_access_key_id=secret['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=secret['AWS_SECRET_ACCESS_KEY'],
                            region_name='us-east-1')
            auth = AWS4Auth(secret['AWS_ACCESS_KEY_ID'], secret['AWS_SECRET_ACCESS_KEY'], 'us-east-1', 'appsync')
            transport = RequestsHTTPTransport(url=self.API_URL,
                                            headers={
                                                'Accept': 'application/json',
                                                'Content-Type': 'application/json',
                                            },
                                            auth=auth)
            self.client = Client(transport=transport, fetch_schema_from_transport=True)

            # setup http
            self.app_sync_http_header = APP_SYNC_HTTP_HEADER
            self.app_sync_http_header['x-api-key'] = secret[apiKey]
            self.conn = http.client.HTTPSConnection(HOST, 443)

            # setup wss
            self.app_sync_wss_header = APP_SYNC_WSS_HEADER
            self.app_sync_wss_header['x-api-key'] = secret[apiKey]
            base64_bytes = base64.b64encode(str(self.app_sync_wss_header).encode("ascii")) 
            base64_string = base64_bytes.decode("ascii") 
            self.encoded_header = base64_string
            self.url = '{}?header={}&payload=e30='.format(WSS_URL, self.encoded_header)
            '''self.ws: websocket.WebSocketApp = websocket.WebSocketApp(self.url, on_open=self._default_on_open(),
                                                                    on_close=self._default_on_close,
                                                                    on_error=self._default_on_error,
                                                                    on_message=self._default_on_message())
            self.process_message = process_message
            self.ws.on_close = on_close
            self.ws.on_error = on_error

            # being authenticated is an event that must occur before any other action is sent to the server
            self._authenticated = threading.Event()
            # self._run_thread is only set if the client is run asynchronously
            self._run_thread: Optional[threading.Thread] = None

            # TODO: this probably isn't great design.
            #  If the user defines their own signal handler then this will gets overwritten.
            #  We still need to make sure that killing, terminating, interrupting the program closes the connection
            #signal.signal(signal.SIGINT, self._cleanup_signal_handler())
            #signal.signal(signal.SIGTERM, self._cleanup_signal_handler())'''

            print ("AppSyncClient - __init__ complete.")

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
            print ("AppSyncClient - mutate_batch() - {} failed".format(graphql_mutation['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    def mutate(self, graphql_mutation):
        try:
            self.graphql_updates.append(graphql_mutation)
            #response = requests.post(self.API_URL, data=json.dumps(graphql_mutation), headers=self.app_sync_http_header)
            #resp = self.client.execute(gql(graphql_mutation['query']), variable_values=graphql_mutation['variables'])
            #return resp
            print ("AppSyncClient - mutate() - {} completed".format(graphql_mutation['operation']), datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
        except Exception as ex:
            print ("AppSyncClient - mutate() - {} failed".format(graphql_mutation['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    # graph ql update functions

    def start_graphql_updates(self, sched):
        sched.add_job(self.initiate_graphql_updates, 'interval', seconds=1, id='initiate_graphql_updates')

    def stop_graphql_updates(self):
        sched.remove_job('AppSyncClient - initiate_graphql_updates')

    def initiate_graphql_updates(self):
        try:
            Thread(target=self.run_graphql_updates).start()
        except Exception as ex:
            print ('AppSyncClient - initiate_graphql_updates() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    # updates throttled at 3 request per second
    def run_graphql_updates(self):
        try:
            if not self.graphql_update_processing and len(self.graphql_updates) > 0:
                print ('AppSyncClient - run_graphql_updates() - Processing {} updates.'.format(len(self.graphql_updates)))
                self.graphql_update_processing = True
                for graphql_update in self.graphql_updates:
                    self.handle_graphql_update(graphql_update)
                self.graphql_update_processing = False
            else:
                print ('AppSyncClient - run_graphql_updates() - currently {} processing updates, waiting to next interval.'.format(len(self.graphql_updates)))
        except Exception as ex:
            print ('AppSyncClient - run_graphql_updates() - failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

    def handle_graphql_update(self, graphql_update):
        graphql_update_failed = False
        graphql_update_failed_message = ''

        try:
            response = requests.post(self.API_URL, data=json.dumps(graphql_update), headers=self.app_sync_http_header, timeout=10)
            print ('AppSyncClient - handle_graphql_update() - completed update {}'.format(graphql_update['operation']), datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as ex:
            graphql_update_failed = True
            print ('AppSyncClient - handle_graphql_update() handle_graphql_update failed {}'.format(graphql_update['operation']), ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        
        self.graphql_updates.remove(graphql_update)
