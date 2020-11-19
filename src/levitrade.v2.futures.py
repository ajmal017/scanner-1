import time
import boto3
import time
import schedule
import json
import datetime
import dateutil.tz
import datetime;
import random
import requests
import uuid
import decimal
import inspect
import calendar
import random
import time
import os 
import threading
import numpy as np
import queue
import websocket

from random import seed
from random import choice
from threading import Thread
from datetime import timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from aiohttp import web
from datetime import timedelta
from math import sqrt
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

est_time_zone = dateutil.tz.gettz('US/Eastern')

s3 = boto3.client('s3')

dynamodb_eu_east_2 = boto3.resource('dynamodb', region_name='us-east-2')
dynamodb_us_west_2 = boto3.resource('dynamodb', region_name='us-west-2')

daily_settings_table = dynamodb_us_west_2.Table('daily_settings')
futures_settings_table = dynamodb_us_west_2.Table('futures_settings')
us_stock_symbols_table = dynamodb_us_west_2.Table('us_stock_symbols')

# daily momentum algorithm
open_trades_table = dynamodb_eu_east_2.Table('open_trades')
trade_history_table = dynamodb_eu_east_2.Table('trade_history')
scanner_stocks_table = dynamodb_eu_east_2.Table('scanner_stocks')

# volume algorithm
open_trades_volume_table = dynamodb_eu_east_2.Table('open_trades_volume')
trade_history_volume_table = dynamodb_eu_east_2.Table('trade_history_volume')
scanner_stocks_volume_table = dynamodb_eu_east_2.Table('scanner_stocks_volume')

e_mini = 0
market_direction = ''

# threading and scheduler
global_action = 'stop'
scheduler_started = False
did_action_close_connection = False
creating_5_min_candle = False
print_message = False
p_start = 0
p_end = 0

# holds high, open, close, and low for period
datenow = datetime.datetime.utcnow()
est_time_zone = dateutil.tz.gettz('US/Eastern')
                  
# secret manager functions

def getSecretString(secret_id):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1'
    )

    decoded_binary_secret = ''

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            decoded_binary_secret = eval(get_secret_value_response['SecretString'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    return decoded_binary_secret

# aurora functions

def execute_statement(sql, database_name, sql_parameters=[]):
    rds_client = boto3.client('rds-data', region_name='us-east-1')
    db_cluster_arn = 'arn:aws:rds:us-east-1:080872292485:cluster:levitrade-historical-data'
    db_credentials_secrets_store_arn = 'arn:aws:secretsmanager:us-east-1:080872292485:secret:levitrade_history-t2x8lU'

    response = rds_client.execute_statement(
        secretArn=db_credentials_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameters=sql_parameters
    )
    return response

def batch_execute_statement(sql, database_name, parameterSets):
    rds_client = boto3.client('rds-data', region_name='us-east-1')
    db_cluster_arn = 'arn:aws:rds:us-east-1:080872292485:cluster:levitrade-historical-data'
    db_credentials_secrets_store_arn = 'arn:aws:secretsmanager:us-east-1:080872292485:secret:levitrade_history-t2x8lU'

    response = rds_client.batch_execute_statement(
        secretArn=db_credentials_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameterSets=parameterSets
    )
    return response

# aurora 

# moves candle data to rds

def single_stock_candles_to_rds(code, candles, table, secret):
    if secret:
        sql_parameter_sets = []
        sql = 'insert into {} (key, code, date, week_number, month, year, timestamp, open, close, high, low, volume) values (:key, :code, :date, :week_number, :month, :year, :timestamp, :open, :close, :high, :low, :volume) on conflict (key) do update set open = excluded.open, close = excluded.close, high = excluded.high, low = excluded.low, volume = excluded.volume;'.format(table)

        for candle in candles:
            if not candle['in_rds']:
                datetime_object = datetime.datetime.strptime(candle['date'], '%Y-%m-%d %H:%M')
                # calculate the business week from iso calendar
                business_week = datetime_object.isocalendar()[1]
                key = '{}_{}'.format(code, candle['timestamp'])

                try:
                    entry = [
                        {'name':'key', 'value':{'stringValue': key}},
                        {'name':'code', 'value':{'stringValue': code}},
                        {'name':'date', 'value':{'stringValue': datetime_object.strftime('%Y-%m-%d %H:%M')}},
                        {'name':'week_number', 'value':{'doubleValue': business_week}},
                        {'name':'month', 'value':{'stringValue': datetime_object.strftime('%B')}},
                        {'name':'year', 'value':{'doubleValue':  int(datetime_object.year)}},
                        {'name':'timestamp', 'value':{'doubleValue': candle['timestamp']}},
                        {'name':'open', 'value':{'doubleValue': candle['open']}},
                        {'name':'close', 'value':{'doubleValue': candle['close']}},
                        {'name':'high', 'value':{'doubleValue': candle['high']}},
                        {'name':'low', 'value':{'doubleValue': candle['low']}},
                        {'name':'volume', 'value':{'doubleValue': candle['volume']}},
                    ]

                    sql_parameter_sets.append(entry)

                    if len(sql_parameter_sets) >= 1000:
                        print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets))) 
                        batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
                        sql_parameter_sets.clear()

                except Exception as e:
                    print('historical data sql processes failed,', e)
                    time.sleep(1)
                
                candle['in_rds'] = True
    
        # insert outstanding records
        try:
            if len(sql_parameter_sets) > 0:
                print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets)))  
                batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
        except Exception as e:
            print('historical data sql processes failed,', e)

    print ('single_stock_candles_to_rds completed for stock', code)

# moves all candles to rds

def all_5_min_candles_to_rds():
    print ('Executing all_5_min_candles_to_rds - 5 min candles to RDS')
    est_date = datetime.datetime.now(tz=est_time_zone) - timedelta(minutes=5)
    timestamp = int(est_date.timestamp()) * 1000
    date = est_date.strftime('%Y-%m-%d %H:%M')

    # try getting secret from secret manager, if succesful perform queries
    secret = getSecretString('levitrade_history')
    if secret:
        print ('Connecting to {}'.format(secret['host']))

        '''execute_statement("""drop table if exists 5_min_candles;""", 'historical_data')
        execute_statement("""CREATE TABLE IF NOT EXISTS candles_5_min(
            key text PRIMARY KEY,
            code text,
            date text,
            week_number decimal,
            month text,
            year decimal,
            timestamp decimal,
            open decimal,
            close decimal,
            high decimal,
            low decimal,
            volume decimal)""", 'historical_data')'''

        sql_parameter_sets = []
        sql = 'insert into candles_5_min (key, code, date, week_number, month, year, timestamp, open, close, high, low, volume) values (:key, :code, :date, :week_number, :month, :year, :timestamp, :open, :close, :high, :low, :volume) on conflict (key) do update set open = excluded.open, close = excluded.close, high = excluded.high, low = excluded.low, volume = excluded.volume;'

        for code in stocks_5_min_candles.keys():
            for candle in stocks_5_min_candles[code]:
                if not candle['in_rds']:
                    # calculate the business week from iso calendar
                    business_week = est_date.isocalendar()[1]
                    key = '{}_{}'.format(code, candle['timestamp'])

                    try:
                        entry = [
                            {'name':'key', 'value':{'stringValue': key}},
                            {'name':'code', 'value':{'stringValue': code}},
                            {'name':'date', 'value':{'stringValue': est_date.strftime('%Y-%m-%d %H:%M')}},
                            {'name':'week_number', 'value':{'doubleValue': business_week}},
                            {'name':'month', 'value':{'stringValue': est_date.strftime('%B')}},
                            {'name':'year', 'value':{'doubleValue':  int(est_date.year)}},
                            {'name':'timestamp', 'value':{'doubleValue': timestamp}},
                            {'name':'open', 'value':{'doubleValue': candle['open']}},
                            {'name':'close', 'value':{'doubleValue': candle['close']}},
                            {'name':'high', 'value':{'doubleValue': candle['high']}},
                            {'name':'low', 'value':{'doubleValue': candle['low']}},
                            {'name':'volume', 'value':{'doubleValue': candle['volume']}},
                        ]

                        sql_parameter_sets.append(entry)

                        if len(sql_parameter_sets) >= 1000:
                            print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets))) 
                            batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
                            sql_parameter_sets.clear()

                    except Exception as e:
                        print('historical data sql processes failed,', e)
                        time.sleep(1)

                    candle['in_rds'] = True

        # insert outstanding records
        try:
            if len(sql_parameter_sets) > 0:
                print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets)))  
                batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
        except Exception as e:
            print('historical data sql processes failed,', e)

        print ('Complete move to historical data.')

# init candle history

def init_candle_history(us_stocks):
    print ('Executing init_candle_history for stock')
    
    secret = getSecretString('levitrade_history')
    if secret:
        est_date = datetime.datetime.now(tz=est_time_zone)
        start_date = (est_date - timedelta(days=5))
        end_date = est_date
        print ('Retrieving history between', start_date.strftime('%Y-%m-%d'), 'and', end_date.strftime('%Y-%m-%d'))

        for stock in us_stocks:
            code = stock['code']

            try:
                select = "select * from candles_5_min where code=\'{}\' and timestamp >= \'{}\' and timestamp <= \'{}\';""".format(code, int(start_date.timestamp()) * 1000, int(end_date.timestamp()) * 1000)
                response = execute_statement(select, 'historical_data')
                candles = []

                for record in response['records']:
                    # [{'stringValue': 'MIME_1588343400000'}, {'stringValue': 'MIME'}, {'stringValue': '2020-05-01 10:30'}, {'stringValue': '18'}, {'stringValue': 'May'}, {'stringValue': '2020'}, {'stringValue': '1588343400000'}, {'stringValue': '39.43'}, {'stringValue': '39.51'}, {'stringValue': '39.53'}, {'stringValue': '39.43'}, {'stringValue': '3953'}]
                    new_5_min_candle = {
                        'timestamp': float(record[6]['stringValue']),
                        'date': record[2]['stringValue'],
                        'open': float(record[7]['stringValue']),
                        'close': float(record[8]['stringValue']),
                        'high': float(record[9]['stringValue']),
                        'low': float(record[10]['stringValue']),
                        'volume': float(record[11]['stringValue'])
                    }

                    candles.append(new_5_min_candle)
                
                print ('Retreived {} candles for stock'.format(len(candles)), code)
                stocks_5_min_candles[code] = sorted(candles, key = lambda i: i['timestamp'], reverse=True) 

            except Exception as e:
                print('init_candle_history failed for stock', code, e)
                time.sleep(1)

    print ('Complete init_candle_history at ', datetime.datetime.now(tz=est_time_zone).strftime('%Y-%m-%d %H:%M'))

# process raw data from socket into readable dict and makes update to live stock status

def process_message(message):
    try:
        msg_objects = json.loads(message)
        date_now = datetime.datetime.now(tz=est_time_zone)

    except Exception as e:
        print ('process_message failed', e, message)

def process_tick_data(ws, message):
    # TEMP: we need throttle 
    #Thread(target=process_message, args=[message]).start()
    process_message(message)

def process_on_error(ws, error):
    global td_connecting
    print("Socket error.", error)

    # reconnect
    if not did_action_close_connection and not td_connecting:
        print("Reconnecting.")
        restart()

def process_on_close(ws):
    global td_connecting
    print("Socket close.")

    # reconnect
    if not did_action_close_connection and not td_connecting:
        print("Reconnecting.")
        restart()
 
# create a single daily candle

def create_daily_candle(code):
    try:
        # set period candle to last tick close
        close_price = daily_stocks_status[code]['close']
        low_price = daily_stocks_status[code]['low']
        high_price = daily_stocks_status[code]['high']
        open_price = daily_stocks_status[code]['open']
        volume = daily_stocks_status[code]['daily_volume']

        est_date = datetime.datetime.now(tz=est_time_zone)
        date = est_date.strftime('%Y-%m-%d')
        timestamp = int(est_date.timestamp()) * 1000

        new_daily_candle = {
            'timestamp': timestamp,
            'date': date,
            'close': close_price,
            'low': low_price,
            'high': high_price,
            'open': open_price,
            'volume': volume,
        }

        return new_daily_candle
   
    except Exception as e:
        print ('create_daily_candle failed for stock'.format(code), e)

# create a single candle 5 min
def create_5_min_candle(code, candle_start_timestamp, candle_end_timestamp, date):
    try :
        volume = 0
        open_price = -1
        close_price = -1
        low_price = 10000000000000000
        high_price = -1

        for tick in price_ticks[code]:
            # we need to ensure all ticks are exactly within bounds of the 5 min period
            if tick['timestamp'] >= candle_start_timestamp and tick['timestamp'] <= candle_end_timestamp:
                if open_price == -1:
                    open_price = tick['price']
                if tick['price'] < low_price:
                    low_price = tick['price']
                if tick['price'] > high_price:
                    high_price = tick['price']

                close_price = tick['price']
                volume += tick['volume']
                # remove tick once added to 5 min candle
                price_ticks[code].remove(tick)

        # only create candles when volume is > 100
        if volume >= 100:
            new_5_min_candle = {
                'timestamp': candle_start_timestamp,
                'date': date,
                'close': close_price,
                'low': low_price,
                'high': high_price,
                'open': open_price,
                'volume': volume,
                'in_rds': False
            }
    
            # latest candle at start
            stocks_5_min_candles[code].insert(0, new_5_min_candle)
            return True

    except Exception as e:
        print ('create_5_min_candle failed for stock'.format(code), e)
    
    return False

# create daily candle for all stocks on market close

def create_daily_candles():
    secret = getSecretString('levitrade_history')
    timestamp = int(candle_start_time.timestamp()) * 1000

    if secret:
        sql_parameter_sets = []
        sql = 'insert into candles_daily (key, code, date, week_number, month, year, timestamp, open, close, high, low, volume) values (:key, :code, :date, :week_number, :month, :year, :timestamp, :open, :close, :high, :low, :volume) on conflict (key) do update set open = excluded.open, close = excluded.close, high = excluded.high, low = excluded.low, volume = excluded.volume;'

        try:
            for code in daily_stocks_status.keys():
                candle = create_daily_candle(code)

                # calculate the business week from iso calendar
                business_week = candle_start_time.isocalendar()[1]
                key = '{}_{}'.format(code, timestamp)

                entry = [
                    {'name':'key', 'value':{'stringValue': key}},
                    {'name':'code', 'value':{'stringValue': code}},
                    {'name':'date', 'value':{'stringValue': candle_start_time.strftime('%Y-%m-%d')}},
                    {'name':'week_number', 'value':{'doubleValue': business_week}},
                    {'name':'month', 'value':{'stringValue': candle_start_time.strftime('%B')}},
                    {'name':'year', 'value':{'doubleValue':  int(candle_start_time.year)}},
                    {'name':'timestamp', 'value':{'doubleValue': timestamp}},
                    {'name':'open', 'value':{'doubleValue': candle['open']}},
                    {'name':'close', 'value':{'doubleValue': candle['close']}},
                    {'name':'high', 'value':{'doubleValue': candle['high']}},
                    {'name':'low', 'value':{'doubleValue': candle['low']}},
                    {'name':'volume', 'value':{'doubleValue': candle['volume']}},
                ]

                sql_parameter_sets.append(entry)

                if len(sql_parameter_sets) >= 1000:
                    print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets))) 
                    batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
                    sql_parameter_sets.clear()

        except Exception as e:
            print('historical data sql processes failed,', e)
            time.sleep(1)

        # insert outstanding records
        try:
            if len(sql_parameter_sets) > 0:
                print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets)))  
                batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
        except Exception as e:
            print('historical data sql processes failed,', e)

        print ('Complete move to historical data.')
    
# create 5 min candles for all stocks 

def create_5_min_candles():
    global creating_5_min_candle

    creating_5_min_candle = True

    est_date = datetime.datetime.now(tz=est_time_zone)
    print ('5 min candles created at ', est_date.strftime('%Y-%m-%d %H:%M'))
    print ('5 min candles created between {} and {}'.format((est_date.replace(second=0, microsecond=0) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M'), est_date.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M')))
    # timestamp for candles represents start of a period, not end so we deduct 5 mins of current time
    candle_start_timestamp = int((est_date.replace(second=0, microsecond=0) - timedelta(minutes=5)).timestamp()) * 1000
    candle_end_timestamp = int((est_date.replace(second=0, microsecond=0)).timestamp()) * 1000
    num_tick_stocks = 0
    
    # every 5 min we create 
    for code in price_ticks.keys():
        # if either close, low or high not assigned in time period, we had no price updates, so create no candle
        if len(price_ticks[code]) > 0:
            created_candle = create_5_min_candle(code, candle_start_timestamp, candle_end_timestamp, (est_date - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M'))
            if created_candle:
                num_tick_stocks += 1

        if est_date >= market_open_time and est_date <= no_more_trades_time:
            # check stock is a scanner stock and has not previously traded today
            if daily_stocks_status[code]['scanner_stock'] or daily_stocks_status[code]['scanner_stock_volume']:
                if not past_traded(code) and less_30_trades():
                    Thread(target=check_trade_entry, args=[code]).start()

            # also check if stock was a volume mover
            if daily_stocks_status[code]['scanner_stock_volume']:
                Thread(target=check_trade_entry, args=[code]).start()

    print ('create_5_min_candles() - total stocks moved in 5 min period {}/{}'.format(num_tick_stocks, len(price_ticks.keys())))
    creating_5_min_candle = False
        
    # save daily status to text and upload to s3, this saves us time to load all US stocks status into memory
    Thread(target=save_candles_5_min_status).start()
    Thread(target=save_intraday_volume).start()
    Thread(target=all_5_min_candles_to_rds).start()

# move polygon history into aurora for 5 min candles

def create_5_min_candles_from_polygon_history(stocks):    
    secret = getSecretString('polygon')
    if secret:
        # get history for every stock from polygon http api
        for key, value in stocks:
            try:
                # global_action used to enfore complete stop on all threads
                if 'stop' in global_action:
                    return

                code = key
                candles = polygon_io_handler(code, 5, 'minute', secret['polygon_api_key'])
                stocks_5_min_candles[code] = candles

                # move all 5 min candles to RDS for stock
                single_stock_candles_to_rds(code, candles, 'candles_5_min', secret)

            except Exception as e:
                print ('create_5_min_candles_from_polygon_history failed for stock {},'.format(code), e)

def update_dynamo_tables():
    try:
        
    except Exception as ex:
        print ("update_dynamo_tables failed at", datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"), ex)
    
    print("Completed update_dynamo_tables at", datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d  %H:%M:%S"))

# update data to live tables so website updates
def update():
    update_daily_trend()
    # scheduler events aren't pushed to background, we move scanner updates to background so updating dynamo tables doesn't block
    Thread(target=update_scanner).start()

# reset daily stock status

def reset():
    
    try:
        
    except Exception as e:
        print ('reset_candle_creation failed', e)

    print ('reset_candle_creation at', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

def start():
    global sched
    global market_open_time
    global market_close_time
    global force_close_trades_time
    global no_more_trades_time
    global candle_start_time
    global candle_stop_time
    global scheduler_started
    global td_connecting

    date_now = datetime.datetime.now(tz=est_time_zone)

    init_socket()

    try:
        # subscribe to 'A' - aggregate 1 sec per US stock
        subscribe('T.{}'.format(code))
        number_of_stocks_subscribed += 1

    except Exception as e:
        print ('start() failed', e)

    # we want to complete finish start function before we allow another connection
    td_connecting = False

    print ('start() at', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

def stop():
    global sched
    global ws
    global did_action_close_connection

    # close socket connection
    did_action_close_connection = True

    ws.close()
    ws.on_message = None
    ws.on_open = None
    ws.close = None    
    del ws
    # forcebly set ws to None          
    ws = None

    did_action_close_connection = False
    print ('stop() at', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

def restart():
    global sched
    global scheduler_started
    global td_connecting

    td_connecting = True
    init_socket()

    try:

    except Exception as e:
        print ('restart failed', e)

    # polygon connection completely finishes after we have subscribed to all stocks 
    # this is used to force throttling on socket connection 
    td_connecting = False

    print ('restart at', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# handles requests for candle creation

async def handle_action (request):
    params = await request.json()
    
    if 'start' in params['action']: 
        thread = threading.Thread(target=start)
    elif 'stop' in params['action']: 
        thread = threading.Thread(target=close_open_trades)
    elif 'reset' in params['action']: 
        thread = threading.Thread(target=reset)

    thread.start()

    return web.Response(text=json.dumps({'status': 'Committed candle action {} at {}.'.format(params['action'], datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))}))

# handles global actions
def handle_stop (request):
    global ws
    global td_connecting

    if not td_connecting:
        try:
            ws.close()
        except Exception as e:
            print ('handle_stop failed', e)

    return web.Response(text=json.dumps({'status': 'handle_stop at {}, td_connecting is {}.'.format(datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"), td_connecting)}))

# web socket

def check_socket():
    global td_connecting
    if not td_connecting:
        try:
            ws.send('')
            print ('check_socket - polygon socket connected.', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as ex:
            print ("check_socket failed at", datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"), ex)
            print ('check_socket - socket not connected, reconnecting . . .')
            restart()
        
    print ('check_socket completed.', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# creates a new global socket connection to polygon
def init_socket():
    global ws
    global _authenticated
    global _connecting
    global socket_connected
    global td_connecting

    try:
        cluster = STOCKS_CLUSTER
        host = DEFAULT_HOST
        url = f"wss://{host}/{cluster}"

        print ('Polygon url', url)

        _authenticated = threading.Event()

        if socket_connected:
            print ('init_socket - socket previously connected, deleting previous socket.')
            ws.on_message = None
            ws.on_open = None
            ws.close = None    
            del ws

        # forcebly set ws to None          
        ws = None
        ws = websocket.WebSocketApp(url, on_message = process_tick_data, on_error = process_on_error, on_close = process_on_close)
        #websocket.enableTrace(True)
        ws.on_open = on_open
        thread = threading.Thread(target=ws.run_forever)
        thread.start()
        socket_connected = True
        
        # remove previous job on previous socket
        for job in sched.get_jobs():
            if 'check_socket' in job.id:
                print ('init_socket - removing job \'check_socket\' from previous socket connection.')
                sched.remove_job('check_socket')
                break

        # sometimes socket on close is not called when the socket closes so we must run a perodic check
        # https://github.com/websocket-client/websocket-client/issues/452
        sched.add_job(check_socket, CronTrigger.from_crontab('*/1 * * * *', timezone="US/Eastern"), id='check_socket')

        print ('init_socket completed.', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
    
    except Exception as e:
        print ('init_socket failed', e)

def on_open(ws):
    print ('Polygon socket open,', datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
    authenticate(ws)
    
def authenticate(ws):
    auth_key = '88MjbFkkupis8_V_n4pkAGyzOEGIHWa__hOzdM'
    ws.send('{"action":"auth","params":"%s"}' % auth_key)

def format_params(params):
    return ",".join(params)

def subscribe(*params):
    global ws
    #print ('subscribe', params, datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
    # using a threading event to force wait on subscriptions
    _authenticated.wait()
    sub_message = '{"action":"subscribe","params":"%s"}' % format_params(params)
    ws.send(sub_message)

async def handle_td():
    params = await request.json()
    code = params['code']
    frequency = params['frequency']
    period = params['period']
    service = params['service']
    request_chart = []

    td_auth_reponse = futures_settings_table.get_item(
        Key={
            'key': 'td_auth'
        }
    )

    if 'Item' in td_auth_reponse.keys():
        data = json.loads(td_auth_reponse['Item']['value'])

        # get user principals
        result = requests.post("https://832h5hua0j.execute-api.us-west-2.amazonaws.com/v1/td-principals", data=json.dumps(data))
        userPrincipalsResponse = result.json()
        
        # Converts ISO-8601 response in snapshot to ms since epoch accepted by Streamer
        pattern = '%Y-%m-%dT%H:%M:%S%z'
        timestamp = int(datetime.datetime.strptime(
            userPrincipalsResponse['streamerInfo']['tokenTimestamp'], pattern).timestamp() * 1000)

        credentials = {
            "userid": userPrincipalsResponse['accounts'][0]['accountId'],
            "token": userPrincipalsResponse['streamerInfo']['token'],
            "company": userPrincipalsResponse['accounts'][0]['company'],
            "segment": userPrincipalsResponse['accounts'][0]['segment'],
            "cddomain": userPrincipalsResponse['accounts'][0]['accountCdDomainId'],
            "usergroup": userPrincipalsResponse['streamerInfo']['userGroup'],
            "accesslevel": userPrincipalsResponse['streamerInfo']['accessLevel'],
            "authorized": "Y",
            "timestamp": timestamp,
            "appid": userPrincipalsResponse['streamerInfo']['appId'],
            "acl": userPrincipalsResponse['streamerInfo']['acl'],
        }

        login_request = {
            "requests": [
                {
                    "service": "ADMIN",
                    "command": "LOGIN",
                    "requestid": 0,
                    "account": userPrincipalsResponse['accounts'][0]['accountId'],
                    "source": userPrincipalsResponse['streamerInfo']['appId'],
                    "parameters": {
                        "credential": urlencode(credentials),
                        "token": userPrincipalsResponse['streamerInfo']['token'],
                        "version": "1.0"
                    }
                }
            ]
        }
        login_request_data = json.dumps(login_request, separators=(',', ':'))

        if 'futures' in service:
            request_chart = {
                "requests": [
                    {
                        "service": "CHART_HISTORY_FUTURES",
                        "requestid": "2",
                        "command": "GET",
                        "account": userPrincipalsResponse['accounts'][0]['accountId'],
                        "source": userPrincipalsResponse['streamerInfo']['appId'],
                        "parameters": {
                            "symbol": code,
                            "frequency": frequency,
                            "period": period,
                            "fields": "0,1,2,3,4,5"
                        }
                    }
                ]
            }
        elif 'equity' in service:
            request_chart = {
                "requests": [
                    {
                        "service": "CHART_EQUITY",
                        "requestid": "2",
                        "command": "SUBS",
                        "account": userPrincipalsResponse['accounts'][0]['accountId'],
                        "source": userPrincipalsResponse['streamerInfo']['appId'],
                        "parameters": {
                            "keys": code,
                            "fields": "0,1,2,3,4,5,6,7,8"
                        }
                    }
                ]
            }

        request_chart_data = json.dumps(
            request_chart, separators=(',', ':'))

        async with websockets.connect("wss://" + userPrincipalsResponse['streamerInfo']['streamerSocketUrl'] + "/ws") as websocket:
            # login
            await websocket.send(login_request_data)
            login_data = await websocket.recv()
            # get futures data
            await websocket.send(request_chart_data)
            futures_json = await websocket.recv()
            candles = json.loads(futures_json)['snapshot'][0]['content'][0]['3'][::-1] 

        return web.Response(text=json.dumps(candles))

def update_daily_trend():
    datenow = datetime.datetime.now(tz=est_time_zone)
    date = datenow.strftime("%Y-%m-%d")
    today110pm = datenow.replace(hour=13, minute=10, second=0, microsecond=0)
    today1010am = datenow.replace(hour=10, minute=10, second=0, microsecond=0)

    try:
        # get the endpoint of the emini environment
        response = futures_settings_table.get_item(
            Key={
                'key': 'futures_endpoint',
            }
        )
        
        futures_endpoint = 'http://{}/td'.format(response['Item']['value'])
        
        # check if we have emini_trend
        response = daily_settings_table.get_item(
            Key={
                'key': 'emini_trend',
            }
        )

        data = {
            "code": "/ES",
            "frequency": "d1",
            "period": "w2",
            "service": "futures"
        }
        
        tdReply = requests.post(futures_endpoint, data=json.dumps(data), headers={'Content-type': 'application/json', 'Accept': 'text/plain'})
        td_data = json.loads(tdReply.text)
        latest = td_data[0]['4']
        previous = td_data[1]['4']
        result = latest - previous
         
        print('previous', td_data[0]['0'], previous, 'latest', td_data[len(td_data) - 1]['0'], latest)
        
        trend = 'sell'
        
        if result >= 0:
            trend = 'buy'
        
        # make sure we dont enable any open rules until after 10:10am
        if datenow > today1010am: 
            if 'Item' in response.keys():
                if trend not in response['Item']['value']:
                    trend = 'both'
                
        # update emini direction
        response = daily_settings_table.put_item(
            Item={
                'key': 'emini_trend',
                'value': trend
            })
            
        response = daily_settings_table.put_item(
            Item={
                'key': 'emini_value',
                'value': decimal.Decimal(round(latest, 2))
            })
               
    except Exception as ex:
        print ("This is an error message at ", date, ex)
        
    print("Updated daily trend table at", date)

def reauth_td():
    date = datetime.datetime.now(tz=est_time_zone).strftime("%Y-%m-%d")
    
    try:
        response = futures_settings_table.get_item(
            Key={
                'key': 'td_auth',
            }
        )

        if 'Item' in response.keys():
            data = json.loads(response['Item']['value'])
            longReply = lambda_client.invoke(FunctionName='levatrade-td-principals', InvocationType='RequestResponse', Payload=json.dumps(data))
            result = json.loads(longReply['Payload'].read().decode())
               
    except Exception as ex:
        print ("This is an error message at ", date, ex)
        
    print("Updated td auth at", date)

def auth_td():
    datenow = datetime.datetime.now(tz=est_time_zone)
    date = datenow.strftime("%Y-%m-%d")

    secret = getSecretString('levitrade_history')
    if secret:
        td_auth_endpoint = 'http://54.174.182.208:5000/auth'
        data = {
            'username': secret['username'],
            'account_number': secret['account_number'],
            'password': secret['password'],
            'client_id': secret['client_id']
        }
        headers = {
            'ContentType': 'application/json'
        }
        
        td_auth_data = requests.post(td_auth_endpoint, data=json.dumps(data), headers=headers)

        auth_data = json.loads(td_auth_data.text)
        auth_data['use_code'] = False
        auth_data['return_uri'] = 'http://localhost'
        auth_data['apikey'] = '{}@AMER.OAUTHAP'.format(event['client_id'])
        auth_data['client_id'] = '{}@AMER.OAUTHAP'.format(event['client_id'])
        longReply = lambda_client.invoke(FunctionName='levatrade-td-principals', InvocationType='RequestResponse', Payload=json.dumps(auth_data))
        result = json.loads(longReply['Payload'].read().decode())

        print('levatrade_auth_td() completed successfully at', date)

# TEMP: implementation of socket handling, will soon move to seperate class
if __name__ == "__main__":
    socket_connected = False
    td_connecting = True

    sched = BackgroundScheduler(timezone="US/Eastern")

    sched.add_job(start, CronTrigger.from_crontab('0 2 * * MON', timezone="US/Eastern"), id='start')
    sched.add_job(stop, CronTrigger.from_crontab('0 22 * * FRI', timezone="US/Eastern"), id='stop')
    sched.add_job(create_5_min_candles, CronTrigger.from_crontab('*/5 4-22 * * MON-FRI', timezone="US/Eastern"), id='create_5_min_candles')
    sched.add_job(update, CronTrigger.from_crontab('*/1 4-22 * * *', timezone="US/Eastern"), id='update')
    sched.add_job(auth_td, CronTrigger.from_crontab('0 12 * * MON-FRI', timezone="US/Eastern"), id='auth_td')
    sched.add_job(reauth_td, CronTrigger.from_crontab('*/5 4-20 * * MON-FRI', timezone="US/Eastern"), id='reauth_td')

    sched.configure()
    sched.start()
    scheduler_started = True

    app = web.Application()

    # post
    app.add_routes([web.post("/action", handle_action)])
    app.add_routes([web.post("/stop", handle_stop)])

    web.run_app(app, port=5000)