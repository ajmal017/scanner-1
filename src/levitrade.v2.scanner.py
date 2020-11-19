import time
import boto3
import time
import schedule
import json
import datetime
import dateutil.tz
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
import csv
import ssl
import socket 

from random import seed
from random import choice
from threading import Thread
from datetime import timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from aiohttp import web
from datetime import timedelta
from math import sqrt, ceil, floor
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from signal_r_client import SignalRClient
from appsync_client import AppSyncClient
from ib_websocket_client import IBWebSocketClient
from secret_manager_client import SecretManagerClient
from ib_manager import IBManager

eastern_time_zone = dateutil.tz.gettz('US/Eastern')

# dyname for global and futures
dynamodb_us_west_2 = boto3.resource('dynamodb', region_name='us-west-2')
daily_settings_table = dynamodb_us_west_2.Table('daily_settings')
us_stock_symbols_table = dynamodb_us_west_2.Table('us_stock_symbols')
ib_orders_table = dynamodb_us_west_2.Table('ib_orders')
ib_order_failures_table = dynamodb_us_west_2.Table('ib_order_failures')

s3 = boto3.client('s3')
ses = boto3.client('ses', region_name='us-west-2')
sns = boto3.client('sns', region_name='us-west-2')

# get ET utc offset
utc_offset = (datetime.datetime.now(tz=eastern_time_zone).replace(tzinfo=None) - datetime.datetime.utcnow()).total_seconds()

e_mini = 0
scanner_tick_checks = 0
market_direction = ''
# threading and scheduler
global_action = 'stop'
scheduler_started = False
did_action_close_polygon_connection = False
removing_ticks_in_progress = False

# polygon web socket variables
STOCKS_CLUSTER = "stocks"
FOREX_CLUSTER = "forex"
CRYPTO_CLUSTER = "crypto"
POLYGON_HOST = "socket.polygon.io"

# holds high, open, close, and low for period
stocks_5_min_candles = {}

# live trading
open_trades = {}
historical_trades = {}
daily_stocks_status = {}
price_ticks = {}
ticks_to_move = []

# scanners
daily_stocks_status = {}
daily_stocks_status = {}

datenow = datetime.datetime.utcnow()
eastern_time_zone = dateutil.tz.gettz('US/Eastern')
start_trading_time = {}
market_close_time = {}
candle_start_time = {}
candle_stop_time = {}

# grow and fall colors
col_grow_above = '#26A69A'
col_grow_below = '#FFCDD2'
col_fall_above = '#B2DFDB'
col_fall_below = '#EF5350'
col_macd = '#0094ff'
col_signal = '#ff6a00'

# buy patterns for ttm and macd, index 3 is the most recent
macd_ttm_buy_patterns = [
    {
        'macd': [col_grow_below, col_grow_below, col_grow_above, col_grow_above],
        'ttm': [col_grow_below, col_grow_below, col_grow_below, col_grow_below]
    },
    {
        'macd': [col_grow_below, col_grow_above, col_grow_above, col_grow_above],
        'ttm': [col_grow_above, col_grow_above, col_fall_above, col_grow_above]
    },
    {
        'macd': [col_fall_below, col_grow_above, col_grow_above, col_grow_above],
        'ttm': [col_grow_below, col_grow_below, col_grow_below, col_grow_below]
    },
    {
        'macd': [col_fall_below, col_fall_below, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_grow_below]
    },
    {
        'macd': [col_fall_below, col_grow_below, col_grow_above, col_grow_above],
        'ttm': [col_fall_below, col_fall_below, col_grow_above, col_grow_above]
    },
    {
        'macd': [col_fall_above, col_grow_above, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_grow_above]
    },
    {
        'macd': [col_grow_above, col_grow_above, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_grow_above]
    },
    {
        'macd': [col_grow_above, col_fall_above, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_grow_above, col_grow_above, col_grow_above]
    },
    {
        'macd': [col_grow_below, col_fall_below, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_fall_above, col_grow_above, col_grow_above]
    },
    {
        'macd': [col_fall_below, col_grow_below, col_grow_above, col_grow_above],
        'ttm': [col_fall_above, col_grow_above, col_grow_above, col_grow_above]
    }
]

# sell patterns for ttm and macd
macd_ttm_sell_patterns = [
    {
        'macd': [col_fall_above, col_fall_above, col_fall_below, col_fall_below],
        'ttm': [col_fall_above, col_fall_above, col_fall_below, col_fall_below]
    },
    {
        'macd': [col_fall_below, col_fall_below, col_grow_below, col_fall_above],
        'ttm': [col_fall_above, col_fall_above, col_fall_below, col_fall_below]
    },
    {
        'macd': [col_fall_below, col_fall_below, col_fall_below, col_fall_below],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_fall_below]
    },
    {
        'macd': [col_grow_above, col_fall_above, col_grow_below, col_fall_below],
        'ttm': [col_grow_below, col_grow_below, col_grow_below, col_grow_below]
    },
    {
        'macd': [col_grow_below, col_fall_below, col_fall_below, col_fall_below],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_fall_below]
    },
    {
        'macd': [col_fall_above, col_fall_above, col_fall_above, col_fall_below],
        'ttm': [col_fall_above, col_fall_above, col_fall_above, col_fall_above]
    },
    {
        'macd': [col_fall_above, col_grow_above, col_fall_above, col_fall_above],
        'ttm': [col_grow_below, col_grow_below, col_fall_below, col_fall_below]
    }
]

# retrieve hostname and ip for eb instance

def get_host_name_IP(): 
    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        external_ip = requests.get('https://checkip.amazonaws.com').text.strip()

        print("get_host_name_IP() - hostname {}, internal IP {}, external IP {}".format(host_name, host_ip, external_ip))

        return {
            'host_name': host_name,
            'internal_ip': host_ip,
            'external_ip': external_ip,
        }

    except Exception as ex: 
        print("get_host_name_IP() - Unable to get Hostname and IP", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M')) 

def isSupport(candles, i):
  support = candles[i]['low'] < candles[i-1]['low'] and candles[i]['low'] < candles[i+1]['low'] and candles[i+1]['low'] < candles[i+2]['low'] and candles[i-1]['low'] < candles[i-2]['low']
  return support

def isResistance(candles, i):
  resistance = candles[i]['high'] > candles[i-1]['high'] and candles[i]['high'] > candles[i+1]['high'] and candles[i+1]['high'] > candles[i+2]['high'] and candles[i-1]['high'] > candles[i-2]['high']
  return resistance
             
def mean(numbers):
    return decimal.Decimal(sum(numbers)) / max(len(numbers), 1)

def calc_atr(time_period, start_index, candles):
    try:
        lastIndex = time_period + start_index
        totalTR = 0

        # https://www.fmlabs.com/reference/default.htm?url=ATR.htm
        # we need to count backwards to calculate first ATR from 14 day average of TR
        for i in range(lastIndex, start_index - 1, -1):
            true_high = 0
            true_low = 0
            tr = 0

            if i < lastIndex:
                true_high = max(candles[i]['high'], candles[i + 1]['close'])
                true_low = min(candles[i]['low'], candles[i + 1]['close'])
                tr = true_high - true_low

            totalTR += tr
            candles[i]['TR'] = tr

            # first 14-day ATR is the average of the daily TR values for the last 14 days
            if i == (lastIndex - 14):
                candles[i]['ATR'] = totalTR / 14
            elif i < (lastIndex - 14):
                # ATR = [(Prior ATR x 13) + Current TR] / 14
                candles[i]['ATR'] = (
                    (candles[i + 1]['ATR'] * 13) + tr) / 14
            else:
                candles[i]['ATR'] = 0
    except Exception as ex:
        print ("calc_atr() failed with stock", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def calc_basing(code, time_period, start_index, candles):
    try:
        date_now = datetime.datetime.now(tz=eastern_time_zone)
        
        for i in range(start_index, time_period):
            high = 0.0
            low = 10000000000.0
            for j in range(i, i + 4):
                if candles[j]['high'] > high:
                    high = candles[j]['high']
                if candles[j]['low'] < low: 
                    low = candles[j]['low']

            # update macd daily bounds
            if high > daily_stocks_status[code]['price_daily_high']:
                daily_stocks_status[code]['price_daily_high'] = high
            if low < daily_stocks_status[code]['price_daily_low']: 
                daily_stocks_status[code]['price_daily_low'] = low

            price_percent_move_in_4_bars = 0
            if date_now >= start_trading_time:  
                if daily_stocks_status[code]['high'] - daily_stocks_status[code]['low'] > 0:
                    price_percent_move_in_4_bars = (abs(high - low) / abs(daily_stocks_status[code]['high'] - daily_stocks_status[code]['low'])) * 100
            else:
                if daily_stocks_status[code]['previous_day_high'] - daily_stocks_status[code]['previous_day_low'] > 0:
                    price_percent_move_in_4_bars = (abs(high - low) / abs(daily_stocks_status[code]['previous_day_high'] - daily_stocks_status[code]['previous_day_low'])) * 100

            high = 0
            low = 10000000000
            for j in range(i, i + 4):
                if candles[j]['macd'] > high:
                    high = candles[j]['macd']
                if candles[j]['macd'] < low: 
                    low = candles[j]['macd']

            # update macd daily bounds
            if high > daily_stocks_status[code]['macd_daily_high']:
                daily_stocks_status[code]['macd_daily_high'] = high
            if low < daily_stocks_status[code]['macd_daily_low']: 
                daily_stocks_status[code]['macd_daily_low'] = low

            macd_percent_move_in_4_bars = 0
            if daily_stocks_status[code]['macd_daily_high'] - daily_stocks_status[code]['macd_daily_low'] > 0:
                macd_percent_move_in_4_bars = (abs(high - low) / abs(daily_stocks_status[code]['macd_daily_high'] - daily_stocks_status[code]['macd_daily_low'])) * 100
            
            candles[i]['price_percent_move_in_4_bars'] = price_percent_move_in_4_bars
            candles[i]['macd_percent_move_in_4_bars'] = macd_percent_move_in_4_bars
    except Exception as ex:
        print ("calc_basing() failed with stock", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def calc_bollinger_bands(time_period, start_index, candles, mult):
    bb_points = []
    current = 0

    for i in range(time_period + start_index, start_index - 1, -1):
        std_dev = calc_std_dev(time_period, start_index, candles, 'close')
        sma20 = calc_sma(time_period, candles, i, 'close')
        candles[i]['sma20'] = sma20
        middle = sma20
        # Upper Channel Line: 20-day SMA + (2 x STDDEV)
        upper = sma20 + (mult * std_dev)
        # Lower Channel Line: 20-day SMA - (2 x STDDEV)
        lower = sma20 - (mult * std_dev)
        
        bb_points.insert(0, {'middle': middle, 'upper': upper, 'lower': lower})

    return bb_points

# calculate keltner channels


def calc_keltner_channels(time_period, start_index, candles, mult):
    k20 = 2.0 / (20.0 + 1.0)
    keltner_points = []
    current = 0

    # Middle Line: 20-day exponential moving average
    for i in range(start_index + time_period, start_index - 1, -1):
        ema20 = calc_ema(time_period, i, k20, candles, 'close', 'close', 'ema20')
        candles[i]['ema20'] = ema20
        middle = ema20
        # Upper Channel Line: 20-day EMA + (2 x ATR(10day))
        upper = ema20 + (mult * candles[current]['ATR'])
        # Lower Channel Line: 20-day EMA - (2 x ATR(10day))
        lower = ema20 - (mult * candles[current]['ATR'])
        keltner_points.insert(0, {'middle': middle, 'upper': upper, 'lower': lower})

    return keltner_points

# calculate standard deviation

def calc_std_dev(time_period, start_index, candles, dict_key):
    mean = calc_sma(time_period, candles, start_index, dict_key)
    sqrdiffTotal = 0

    for i in range(time_period + start_index, start_index - 1, -1):
        sqrdiff = (candles[i][dict_key] - mean) ** 2
        sqrdiffTotal += sqrdiff

    variance = 1 / time_period * sqrdiffTotal
    return sqrt(variance)

# calculate simple moving average

def calc_sma(time_period, candles, start_index, dict_key):
    total = 0.0

    if (time_period + start_index) < len(candles):
        for i in range(start_index, (time_period + start_index)):
            total += candles[i][dict_key]

    return total / time_period

# calculate exponential moving averages

def calc_ema(time_period, candle_index, k, candles, seed_key, ema_key1, ema_key2):
    previous_day_index = candle_index + 1
    
    # use sma price as seed for first ema
    if previous_day_index > time_period - 1:
        return calc_sma(time_period, candles, candle_index, seed_key)

    # EMA is calculated by (Price [today] * K) + (EMA [yesterday] * [1 minus K])
    #return (candles[candle_index][ema_key1] * k) + (candles[previous_day_index][ema_key2] * (1 - k))
    # EMA (from trading view) is alpha + [price] x + (1 - alpha) * [prev price] | alpha = 2 / (y + 1)
    return k * candles[candle_index][ema_key1] + (1 - k) * candles[previous_day_index][ema_key2]

def calc_adx_and_di(code, time_period, start_index, candles):
    try: 
        # make sure we go 1 extra on sample size to cater for last 
        for i in range(len(candles) - 2, start_index - 1, -1):
            true_range = max(max(candles[i]['high'] - candles[i]['low'], abs(candles[i]['high'] - candles[i + 1]['close'])), abs(candles[i]['low'] - candles[i + 1]['close']))
            candles[i]['true_range'] = true_range
            directional_movement_plus = (candles[i]['high'] - candles[i + 1]['high'] > candles[i + 1]['low'] - candles[i]['low']) and candles[i]['high'] - candles[i + 1]['high'] or 0
            candles[i]['directional_movement_plus'] = directional_movement_plus
            directional_movement_minus = (candles[i + 1]['low'] - candles[i]['low'] > candles[i]['high'] - candles[i + 1]['high']) and candles[i + 1]['low'] - candles[i]['low'] or 0
            candles[i]['directional_movement_minus'] = directional_movement_minus
            # init 0s for remaining
            candles[i]['smoothed_true_range'] = 0
            candles[i]['smoothed_directional_movement_plus'] = 0
            candles[i]['smoothed_directional_movement_minus'] = 0
            candles[i]['di_plus'] = 0
            candles[i]['di_minus'] = 0
            candles[i]['di_diff'] = 0
            candles[i]['dx'] = 0

        for i in range(len(candles) - 3, start_index - 1, -1):
            # we need to ensure none of these values are 0 as they may when high/lows are same value
            if candles[i]['true_range'] > 0 or candles[i]['directional_movement_plus'] > 0 or candles[i]['directional_movement_minus'] > 0:
                candles[i]['smoothed_true_range'] = candles[i + 1]['smoothed_true_range'] - (candles[i + 1]['smoothed_true_range'] / len(candles)) + candles[i]['true_range']
                candles[i]['smoothed_directional_movement_plus'] = candles[i + 1]['smoothed_directional_movement_plus'] - (candles[i + 1]['smoothed_directional_movement_plus'] / len(candles)) + candles[i]['directional_movement_plus']
                candles[i]['smoothed_directional_movement_minus'] = candles[i + 1]['smoothed_directional_movement_minus'] - (candles[i + 1]['smoothed_directional_movement_minus'] / len(candles)) + candles[i]['directional_movement_minus']

            if candles[i]['smoothed_true_range'] > 0:
                candles[i]['di_plus'] = candles[i]['smoothed_directional_movement_plus'] / candles[i]['smoothed_true_range'] * 100
                candles[i]['di_minus'] = candles[i]['smoothed_directional_movement_minus'] / candles[i]['smoothed_true_range'] * 100
                candles[i]['di_diff'] = candles[i]['di_plus'] - candles[i]['di_minus']
            
            if (candles[i]['di_plus'] + candles[i]['di_minus']) > 0:
                candles[i]['dx'] = abs(candles[i]['di_plus'] - candles[i]['di_minus']) / (candles[i]['di_plus'] + candles[i]['di_minus']) * 100
        
        for i in range(time_period, start_index - 1, -1):
            candles[i]['adx'] = calc_sma(time_period, candles, i, 'dx')

    except Exception as ex:
        print ("calc_adx_and_di() failed with stock", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def calc_macd(time_period, start_index, candles):
    try :
        # https://www.dummies.com/personal-finance/investing/stocks-trading/how-to-calculate-exponential-moving-average-in-trading/
        # k value - smoothing value
        k12 = 2.0 / (12.0 + 1.0)
        k26 = 2.0 / (26.0 + 1.0)
        k9 = 2.0 / (9.0 + 1.0)

        lastIndex = time_period + start_index
        macd_hist_color = col_grow_above
        
        # loop through first to calculate all macd values (shortema - longema)
        # then loop through again to calculate signal and hist
        for i in range(lastIndex, start_index - 1, -1):
            ema12 = calc_ema(12, i, k12, candles, 'close', 'close', 'ema12')
            candles[i]['ema12'] = ema12
            ema26 = calc_ema(26, i, k26, candles, 'close', 'close', 'ema26')
            candles[i]['ema26'] = ema26
            shortema = ema12
            longema = ema26

            if i < lastIndex:
                shortema = ema12
                longema = ema26
                # smoothing code below slows macd
                '''shortema = 0.15 * candles[i]['close'] + \
                    0.85 * candles[i + 1]['ema12']
                longema = 0.075 * candles[i]['close'] + \
                    0.925 * candles[i + 1]['ema26']'''

            # MACD caluclation by (short EMA - long EMA)
            macdVal = shortema - longema
            candles[i]['macd'] = macdVal
        
        # 9 (the sample length) + 1 (we dont have a macd on the very last candle of the set)
        lastIndex = time_period - 10
        
        for i in range(lastIndex, start_index - 1, -1):
            macd_signal = calc_ema(9, i, k9, candles, 'macd', 'macd', 'macd_signal')
            candles[i]['macd_signal'] = macd_signal
            macd_hist = candles[i]['macd'] - candles[i]['macd_signal']
            candles[i]['macd_hist'] = macd_hist
            
            # make sure we have the first macd_hist calculated
            if i < lastIndex - 1:
                macd_hist_color = check_osc_color(macd_hist, candles[i + 1]['macd_hist'])

            candles[i]['macd_hist_color'] = macd_hist_color
    except Exception as ex:
        print ("calc_macd() failed.", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
def calc_rsi(time_period, start_index, candles):
    try:
        totalGain = 0.0
        avgGain14 = 0.0
        totalLoss = 0.0
        avgLoss14 = 0.0

        lastIndex = time_period + start_index

        # RS = Average Gain / Average Loss
        # we need to count backwards to calculate first RSI from 14 day average of RS
        for i in range(lastIndex, start_index - 1, -1):
            diff = candles[i]['close'] - candles[i + 1]['close']

            currentGain = 0
            currentLoss = 0

            if diff >= 0:
                currentGain = diff
                totalGain += diff
            else:
                absolute = abs(diff)
                currentLoss = absolute
                totalLoss += absolute

            avgGain = 0
            avgLoss = 0

            # first RS calculation has no smoothing
            if i == (lastIndex - 14):
                avgGain = totalGain / i
                avgLoss = totalLoss / i
            elif i < (lastIndex - 14):
                avgGain = (candles[i + 1]['avggain']
                        * 13 + currentGain) / 14
                avgLoss = (candles[i + 1]['avgloss']
                        * 13 + currentLoss) / 14

            candles[i]['avggain'] = avgGain
            candles[i]['avgloss'] = avgLoss

            rs = 0.0
            rsi = 0.0

            if avgGain > 0 and avgLoss > 0:
                rs = avgGain / avgLoss

            rsi = 100 - (100.0 / (1.0 + rs))

            candles[i]['rs'] = rs
            candles[i]['rsi'] = rsi
    except Exception as ex:
        print ("calc_rsi() failed.", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def calc_candle_bounds(time_period, candles, start_index):
    highest = candles[start_index]['high']
    lowest = candles[start_index]['close']
    
    lastIndex = time_period + start_index
    
    if lastIndex < len(candles) - 1:
        for i in range(time_period + start_index, start_index - 1, -1):
            close = candles[i]['close']
            if close > highest:
                highest = close
    
            if close < lowest:
                lowest = close

    return {'lowest': lowest, 'highest': highest}

# following linreg function from trading_view
# https://www.mathsisfun.com/data/least-squares-calculator.html

def calc_least_squares_regression(time_period, start_index, candles, offset, e1_func):
    sumx = 0
    sumy = 0
    sumx2 = 0
    sumxy = 0
    
    lastIndex = time_period + start_index

    if lastIndex < len(candles) - 1:
        for i in range(lastIndex, start_index - 1, -1):
            # where x is ranging from -(length-1) to 0
            x = i
            
            # generate y-value
            y = candles[i]['close'] - e1_func(i)

            sumx += x
            sumy += y
    
            x2 = (x)**2
            xy = x*y
    
            sumx2 += x2
            sumxy += xy
    
        slopem = (time_period * sumxy - sumx * sumy) / \
            (time_period * sumx2 - sumx**2)

        interceptb = (sumy - slopem * sumx) / time_period

    return interceptb + slopem * start_index


def check_osc_color(val, prev_val):
    if val >= 0:
        if prev_val < val:
            return col_grow_above
        else:
            return col_fall_above
    else:
        if prev_val < val:
            return col_grow_below
        else:
            return col_fall_below

def calc_ttm_lrc_bands(time_period, start_index, candles, inner_band, outer_band):
    ttm_lrc_points = []

    for i in range(time_period + start_index, start_index - 1, -1):
        std_dev = calc_std_dev(time_period, i, candles, 'close')
        e1_func = lambda i: 0
        linreg = calc_least_squares_regression(time_period, i, candles, 0, e1_func)
        
        # Upper Channels
        upper1 = linreg + (inner_band * std_dev)
        upper2 = linreg + (outer_band * std_dev)
        
        # Lower Channels
        lower1 = linreg - (inner_band * std_dev)
        lower2 = linreg - (outer_band * std_dev)
        
        ttm_lrc_points.insert(0, {'middle': linreg, 'upper1': upper1, 'upper2': upper2, 'lower1': lower1, 'lower2': lower2})

    return ttm_lrc_points
    
def calc_ttm_lrc(time_period, start_index, candles, inner_band, outer_band):
    ttm_lrc_bands = calc_ttm_lrc_bands(time_period, start_index, candles, inner_band, outer_band)
    return ttm_lrc_bands
        
def calc_ttm_squeeze(time_period, start_index, candles):
    # calculate keltner bands with y factor 1
    keltner_plot = calc_keltner_channels(time_period, start_index, candles, 1.5)
    bb_plot = calc_bollinger_bands(time_period, start_index, candles, 2)

    mid_colors = []
    bar_colors = []

    lastIndex = time_period + start_index - 1
    
    # calcualte all e1 values because we require time_period + start_index sum
    # calculate e1 for max number of candles adding time period for starting
    for i in range(len(candles) - time_period, start_index - 1, -1):
        basis = calc_sma(time_period, candles, i, 'close')
        bounds = calc_candle_bounds(time_period, candles, i)
        e1 = (bounds['highest'] + bounds['lowest']) / 2 + basis
        candles[i]['e1'] = e1
    
    for i in range(lastIndex, start_index - 1, -1):
        # linreg function
        e1_func = lambda i: candles[i]['e1'] / 2
        linreg = calc_least_squares_regression(time_period, i, candles, 0, e1_func)
        candles[i]['linreg'] = linreg
        diff = bb_plot[i]['upper'] - keltner_plot[i]['upper']

        if i < lastIndex:
            bar_color = check_osc_color(linreg, candles[i + 1]['linreg'])

            if diff >= 0:
                mid_color = 'green'
            else:
                mid_color = 'red'
    
            mid_colors.insert(0, mid_color)
            bar_colors.insert(0, bar_color)

    return {'mid_colors': mid_colors, 'bar_colors': bar_colors}

def validate_macd_ttm_pattern(macd_ttm_patterns, candles, ttm_squeeze):
    for macd_ttm_pattern in macd_ttm_patterns:
        # we must check 4 candle colours in a row, 3 times back to 6th candle
        for i in range(0, 3):
            count = 0
            start_candle_index = (3 + i)
            for index in range(0, 4):
                if candles[start_candle_index - index]['macd_hist_color'] == macd_ttm_pattern['macd'][index] and ttm_squeeze['bar_colors'][start_candle_index - index] == macd_ttm_pattern['ttm'][index]:
                    count += 1            
            if count == 3:
                return True

    return False

# momentum setups 

def check_momentum_sell(code, candles, ttm_squeeze, ttm_lrc):
    # check rsi is over 50
    rsi_below_50 = candles[0]['rsi'] < 50 and candles[0]['rsi'] > 30
    ttm_lrc_down = True #ttm_lrc[0]['middle'] < ttm_lrc[37]['middle']
    rsi_down = (candles[0]['rsi'] < candles[1]['rsi']) and (candles[1]['rsi'] < candles[2]['rsi'])
    rsi_percent_move_in_4_bars = ((candles[0]['rsi'] - candles[3]['rsi']) / candles[3]['rsi']) * 100
    # is the RSI move in past 4 candles > 9 %
    rsi_move_9_percent_in_4_bars = rsi_percent_move_in_4_bars < -9
    macd_lines_below_zero = candles[0]['macd'] < 0 and candles[0]['macd_signal'] < 0
    macd_below_signal = candles[0]['macd'] < candles[0]['macd_signal']
    exhausted_sell = daily_stocks_status[code]['macd_percent_move_in_4_bars'] >= 50
    # volume
    volume_avg_in_4_bars = (candles[0]['volume'] + candles[1]['volume'] + candles[2]['volume'] + candles[3]['volume']) / 4
    volume_above_avg_in_2_bars = (candles[0]['volume'] > volume_avg_in_4_bars) or (candles[1]['volume'] > volume_avg_in_4_bars)
    max_volume_in_4_bars = 0

    for i in range(0, 4):
        if candles[i]['volume'] > max_volume_in_4_bars:
            max_volume_in_4_bars = candles[i]['volume']
    
    max_volume_percent_vs_4_bar_avg = max_volume_in_4_bars / volume_avg_in_4_bars
    max_volume_greater_than_4_bar_avg = max_volume_percent_vs_4_bar_avg > 1
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')
    # check low is less than SMA50 and SMA20
    bars_below_sma50 = candles[0]['low'] < sma50
    bars_below_sma20 = candles[0]['low'] < sma20
    oneRedCandle = candles[0]['close'] < candles[0]['open']
    macd_ttm_setup = validate_macd_ttm_pattern(macd_ttm_sell_patterns, candles, ttm_squeeze)

    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"rsi_below_50": rsi_below_50, "rsi_down": rsi_down, "rsi_move_9_percent_in_4_bars": rsi_move_9_percent_in_4_bars, "bars_below_sma50": bars_below_sma50, 
        "bars_below_sma20": bars_below_sma20, "ttm_lrc_down": ttm_lrc_down, "macd_lines_below_zero": macd_lines_below_zero,
        'oneRedCandle': oneRedCandle, "macd_ttm_setup": macd_ttm_setup, "macd_below_signal": macd_below_signal, "exhausted_sell": not exhausted_sell, 
        "volume_above_avg_in_2_bars": volume_above_avg_in_2_bars, "max_volume_greater_than_4_bar_avg": max_volume_greater_than_4_bar_avg}
    
    failed_reasons = []

    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'rsi_percent_move_in_4_bars': rsi_percent_move_in_4_bars,
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'volume_avg_in_4_bars': volume_avg_in_4_bars,
        'max_volume_in_4_bars': max_volume_in_4_bars,
        'max_volume_percent_vs_4_bar_avg': max_volume_percent_vs_4_bar_avg,
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': rsi_below_50 and rsi_move_9_percent_in_4_bars and bars_below_sma50 and bars_below_sma20 and ttm_lrc_down and macd_lines_below_zero and macd_below_signal and rsi_down and oneRedCandle and not exhausted_sell and macd_ttm_setup and volume_above_avg_in_2_bars and max_volume_greater_than_4_bar_avg and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

def check_momentum_buy(code, candles, ttm_squeeze, ttm_lrc):
    # check rsi is over 50
    rsi_over_50 = candles[0]['rsi'] >= 44 and candles[0]['rsi'] <= 72
    rsi_percent_move_in_4_bars = ((candles[0]['rsi'] - candles[3]['rsi']) / candles[3]['rsi']) * 100
    # is the RSI move in past 4 candles > 9 %
    rsi_move_9_percent_in_4_bars = rsi_percent_move_in_4_bars > 9
    ttm_lrc_up = True #ttm_lrc[0]['middle'] > ttm_lrc[37]['middle']
    rsi_up = (candles[0]['rsi'] > candles[1]['rsi']) and (candles[1]['rsi'] > candles[2]['rsi'])
    macd_lines_above_zero = candles[0]['macd'] > 0 and candles[0]['macd_signal'] > 0
    macd_above_signal = candles[0]['macd'] > candles[0]['macd_signal']
    # volume
    volume_avg_in_4_bars = (candles[0]['volume'] + candles[1]['volume'] + candles[2]['volume'] + candles[3]['volume']) / 4
    volume_above_avg_in_2_bars = (candles[0]['volume'] > volume_avg_in_4_bars) or (candles[1]['volume'] > volume_avg_in_4_bars)
    max_volume_in_4_bars = 0

    for i in range(0, 4):
        if candles[i]['volume'] > max_volume_in_4_bars:
            max_volume_in_4_bars = candles[i]['volume']

    max_volume_percent_vs_4_bar_avg = max_volume_in_4_bars / volume_avg_in_4_bars
    max_volume_greater_than_4_bar_avg = max_volume_percent_vs_4_bar_avg > 1
    exhausted_buy = daily_stocks_status[code]['macd_percent_move_in_4_bars'] >= 50
    #macd_ttm_setup = validate_macd_ttm_pattern(macd_ttm_buy_patterns, candles, ttm_squeeze)
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')
    bars_over_sma50 = candles[0]['high'] > sma50
    bars_over_sma20 = candles[0]['high'] > sma20

    twoGreenBars = candles[0]['close'] > candles[0]['open'] and candles[1]['close'] > candles[1]['open']

    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"rsi_over_50": rsi_over_50, "rsi_up": rsi_up, "rsi_move_9_percent_in_4_bars=": rsi_move_9_percent_in_4_bars, "bars_over_sma50": bars_over_sma50, 
                "bars_over_sma20": bars_over_sma20, "twoGreenBars": twoGreenBars, "ttm_lrc_up": ttm_lrc_up, ", macd_lines_above_zero": macd_lines_above_zero, "macd_above_signal": macd_above_signal,
                "exhausted_buy": not exhausted_buy, "volume_above_avg_in_2_bars": volume_above_avg_in_2_bars, "max_volume_greater_than_4_bar_avg": max_volume_greater_than_4_bar_avg}

    failed_reasons = []
    
    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'rsi_percent_move_in_4_bars': rsi_percent_move_in_4_bars,
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'volume_avg_in_4_bars': volume_avg_in_4_bars,
        'max_volume_in_4_bars': max_volume_in_4_bars,
        'max_volume_percent_vs_4_bar_avg': max_volume_percent_vs_4_bar_avg,
        'candle_high': candles[0]['high'],
        'candle_low': candles[0]['low'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': rsi_over_50 and rsi_move_9_percent_in_4_bars and bars_over_sma50 and bars_over_sma20 and twoGreenBars and ttm_lrc_up and macd_lines_above_zero and rsi_up and not exhausted_buy and macd_above_signal and volume_above_avg_in_2_bars and max_volume_greater_than_4_bar_avg and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

# volume setups 

def check_volume_sell(code, candles, high, low, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')

    bars_below_sma20 = candles[0]['low'] < sma20
    #stock_is_basing = daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30
    current_candle = create_real_time_candle(code)
    is_red_candle = current_candle['close'] < current_candle['open']
    last_low_less_than_daily_low = current_candle['low'] < daily_stocks_status[code]['low']
    #exhausted = daily_stocks_status[code]['price_percent_move_in_10_bars'] >= 50
    last_volume_greater_previous_volume = candles[0]['volume'] > candles[1]['volume']
    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"is_red_candle": is_red_candle, "last_low_less_than_daily_low": last_low_less_than_daily_low, "last_volume_greater_previous_volume": last_volume_greater_previous_volume, "bars_below_sma20": bars_below_sma20, "volume_over_10k": volume_over_10k}
    failed_reasons = []

    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_red_candle and last_low_less_than_daily_low and last_volume_greater_previous_volume and bars_below_sma20 and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

def check_volume_buy(code, candles, high, low, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')
    bars_over_sma20 = candles[0]['high'] > sma20
    
    current_candle = create_real_time_candle(code)
    is_green_candle = current_candle['close'] > current_candle['open']
    last_high_greater_than_daily_high = current_candle['high'] > daily_stocks_status[code]['high']
    #exhausted = daily_stocks_status[code]['macd_percent_move_in_4_bars'] >= 50
    last_volume_greater_previous_volume = candles[0]['volume'] > candles[1]['volume']
    volume_over_10k = candles[0]['volume'] > 10000
    rsi_less_than_80 = candles[0]['rsi'] < 80

    # store all reason for failures
    all_results = {"is_green_candle": is_green_candle, "last_high_greater_than_daily_high": last_high_greater_than_daily_high, "last_volume_greater_previous_volume": last_volume_greater_previous_volume, "bars_over_sma20": bars_over_sma20, "rsi_less_than_80": rsi_less_than_80, "volume_over_10k": volume_over_10k}
    failed_reasons = []
    
    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_green_candle and last_high_greater_than_daily_high and last_volume_greater_previous_volume and bars_over_sma20 and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

# basing setups 

def check_basing_sell(code, candles, min_price_in_4_bars, largest_volume_in_4_bars, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')

    stock_is_basing = daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30
    current_candle = create_real_time_candle(code)
    is_red_candle = current_candle['close'] < current_candle['open']
    last_low_less_than_basing_low = current_candle['low'] < daily_stocks_status[code]['basing_price_low']
    last_volume_greater_previous_volume = current_candle['volume'] > candles[0]['volume']
    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"stock_is_basing": stock_is_basing, "is_red_candle": is_red_candle, "last_low_less_than_daily_low": last_low_less_than_basing_low, "last_volume_greater_previous_volume": last_volume_greater_previous_volume, "volume_over_10k":volume_over_10k}
    failed_reasons = []

    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_red_candle and last_low_less_than_basing_low and last_volume_greater_previous_volume and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

def check_basing_buy(code, candles, max_price_in_2_bars, largest_volume_in_4_bars, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')

    stock_is_basing = daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30
    current_candle = create_real_time_candle(code)
    is_green_candle = current_candle['close'] > current_candle['open']
    last_high_greater_than_basing_high = current_candle['high'] > daily_stocks_status[code]['basing_price_high']
    last_volume_greater_previous_volume = current_candle['volume'] > candles[0]['volume']
    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"stock_is_basing": stock_is_basing, "is_green_candle": is_green_candle, "last_high_greater_than_basing_high": last_high_greater_than_basing_high, "last_volume_greater_previous_volume": last_volume_greater_previous_volume, "volume_over_10k":volume_over_10k}
    failed_reasons = []
    
    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_green_candle and last_high_greater_than_basing_high and last_volume_greater_previous_volume and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

# exhausted setups 

def check_reversal_buy(code, candles, ttm_squeeze, ttm_lrc):
    # check rsi is over 50
    rsi_below_50 = candles[0]['rsi'] < 50 and candles[0]['rsi'] > 30
    ttm_lrc_down = True #ttm_lrc[0]['middle'] < ttm_lrc[37]['middle']
    rsi_down = (candles[0]['rsi'] < candles[1]['rsi']) and (candles[1]['rsi'] < candles[2]['rsi'])
    rsi_percent_move_in_4_bars = ((candles[0]['rsi'] - candles[3]['rsi']) / candles[3]['rsi']) * 100
    # is the RSI move in past 4 candles > 9 %
    rsi_move_9_percent_in_4_bars = rsi_percent_move_in_4_bars < -9
    macd_lines_below_zero = candles[0]['macd'] < 0 and candles[0]['macd_signal'] < 0
    macd_below_signal = candles[0]['macd'] < candles[0]['macd_signal']
    exhausted_sell = daily_stocks_status[code]['macd_percent_move_in_4_bars'] >= 80
    # volume
    volume_avg_in_4_bars = (candles[0]['volume'] + candles[1]['volume'] + candles[2]['volume'] + candles[3]['volume']) / 4
    volume_above_avg_in_2_bars = (candles[0]['volume'] > volume_avg_in_4_bars) or (candles[1]['volume'] > volume_avg_in_4_bars)
    max_volume_in_4_bars = 0

    for i in range(0, 4):
        if candles[i]['volume'] > max_volume_in_4_bars:
            max_volume_in_4_bars = candles[i]['volume']
    
    max_volume_percent_vs_4_bar_avg = max_volume_in_4_bars / volume_avg_in_4_bars
    max_volume_greater_than_4_bar_avg = max_volume_percent_vs_4_bar_avg > 1
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')
    bars_below_sma50 = candles[0]['low'] < sma50
    bars_below_sma20 = candles[0]['low'] < sma20
    oneRedCandle = candles[0]['close'] < candles[0]['open']
    macd_ttm_setup = validate_macd_ttm_pattern(macd_ttm_sell_patterns, candles, ttm_squeeze)

    # store all reason for failures
    all_results = {"rsi_below_50": rsi_below_50, "rsi_down": rsi_down, "rsi_move_9_percent_in_4_bars": rsi_move_9_percent_in_4_bars, "bars_below_sma50": bars_below_sma50, 
        "bars_below_sma20": bars_below_sma20, "ttm_lrc_down": ttm_lrc_down, "macd_lines_below_zero": macd_lines_below_zero,
        'oneRedCandle': oneRedCandle, "macd_ttm_setup": macd_ttm_setup, "macd_below_signal": macd_below_signal, "exhausted_sell": exhausted_sell, 
        "volume_above_avg_in_2_bars": volume_above_avg_in_2_bars, "max_volume_greater_than_4_bar_avg": max_volume_greater_than_4_bar_avg}
            
    failed_reasons = []

    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'rsi_percent_move_in_4_bars': rsi_percent_move_in_4_bars,
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'volume_avg_in_4_bars': volume_avg_in_4_bars,
        'max_volume_in_4_bars': max_volume_in_4_bars,
        'max_volume_percent_vs_4_bar_avg': max_volume_percent_vs_4_bar_avg,
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': rsi_below_50 and rsi_move_9_percent_in_4_bars and bars_below_sma50 and bars_below_sma20 and ttm_lrc_down and macd_lines_below_zero and macd_below_signal and rsi_down and oneRedCandle and exhausted_sell and macd_ttm_setup and volume_above_avg_in_2_bars and max_volume_greater_than_4_bar_avg, #(macd_ttm_rule_1 or macd_ttm_rule_2 or macd_ttm_rule_3),
        'failed_reason': str(failed_reasons)
    }

def check_reversal_sell(code, candles, ttm_squeeze, ttm_lrc):
    # check rsi is over 50
    rsi_over_50 = candles[0]['rsi'] >= 44 and candles[0]['rsi'] <= 72
    rsi_percent_move_in_4_bars = ((candles[0]['rsi'] - candles[3]['rsi']) / candles[3]['rsi']) * 100
    # is the RSI move in past 4 candles > 9 %
    rsi_move_9_percent_in_4_bars = rsi_percent_move_in_4_bars > 9
    ttm_lrc_up = True #ttm_lrc[0]['middle'] > ttm_lrc[37]['middle']
    rsi_up = (candles[0]['rsi'] > candles[1]['rsi']) and (candles[1]['rsi'] > candles[2]['rsi'])
    macd_lines_above_zero = candles[0]['macd'] > 0 and candles[0]['macd_signal'] > 0
    macd_above_signal = candles[0]['macd'] > candles[0]['macd_signal']
    # volume
    volume_avg_in_4_bars = (candles[0]['volume'] + candles[1]['volume'] + candles[2]['volume'] + candles[3]['volume']) / 4
    volume_above_avg_in_2_bars = (candles[0]['volume'] > volume_avg_in_4_bars) or (candles[1]['volume'] > volume_avg_in_4_bars)
    max_volume_in_4_bars = 0

    for i in range(0, 4):
        if candles[i]['volume'] > max_volume_in_4_bars:
            max_volume_in_4_bars = candles[i]['volume']

    max_volume_percent_vs_4_bar_avg = max_volume_in_4_bars / volume_avg_in_4_bars
    max_volume_greater_than_4_bar_avg = max_volume_percent_vs_4_bar_avg > 1
    exhausted_buy = daily_stocks_status[code]['macd_percent_move_in_4_bars'] >= 80
    macd_ttm_setup = validate_macd_ttm_pattern(macd_ttm_buy_patterns, candles, ttm_squeeze)
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')
    bars_over_sma50 = candles[0]['high'] > sma50
    bars_over_sma20 = candles[0]['high'] > sma20
    twoGreenBars = candles[0]['close'] > candles[0]['open'] and candles[1]['close'] > candles[1]['open']

    # store all reason for failures
    all_results = {"rsi_over_50": rsi_over_50, "rsi_up": rsi_up, "rsi_move_9_percent_in_4_bars=": rsi_move_9_percent_in_4_bars, "bars_over_sma50": bars_over_sma50, 
                "bars_over_sma20": bars_over_sma20, "twoGreenBars": twoGreenBars, "ttm_lrc_up": ttm_lrc_up, ", macd_lines_above_zero": macd_lines_above_zero, "macd_above_signal": macd_above_signal,
                'macd_ttm_setup': macd_ttm_setup, "exhausted_buy": exhausted_buy, "volume_above_avg_in_2_bars": volume_above_avg_in_2_bars, "max_volume_greater_than_4_bar_avg": max_volume_greater_than_4_bar_avg}

    failed_reasons = []
    
    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'rsi_percent_move_in_4_bars': rsi_percent_move_in_4_bars,
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'volume_avg_in_4_bars': volume_avg_in_4_bars,
        'max_volume_in_4_bars': max_volume_in_4_bars,
        'max_volume_percent_vs_4_bar_avg': max_volume_percent_vs_4_bar_avg,
        'candle_high': candles[0]['high'],
        'candle_low': candles[0]['low'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': rsi_over_50 and rsi_move_9_percent_in_4_bars and bars_over_sma50 and bars_over_sma20 and twoGreenBars and ttm_lrc_up and macd_lines_above_zero and rsi_up and exhausted_buy and macd_above_signal and macd_ttm_setup and volume_above_avg_in_2_bars and max_volume_greater_than_4_bar_avg,
        'failed_reason': str(failed_reasons)
    }

# volume setups 

def check_ranging_sell(code, candles, high, low, break_support_level, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')

    stock_is_basing = daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30
    current_candle = create_real_time_candle(code)
    is_red_candle = current_candle['close'] < current_candle['open']
    last_price_less_than_2_bar_min = current_candle['low'] < low
    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"is_red_candle": is_red_candle, "break_support_level": break_support_level, "last_price_less_than_2_bar_min": last_price_less_than_2_bar_min, "volume_over_10k": volume_over_10k}
    failed_reasons = []

    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_red_candle and break_support_level and last_price_less_than_2_bar_min and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

def check_ranging_buy(code, candles, high, low, break_resistance_level, ttm_squeeze, ttm_lrc):
    sma5 = calc_sma(5, candles, 0, 'close')
    sma20 = calc_sma(20, candles, 0, 'close')
    sma50 = calc_sma(50, candles, 0, 'close')

    stock_is_basing = daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30
    current_candle = create_real_time_candle(code)
    is_green_candle = current_candle['close'] > current_candle['open']
    last_price_greater_than_2_bar_max = current_candle['high'] > high
    volume_over_10k = candles[0]['volume'] > 10000

    # store all reason for failures
    all_results = {"is_green_candle": is_green_candle, "break_resistance_level": break_resistance_level, "last_price_greater_than_2_bar_max": last_price_greater_than_2_bar_max, "volume_over_10k": volume_over_10k}

    failed_reasons = []
    
    for key in all_results.keys():
        if not all_results[key]:
            failed_reasons.append(key)

    return {
        'rsi': candles[0]['rsi'],
        'macd': candles[0]['macd'],
        'macd_hist': candles[0]['macd_hist'],
        'ttm_squeeze': candles[0]['linreg'],
        'candle_low': candles[0]['low'],
        'candle_high': candles[0]['high'],
        'sma5': sma5,
        'sma20': sma20,
        'sma50': sma50,
        'result': is_green_candle and break_resistance_level and last_price_greater_than_2_bar_max and volume_over_10k,
        'failed_reason': str(failed_reasons)
    }

# update indicators for stock

def update_stock_indicators(code, candles):   
    try: 
        calc_adx_and_di(code, 20, 0, candles)
        calc_macd(50, 0, candles)
        calc_rsi(50, 0, candles)
        calc_atr(20, 0, candles)
        calc_basing(code, 20, 0, candles)
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)

    except Exception as ex:
        print ("update_stock_indicators() failed with stock", ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, bytes):
            item = item.decode('utf-8', 'ignore')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv


def _decode_dict(data):
    rv = {}
    for key, value in data.items():
        if isinstance(key, bytes):
            key = key.encode('utf-8')
        if isinstance(value, bytes):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

# check stock has been previously traded

def past_traded(code):
    if code not in historical_trades.keys() and code not in open_trades.keys():
        return False
    return True

# check trade entries 

def check_momentum_trade_setup(code, candles):
    try:
        last_price = candles[0]['close']
        prev_price = candles[1]['close']
        prev_prev_price = candles[2]['close']
        lastVolume = candles[0]['volume']
        previous_day_volume = candles[1]['volume']
        prev_previous_day_volume = candles[2]['volume']
        atr = candles[0]['ATR']
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)
        ttm_lrc = calc_ttm_lrc(38, 0, candles, 1, 2)
        result = {}

        if daily_stocks_status[code]['percentage_moved'] > 0:
            result = check_momentum_buy(code, candles, ttm_squeeze, ttm_lrc)
            trade_action = 'long'
        elif daily_stocks_status[code]['percentage_moved'] < 0:
            result = check_momentum_sell(code, candles, ttm_squeeze, ttm_lrc)
            trade_action = 'short'
        else:
            return

        macd_signal = candles[0]['macd_signal']
        macd_delta = abs(candles[0]['macd'] - candles[2]['macd'])
        ttm_squeeze_delta = abs(candles[0]['linreg'] - candles[2]['linreg'])

        return {
            'last_price': last_price,
            'prev_price': prev_price,
            'purchase_price': candles[0]['open'],
            'prev_prev_price': prev_prev_price,
            'atr': atr,
            'volume': lastVolume,
            'previous_day_volume': previous_day_volume,
            'prev_previous_day_volume': prev_previous_day_volume,
            'rsi': result['rsi'],
            'rsi_percent_move_in_4_bars': result['rsi_percent_move_in_4_bars'],
            'macd': result['macd'],
            'macd_hist': result['macd_hist'],
            'macd_delta': macd_delta,
            'macd_signal': macd_signal,
            'ttm_squeeze': result['ttm_squeeze'],
            'ttm_squeeze_delta': ttm_squeeze_delta,
            'candle_low': result['candle_low'],
            'candle_high': result['candle_high'],
            'sma50': result['sma50'],
            'sma20': result['sma20'],
            'sma5': result['sma5'],
            'result': result['result'],
            'failed_reason': result['failed_reason'],
            'volume_avg_in_4_bars': result['volume_avg_in_4_bars'],
            'max_volume_in_4_bars': result['max_volume_in_4_bars'],
            'max_volume_percent_vs_4_bar_avg': result['max_volume_percent_vs_4_bar_avg'],
            'trade_action': trade_action,
        }

    except Exception as ex:
        print ("check_momentum_trade_setup failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    print ("Completed check_momentum_trade_setup for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def check_volume_trade_setup(code, candles):
    global market_direction
    
    try:
        last_price = candles[0]['close']
        prev_price = candles[1]['close']
        prev_prev_price = candles[2]['close']
        lastVolume = candles[0]['volume']
        previous_day_volume = candles[1]['volume']
        prev_previous_day_volume = candles[2]['volume']
        atr = candles[0]['ATR']
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)
        ttm_lrc = calc_ttm_lrc(38, 0, candles, 1, 2)
        result = {}

        high = 0
        low = 10000000000000000
        for i in range(1, 3):
            if candles[i]['high'] > high:
                high = candles[i]['high']
            if candles[i]['low'] < low:
                low = candles[i]['low']
        
        trade_action = ''

        if 'buy' in market_direction:
            result = check_volume_buy(code, candles, high, low, ttm_squeeze, ttm_lrc)
            trade_action = 'long'
        elif 'sell' in market_direction:
            result = check_volume_sell(code, candles, high, low, ttm_squeeze, ttm_lrc)
            trade_action = 'short'

        if len(result.keys()) > 0:
            macd_signal = candles[0]['macd_signal']
            macd_delta = abs(candles[0]['macd'] - candles[2]['macd'])
            ttm_squeeze_delta = abs(candles[0]['linreg'] - candles[2]['linreg'])
        
            return {
                'last_price': last_price,
                'prev_price': prev_price,
                'purchase_price': candles[0]['open'],
                'prev_prev_price': prev_prev_price,
                'atr': atr,
                'volume': lastVolume,
                'previous_day_volume': previous_day_volume,
                'prev_previous_day_volume': prev_previous_day_volume,
                'rsi': result['rsi'],
                'macd': result['macd'],
                'macd_hist': result['macd_hist'],
                'macd_delta': macd_delta,
                'macd_signal': macd_signal,
                'ttm_squeeze': result['ttm_squeeze'],
                'ttm_squeeze_delta': ttm_squeeze_delta,
                'candle_low': result['candle_low'],
                'candle_high': result['candle_high'],
                'sma50': result['sma50'],
                'sma20': result['sma20'],
                'sma5': result['sma5'],
                'result': result['result'],
                'failed_reason': result['failed_reason'],
                'trade_action': trade_action,
            }

    except Exception as ex:
        print ("check_volume_trade_setup() failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    print ("Completed check_volume_trade_setup() for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def check_basing_trade_setup(code, candles):
    global market_direction

    try:
        last_price = candles[0]['close']
        prev_price = candles[1]['close']
        prev_prev_price = candles[2]['close']
        lastVolume = candles[0]['volume']
        previous_day_volume = candles[1]['volume']
        prev_previous_day_volume = candles[2]['volume']
        atr = candles[0]['ATR']
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)
        ttm_lrc = calc_ttm_lrc(38, 0, candles, 1, 2)
        result = {}

        min_price_in_4_bars = 10000000000000000
        max_price_in_4_bars = 0
        largest_volume_in_4_bars = 0

        for i in range(1, 5):
            if candles[i]['volume'] > largest_volume_in_4_bars:
                largest_volume_in_4_bars = candles[i]['volume']
            if candles[i]['low'] < min_price_in_4_bars:
                min_price_in_4_bars = candles[i]['low']
            if candles[i]['high'] > max_price_in_4_bars:
                max_price_in_4_bars = candles[i]['high']
        
        trade_action = ''
        
        if 'buy' in market_direction:
            result = check_basing_buy(code, candles, max_price_in_4_bars, largest_volume_in_4_bars, ttm_squeeze, ttm_lrc)
            trade_action = 'long'
        elif 'sell' in market_direction:
            result = check_basing_sell(code, candles, min_price_in_4_bars, largest_volume_in_4_bars, ttm_squeeze, ttm_lrc)
            trade_action = 'short'

        if len(result.keys()) > 0:
            macd_signal = candles[0]['macd_signal']
            macd_delta = abs(candles[0]['macd'] - candles[2]['macd'])
            ttm_squeeze_delta = abs(candles[0]['linreg'] - candles[2]['linreg'])
            
            return {
                'last_price': last_price,
                'prev_price': prev_price,
                'purchase_price': candles[0]['open'],
                'prev_prev_price': prev_prev_price,
                'atr': atr,
                'volume': lastVolume,
                'previous_day_volume': previous_day_volume,
                'prev_previous_day_volume': prev_previous_day_volume,
                'rsi': result['rsi'],
                'macd': result['macd'],
                'macd_hist': result['macd_hist'],
                'macd_delta': macd_delta,
                'macd_signal': macd_signal,
                'ttm_squeeze': result['ttm_squeeze'],
                'ttm_squeeze_delta': ttm_squeeze_delta,
                'candle_low': result['candle_low'],
                'candle_high': result['candle_high'],
                'sma50': result['sma50'],
                'sma20': result['sma20'],
                'sma5': result['sma5'],
                'result': result['result'],
                'failed_reason': result['failed_reason'],
                'trade_action': trade_action,
            }

    except Exception as ex:
        print ("check_basing_trade_setup() failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    print ("Completed check_basing_trade_setup() for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def check_exhausted_trade_setup(code, candles):
    try:
        last_price = candles[0]['close']
        prev_price = candles[1]['close']
        prev_prev_price = candles[2]['close']
        lastVolume = candles[0]['volume']
        previous_day_volume = candles[1]['volume']
        prev_previous_day_volume = candles[2]['volume']
        atr = candles[0]['ATR']
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)
        ttm_lrc = calc_ttm_lrc(38, 0, candles, 1, 2)
        result = {}

        if daily_stocks_status[code]['percentage_moved'] < 0:
            result = check_reversal_buy(code, candles, ttm_squeeze, ttm_lrc)
            trade_action = 'long'
        elif daily_stocks_status[code]['percentage_moved'] > 0:
            result = check_reversal_sell(code, candles, ttm_squeeze, ttm_lrc)
            trade_action = 'short'
        else:
            return

        macd_signal = candles[0]['macd_signal']
        macd_delta = abs(candles[0]['macd'] - candles[2]['macd'])
        ttm_squeeze_delta = abs(candles[0]['linreg'] - candles[2]['linreg'])
        
        return {
            'last_price': last_price,
            'prev_price': prev_price,
            'purchase_price': candles[0]['open'],
            'prev_prev_price': prev_prev_price,
            'atr': atr,
            'volume': lastVolume,
            'previous_day_volume': previous_day_volume,
            'prev_previous_day_volume': prev_previous_day_volume,
            'rsi': result['rsi'],
            'rsi_percent_move_in_4_bars': result['rsi_percent_move_in_4_bars'],
            'macd': result['macd'],
            'macd_hist': result['macd_hist'],
            'macd_delta': macd_delta,
            'macd_signal': macd_signal,
            'ttm_squeeze': result['ttm_squeeze'],
            'ttm_squeeze_delta': ttm_squeeze_delta,
            'candle_low': result['candle_low'],
            'candle_high': result['candle_high'],
            'sma50': result['sma50'],
            'sma20': result['sma20'],
            'sma5': result['sma5'],
            'result': result['result'],
            'failed_reason': result['failed_reason'],
            'volume_avg_in_4_bars': result['volume_avg_in_4_bars'],
            'max_volume_in_4_bars': result['max_volume_in_4_bars'],
            'max_volume_percent_vs_4_bar_avg': result['max_volume_percent_vs_4_bar_avg'],
            'trade_action': trade_action,
        }

    except Exception as ex:
        print ("check_exhausted_trade_setup failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    print ("Completed check_exhausted_trade_setup for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def check_ranging_trade_setup(code, candles):
    global market_direction

    try:
        last_price = candles[0]['close']
        prev_price = candles[1]['close']
        prev_prev_price = candles[2]['close']
        lastVolume = candles[0]['volume']
        previous_day_volume = candles[1]['volume']
        prev_previous_day_volume = candles[2]['volume']
        atr = candles[0]['ATR']
        ttm_squeeze = calc_ttm_squeeze(20, 0, candles)
        ttm_lrc = calc_ttm_lrc(38, 0, candles, 1, 2)
        result = {}

        high = 0
        low = 10000000000000000
        break_resistance_level = False
        break_support_level = False

        for i in range(1, 3):
            if candles[i]['high'] > high:
                high = candles[i]['high']
            if high > daily_stocks_status[code]['ranging_price_high']:
                break_resistance_level = True
            if low < daily_stocks_status[code]['ranging_price_low']:
                break_support_level = True
            if candles[i]['low'] < low:
                low = candles[i]['low']

        trade_action = ''
        
        if 'buy' in market_direction:
            result = check_ranging_buy(code, candles, high, low, break_resistance_level, ttm_squeeze, ttm_lrc)
            trade_action = 'long'
        elif 'sell' in market_direction:
            result = check_ranging_sell(code, candles, high, low, break_support_level, ttm_squeeze, ttm_lrc)
            trade_action = 'short'

        macd_signal = candles[0]['macd_signal']
        macd_delta = abs(candles[0]['macd'] - candles[2]['macd'])
        ttm_squeeze_delta = abs(candles[0]['linreg'] - candles[2]['linreg'])

        if len(result.keys()) > 0:
            return {
                'last_price': last_price,
                'prev_price': prev_price,
                'purchase_price': candles[0]['open'],
                'prev_prev_price': prev_prev_price,
                'atr': atr,
                'volume': lastVolume,
                'previous_day_volume': previous_day_volume,
                'prev_previous_day_volume': prev_previous_day_volume,
                'rsi': result['rsi'],
                'macd': result['macd'],
                'macd_hist': result['macd_hist'],
                'macd_delta': macd_delta,
                'macd_signal': macd_signal,
                'ttm_squeeze': result['ttm_squeeze'],
                'ttm_squeeze_delta': ttm_squeeze_delta,
                'candle_low': result['candle_low'],
                'candle_high': result['candle_high'],
                'sma50': result['sma50'],
                'sma20': result['sma20'],
                'sma5': result['sma5'],
                'result': result['result'],
                'failed_reason': result['failed_reason'],
                'trade_action': trade_action,
            }

    except Exception as ex:
        print ("check_ranging_trade_setup failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

    print ("Completed check_ranging_trade_setup for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

# check stock is an entry

def check_trade_entry(code, behavior, scanner): 
    global e_mini
    global total_trades
    global total_momentum_trades
    global total_volume_trades
    global total_basing_trades
    global total_exhausted_trades
    global market_direction
    global trade_quantity_dollar_value
    global ibManager

    try:
        trade_setup_result = False
        trade_action = ''
        candles = sorted(stocks_5_min_candles[code].values(), key = lambda i: i['timestamp'], reverse=True)
        if len(candles) >= 80:
            if 'momentum' in behavior:
                trade_setup_result = check_momentum_trade_setup(code, candles)
            elif 'volume' in behavior:
                trade_setup_result = check_volume_trade_setup(code, candles)
            elif 'basing' in behavior:
                trade_setup_result = check_basing_trade_setup(code, candles)
            elif 'exhausted' in behavior:
                trade_setup_result = check_exhausted_trade_setup(code, candles)
            elif 'ranging' in behavior:
                trade_setup_result = check_ranging_trade_setup(code, candles)

            if trade_setup_result:
                trade_action = trade_setup_result['trade_action']
                print ('Trade setup for {} - {} is'.format(code, trade_action), trade_setup_result['result'])
                volume = daily_stocks_status[code]['last_volume']
                price = daily_stocks_status[code]['last_price']
                # check stock is a buy/sell and add to open trades
                if trade_setup_result['result']:
                    # update total trade numbers
                    total_trades = daily_settings_table.get_item(
                        Key={
                            'key': 'total_trades',
                        }
                    )['Item']['value']
                    total_momentum_trades = daily_settings_table.get_item(
                        Key={
                            'key': 'total_momentum_trades',
                        }
                    )['Item']['value']
                    total_volume_trades = daily_settings_table.get_item(
                        Key={
                            'key': 'total_volume_trades',
                        }
                    )['Item']['value']
                    total_basing_trades = daily_settings_table.get_item(
                        Key={
                            'key': 'total_basing_trades',
                        }
                    )['Item']['value']
                    total_exhausted_trades = daily_settings_table.get_item(
                        Key={
                            'key': 'total_exhausted_trades',
                        }
                    )['Item']['value']

                    trade_id = str(uuid.uuid4())
                    price_one_percent = price * 0.01

                    if 'short' in trade_action:
                        limit = price - (price * 0.1)
                        stop_loss_500 = price + ((500 * price) / trade_quantity_dollar_value)
                    else:
                        limit = price + (price * 0.1)
                        stop_loss_500 = price - ((500 * price) / trade_quantity_dollar_value)
                    
                    total_trades += 1

                    if 'momentum' in behavior:
                        total_momentum_trades += 1
                    elif 'volume' in behavior:
                        total_volume_trades += 1
                    elif 'basing' in behavior:
                        total_basing_trades += 1
                    elif 'exhausted' in behavior:
                        total_exhausted_trades += 1

                    quantity = int(ceil(trade_quantity_dollar_value / price))
                    if 'polygon_auth_key_test' not in os.environ['polygon_auth_secret']:
                        if 'volume' in behavior or 'basing' in behavior:
                            ibManager.execute_order(code, trade_action, quantity)

                    open_trades[code] = {
                        'code': code,
                        'id': trade_id,
                        'date': datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d'),
                        'trade_action': trade_action,
                        'limit': limit,
                        'purchase_price': price,
                        'stop_loss_500': stop_loss_500,
                        'trade_opened': int((datetime.datetime.now(tz=eastern_time_zone)).timestamp() * 1000),
                        'rsi': trade_setup_result['rsi'],
                        'macd': trade_setup_result['macd'],
                        'macd_hist': trade_setup_result['macd_hist'],
                        'macd_delta': trade_setup_result['macd_delta'],
                        'ttm_squeeze': trade_setup_result['ttm_squeeze'],
                        'ttm_squeeze_delta': trade_setup_result['ttm_squeeze_delta'],
                        'candle_high': trade_setup_result['candle_high'],
                        'candle_low': trade_setup_result['candle_low'],
                        'sma50': trade_setup_result['sma50'],
                        'sma20': trade_setup_result['sma20'],
                        'sma5': trade_setup_result['sma5'],
                        'atr': trade_setup_result['atr'],
                        'percentage_moved': daily_stocks_status[code]['percentage_moved'],
                        'e_mini_sp': e_mini,
                        'e_mini_trend': market_direction,
                        'in_dynamo': False,
                        'behavior': behavior,
                        'scanner': scanner,
                        'volume': volume,
                        'max_profit': 0,
                        'profit': 0,
                        'ib_quantity': quantity
                    }

                    # put item in to OpenTrades
                    graphql_mutation = {
                        'operation': 'createOpenTrade', 
                        'query': 'mutation($input:CreateOpenTradesInput!){createOpenTrade(input:$input){code}}',
                        'variables': '{ "input": {"code":"%s", "id":"%s", "date":"%s", "trade_action":"%s", "limit": %d, \
        "purchase_price": %d, "stop_loss_500": %d, "trade_opened":"%s", "rsi": %d, "macd": %d, "macd_hist": %d, \
        "macd_delta": %d, "ttm_squeeze": %d, "ttm_squeeze_delta": %d, "candle_high": %d, "candle_low": %d, \
        "sma50": %d, "sma20": %d, "sma5": %d, "atr": %d, "percentage_moved": %d, "e_mini_sp": %d, \
        "e_mini_trend":"%s", "in_dynamo":"%r", "behavior":"%s", "scanner":"%s", "volume": %d, "profit": %d, "max_profit": %d, "ib_quantity": %d} }' % (code, trade_id, 
                        datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d'), 
                        trade_action, limit, price, stop_loss_500, int((datetime.datetime.now(tz=eastern_time_zone)).timestamp() * 1000), 
                        trade_setup_result['rsi'], trade_setup_result['macd'], trade_setup_result['macd_hist'], trade_setup_result['macd_delta'],
                        trade_setup_result['ttm_squeeze'], trade_setup_result['ttm_squeeze_delta'], trade_setup_result['candle_high'],
                        trade_setup_result['candle_low'], trade_setup_result['sma50'], trade_setup_result['sma20'], 
                        trade_setup_result['sma5'], trade_setup_result['atr'], daily_stocks_status[code]['percentage_moved'], e_mini, market_direction,
                        False, behavior, scanner, volume, 0, 0, quantity)
                    }
                    appSyncClient.mutate(graphql_mutation)

                    # update trade numbers in dynamo
                    response = daily_settings_table.put_item(
                        Item={
                            'key': 'total_trades',
                            'value': total_trades
                        })
                    response = daily_settings_table.put_item(
                        Item={
                            'key': 'total_momentum_trades',
                            'value': total_momentum_trades
                        })
                    response = daily_settings_table.put_item(
                        Item={
                            'key': 'total_volume_trades',
                            'value': total_volume_trades
                        })
                    response = daily_settings_table.put_item(
                        Item={
                            'key': 'total_basing_trades',
                            'value': total_basing_trades
                        })
                    response = daily_settings_table.put_item(
                        Item={
                            'key': 'total_exhausted_trades',
                            'value': total_exhausted_trades
                        })
            
    except Exception as ex:
        print ("check_trade_entry() failed with stock", code, ex, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
    print ("Completed check_trade_entry() {} for {}".format(behavior, code), datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
    
# check stock has hit stop loss or limit

def check_trade_exit(code):
    global trade_quantity_dollar_value
    global force_close_trades_time
    global market_direction
    global ibManager

    trade_setup_result = None

    try:
        candles = sorted(stocks_5_min_candles[code].values(), key = lambda i: i['timestamp'], reverse=True)
        if len(candles) >= 80:
            update_stock_indicators(code, candles)

            trade_action = open_trades[code]['trade_action']
            purchase_price = open_trades[code]['purchase_price']
            limit = open_trades[code]['limit']
            stop_loss_500 = open_trades[code]['stop_loss_500']
            trade_opened = open_trades[code]['trade_opened']
            last_price = daily_stocks_status[code]['last_price']

            if 'momentum' in open_trades[code]['behavior']:
                trade_setup_result = check_momentum_trade_setup(code, candles)
            elif 'volume' in open_trades[code]['behavior']:
                trade_setup_result = check_volume_trade_setup(code, candles)
            elif 'basing' in open_trades[code]['behavior']:
                trade_setup_result = check_basing_trade_setup(code, candles)
            elif 'exhausted' in open_trades[code]['behavior']:
                trade_setup_result = check_exhausted_trade_setup(code, candles)
            elif 'ranging' in open_trades[code]['behavior']:
                trade_setup_result = check_ranging_trade_setup(code, candles)

            profit_update = False

            if trade_setup_result:
                trade_result = 'none'
                open_trades[code]['delta'] = 0.0
                open_trades[code]['delta_diff'] = 0.0
                exit_trade = False
                num_shares = trade_quantity_dollar_value / purchase_price
                date_now = datetime.datetime.now(tz=eastern_time_zone)

                if 'short' in trade_action:
                    open_trades[code]['delta'] = purchase_price - last_price
                    open_trades[code]['delta_diff'] = open_trades[code]['delta'] / purchase_price * 100
                    if open_trades[code]['delta'] * num_shares != open_trades[code]['profit']:
                        profit_update = True

                    open_trades[code]['profit'] = open_trades[code]['delta'] * num_shares
                    
                    if date_now >= force_close_trades_time:
                        exit_trade = True
                        trade_result = 'Force closed.'
                    # if we hit limit
                    elif last_price <= limit:
                        exit_trade = True
                        trade_result = 'Hit limit.'
                    # if we hit a stop loss
                    elif last_price >= stop_loss_500:
                        exit_trade = True
                        trade_result = 'Hit $500 stop loss.'
                    elif open_trades[code]['max_profit'] >= 150 and open_trades[code]['profit'] <= 0 and last_price >= trade_setup_result['sma5']:
                        trade_result = 'Made $150, back $0 and cross sma5.'
                        exit_trade = True
                    elif open_trades[code]['profit'] <= -200 and last_price >= trade_setup_result['sma5']:
                        trade_result = 'Lost $200 and cross sma5.'
                        exit_trade = True
                    elif 'exhausted' not in open_trades[code]['scanner'] and last_price >= trade_setup_result['sma5'] and last_price >= trade_setup_result['sma20'] and last_price >= trade_setup_result['sma50']:
                        trade_result = 'Cross sma5, sma20 and sma50.'
                        exit_trade = True
                    elif 'exhausted' in open_trades[code]['scanner'] and last_price <= trade_setup_result['sma5'] and last_price <= trade_setup_result['sma20'] and last_price <= trade_setup_result['sma50']:
                        trade_result = 'Cross sma5, sma20 and sma50.'
                        exit_trade = True
                    elif 'momentum' in open_trades[code]['scanner']:
                        if last_price >= trade_setup_result['sma20'] and last_price >= trade_setup_result['sma50'] and trade_setup_result['rsi'] > 40 and (trade_setup_result['macd'] > 0 and trade_setup_result['macd_signal'] > 0):
                            exit_trade = True
                            trade_result = 'Above MAs RSI > 40 and MACD 2 lines above 0.'
                    
                    if last_price > purchase_price:
                        trade_result = '{} - Lost {}'.format(trade_result, open_trades[code]['delta'])
                    else:
                        trade_result = '{} - Won {}'.format(trade_result, open_trades[code]['delta'])
                else:
                    open_trades[code]['delta'] = last_price - purchase_price
                    open_trades[code]['delta_diff'] = open_trades[code]['delta'] / purchase_price * 100
                    open_trades[code]['profit'] = open_trades[code]['delta'] * num_shares
                        
                    if date_now >= force_close_trades_time:
                        exit_trade = True
                        trade_result = 'Force closed.'
                    # if we hit limit
                    elif last_price >= limit:
                        exit_trade = True
                        trade_result = 'Hit limit.'
                    # if we hit a stop loss
                    elif last_price <= stop_loss_500:
                        exit_trade = True
                        trade_result = 'Hit $500 stop loss.'
                    elif open_trades[code]['max_profit'] >= 150 and open_trades[code]['profit'] <= 0 and last_price <= trade_setup_result['sma5']:
                        trade_result = 'Made $150, back $0 and cross sma5.'
                        exit_trade = True
                    elif open_trades[code]['profit'] <= -200 and last_price <= trade_setup_result['sma5']:
                        trade_result = 'Lost $200 and cross sma5.'
                        exit_trade = True
                    elif 'exhausted' not in open_trades[code]['scanner'] and last_price <= trade_setup_result['sma5'] and last_price <= trade_setup_result['sma20'] and last_price <= trade_setup_result['sma50']:
                        trade_result = 'Cross sma5, sma20 and sma50.'
                        exit_trade = True
                    elif 'exhausted' in open_trades[code]['scanner'] and last_price >= trade_setup_result['sma5'] and last_price >= trade_setup_result['sma20'] and last_price >= trade_setup_result['sma50']:
                        trade_result = 'Cross sma5, sma20 and sma50.'
                        exit_trade = True
                    elif 'momentum' in open_trades[code]['scanner']:
                        if last_price <= trade_setup_result['sma20'] and last_price <= trade_setup_result['sma50'] and trade_setup_result['rsi'] < 40 and (trade_setup_result['macd'] < 0 and trade_setup_result['macd_signal'] < 0):
                            exit_trade = True
                            trade_result = 'Below MAs RSI < 40 and MACD 2 lines below 0.'
                            
                    if last_price < last_price:
                        trade_result = '{} - Lost {}'.format(trade_result, open_trades[code]['delta'])
                    else:
                        trade_result = '{} - Won {}'.format(trade_result, open_trades[code]['delta'])

                if open_trades[code]['max_profit'] >= 200 and open_trades[code]['profit'] <= (open_trades[code]['max_profit'] * 0.3):
                    trade_result = 'Lost 70% Profit Exit - Won {}'.format(open_trades[code]['delta'])
                    exit_trade = True
                elif open_trades[code]['max_profit'] >= 400 and open_trades[code]['profit'] <= (open_trades[code]['max_profit'] * 0.85):
                    trade_result = 'Lost 15% Profit Exit - Won {}'.format(open_trades[code]['delta'])
                    exit_trade = True

                if open_trades[code]['profit'] > open_trades[code]['max_profit']:
                    profit_update = True
                    open_trades[code]['max_profit'] = open_trades[code]['profit']

                # if profit change update dynamo
                if profit_update and not daily_stocks_status[code]['traded']:
                    graphql_mutation = {
                        'operation': 'updateOpenTrade', 
                        'query': 'mutation($input:UpdateOpenTradesInput!){updateOpenTrade(input:$input){code}}',
                        'variables': '{ "input": {"code":"%s", "profit":"%d", "max_profit":"%d"} }' % (code, open_trades[code]['profit'], open_trades[code]['max_profit'])
                    }
                    appSyncClient.mutate(graphql_mutation)

                # if we hit stop loss or limit we add to history
                if exit_trade:
                    print("Trade exited for stock", code, datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))
                    trade_id = open_trades[code]['id']
                    # remove from open trades table
                    response = open_trades_table.delete_item(
                            Key={
                                'code': code
                            }
                        )
                        
                    if last_price > 0:
                        if 'polygon_auth_key_test' not in os.environ['polygon_auth_secret']:
                            if 'volume' in open_trades[code]['behavior'] or 'basing' in open_trades[code]['behavior']:
                                ibManager.exit_order(code, trade_action, open_trades[code]['ib_quantity'])

                        historical_trades[code] = {
                            'id': trade_id,
                            'trade_action': trade_action,
                            'trade_result': trade_result,
                            'force_closed': date_now >= force_close_trades_time,
                            'trade_opened': trade_opened,
                            'trade_closed': int((datetime.datetime.now(tz=eastern_time_zone)).timestamp() * 1000),
                            'stop_loss_500': open_trades[code]['stop_loss_500'],
                            'limit': limit,
                            'purchase_price': purchase_price,
                            'close_price': last_price,
                            'delta': open_trades[code]['delta'],
                            'delta_diff': open_trades[code]['delta_diff'],
                            'profit': open_trades[code]['profit'],
                            'rsi': open_trades[code]['rsi'],
                            'macd': open_trades[code]['macd'],
                            'macd_hist': open_trades[code]['macd_hist'],
                            'macd_delta': open_trades[code]['macd_delta'],
                            'ttm_squeeze': open_trades[code]['ttm_squeeze'],
                            'ttm_squeeze_delta': open_trades[code]['ttm_squeeze_delta'],
                            'candle_high': trade_setup_result['candle_high'],
                            'candle_low': trade_setup_result['candle_low'],
                            'sma50': open_trades[code]['sma50'],
                            'sma20': open_trades[code]['sma20'],
                            'sma5': open_trades[code]['sma5'],
                            'e_mini_sp': open_trades[code]['e_mini_sp'],
                            'e_mini_trend_open': open_trades[code]['e_mini_trend'],
                            'e_mini_trend_close': market_direction,
                            'volume': open_trades[code]['volume'],
                            'atr': trade_setup_result['atr'],
                            'percentage_moved': open_trades[code]['percentage_moved'],
                            'behavior': open_trades[code]['behavior'],
                            'scanner': open_trades[code]['scanner'],
                            'in_rds': False,
                            'ib_quantity': open_trades[code]['ib_quantity']
                        }
                        
                        daily_stocks_status[code]['traded'] = True
                        # Perform a query to get a Todo ID
                        graphql_mutation = {
                            'operation': 'deleteOpenTrades',
                            'query': 'mutation($input:[DeleteOpenTradesInput]!){deleteOpenTrades(input:$input){code}}',
                            'variables': '{ "input": [{"code":"%s"}] }' % (code)
                        }
                        appSyncClient.mutate(graphql_mutation)
                        # remove from open trades
                        if code in open_trades.keys():
                            del open_trades[code]

    except Exception as ex:
        print("check_trade_exit() failed with stock", code, ex, trade_setup_result)

# aurora functions

def execute_statement(sql, database_name, sql_parameters=[]):
    global db_cluster_arn

    rds_client = boto3.client('rds-data', region_name='us-east-1')
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
    global db_cluster_arn

    rds_client = boto3.client('rds-data', region_name='us-east-1')
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

# moves candle data to rds from polygon data

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
    global secretManagerClient

    print ('Executing all_5_min_candles_to_rds - 5 min candles to RDS')
    est_date = datetime.datetime.now(tz=eastern_time_zone)
    # round up mini to nearest multiple of 5
    nearest_5_min_down = int((floor(est_date.minute / 5) * 5))
    est_date_nearest_5_min_down = int((est_date.replace(minute=nearest_5_min_down, second=0, microsecond=0) - timedelta(minutes=5)).timestamp()) * 1000

    # try getting secret from secret manager, if succesful perform queries
    secret = secretManagerClient.getSecretString('levitrade_history')
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

        for code in stocks_5_min_candles.copy().keys():
            for timestamp in stocks_5_min_candles[code].keys():
                candle = stocks_5_min_candles[code][timestamp]
                if candle['timestamp'] <= est_date_nearest_5_min_down:
                    if not candle['in_rds']:
                        # fromtimestamp uses local timezone, make sure we adjust according to where server is hosted
                        est_date = datetime.datetime.utcfromtimestamp(candle['timestamp'] / 1000) + timedelta(seconds=utc_offset)
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
                                {'name':'timestamp', 'value':{'doubleValue': int(timestamp)}},
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

                        stocks_5_min_candles[code][timestamp]['in_rds'] = True

        # insert outstanding records
        try:
            if len(sql_parameter_sets) > 0:
                print ('Inserting {} records into historical data.'.format(len(sql_parameter_sets)))  
                batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
        except Exception as e:
            print('historical data sql processes failed,', e)

        print ('Complete move to historical data.', est_date.strftime('%Y-%m-%d %H:%M'))

# process all close trades to rds every 1 min

def process_closed_trades_to_rds():
    print ('process_closed_trades_to_rds() - moving trades to rds.')
    for code in historical_trades.copy().keys():
        if not historical_trades[code]['in_rds']:
            single_trade_to_rds(code)

# move single trade to rds

def single_trade_to_rds(code):
    global secretManagerClient

    print ('single_trade_to_rds() - moving trade to rds.')
    est_date = datetime.datetime.now(tz=eastern_time_zone)
    secret = secretManagerClient.getSecretString('levitrade_history')
    if secret:
        try:
            trade = historical_trades[code]
            trade_opened = False
            trade_closed = False
            open_order = {
                'order_id': 0,
                'cOID': 'none',
            }
            close_order = {
                'order_id': 0,
                'cOID': 'none',
            }

            # if volume or basing check trade has exited
            if 'volume' in trade['behavior'] or 'basing' in trade['behavior']:                                     
                # get open order from ib_orders table
                open_order_response = ib_orders_table.get_item(
                    Key={
                        'key': '{}_entry'.format(code)
                    }
                )
                # get open order from ib_orders table
                close_order_response = ib_orders_table.get_item(
                    Key={
                        'key': '{}_exit'.format(code)
                    }
                )
                trade_opened = 'Item' in open_order_response.keys()
                if trade_opened:
                    open_order = open_order_response['Item']
                trade_closed = 'Item' in close_order_response.keys()
                if trade_closed:
                    close_order = close_order_response['Item']

            # ai will only learn from very profitable trades
            if trade['profit'] > 500:
                # ensure we don't insert more than once
                response = execute_statement("select * from trades_ai where code=\'{}\' and trade_opened >=\'{}\';""".format(code, int(est_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)), 'historical_data')
                if len(response['records']) == 0:
                    execute_statement('insert into trades_ai (id, code, trade_action, trade_result, behavior, scanner, force_closed, trade_opened, trade_closed, stop_loss_500, trade_limit, purchase_price, close_price, delta, delta_diff, profit, rsi, macd, macd_delta, ttm_squeeze, ttm_squeeze_delta, sma50, sma20, sma5, e_mini_sp, e_mini_trend_open, e_mini_trend_close, volume, atr, percentage_moved, ib_order_number_open, ib_order_number_close, ib_order_executed_open, ib_order_executed_close, ib_coid_open, ib_coid_close, ib_quantity) values (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\');'.format(trade['id'], code, trade['trade_action'], trade['trade_result'], trade['behavior'], trade['scanner'], trade['force_closed'], trade['trade_opened'], trade['trade_closed'], round(trade['stop_loss_500'], 2), round(trade['limit'], 2), round(trade['purchase_price'], 2), round(trade['close_price'], 2), round(trade['delta'], 2), round(trade['delta_diff'], 2), round(trade['profit'], 2), round(trade['rsi'], 2), round(trade['macd'], 2), round(trade['macd_delta'], 2), round(trade['ttm_squeeze'], 2), round(trade['ttm_squeeze_delta'], 2), round(trade['sma50'], 2), round(trade['sma20'], 2), round(trade['sma5'], 2), round(trade['e_mini_sp'], 2), trade['e_mini_trend_open'], trade['e_mini_trend_close'], trade['volume'], round(trade['atr'], 2), round(trade['percentage_moved'], 2), int(open_order['order_id']), int(close_order['order_id']), trade_opened, trade_closed, open_order['cOID'], close_order['cOID'], trade['ib_quantity']), 'historical_data')
            response = execute_statement("select * from trades where code=\'{}\' and trade_opened >=\'{}\';""".format(code, int(est_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)), 'historical_data')
            if len(response['records']) == 0:
                execute_statement('insert into trades (id, code, trade_action, trade_result, behavior, scanner, force_closed, trade_opened, trade_closed, stop_loss_500, trade_limit, purchase_price, close_price, delta, delta_diff, profit, rsi, macd, macd_delta, ttm_squeeze, ttm_squeeze_delta, sma50, sma20, sma5, e_mini_sp, e_mini_trend_open, e_mini_trend_close, volume, atr, percentage_moved, ib_order_number_open, ib_order_number_close, ib_order_executed_open, ib_order_executed_close, ib_coid_open, ib_coid_close, ib_quantity) values (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\');'.format(trade['id'], code, trade['trade_action'], trade['trade_result'], trade['behavior'], trade['scanner'], trade['force_closed'], trade['trade_opened'], trade['trade_closed'], round(trade['stop_loss_500'], 2), round(trade['limit'], 2), round(trade['purchase_price'], 2), round(trade['close_price'], 2), round(trade['delta'], 2), round(trade['delta_diff'], 2), round(trade['profit'], 2), round(trade['rsi'], 2), round(trade['macd'], 2), round(trade['macd_delta'], 2), round(trade['ttm_squeeze'], 2), round(trade['ttm_squeeze_delta'], 2), round(trade['sma50'], 2), round(trade['sma20'], 2), round(trade['sma5'], 2), round(trade['e_mini_sp'], 2), trade['e_mini_trend_open'], trade['e_mini_trend_close'], trade['volume'], round(trade['atr'], 2), round(trade['percentage_moved'], 2), int(open_order['order_id']), int(close_order['order_id']), trade_opened, trade_closed, open_order['cOID'], close_order['cOID'], trade['ib_quantity']), 'historical_data')
            
            historical_trades[code]['in_rds'] = True
            print('single_trade_to_rds() - {} added to rds.'.format(code))
        except Exception as e:
            print('single_trade_to_rds() - inserting trades to rds failed,', e, historical_trades[code])

    print ('single_trade_to_rds() - complete move to historical data.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

def all_trades_to_rds():
    global historical_trades
    global secretManagerClient

    print ('all_trades_to_rds() - moving trade history to rds.')
    # try getting secret from secret manager, if succesful perform queries
    secret = secretManagerClient.getSecretString('levitrade_history')
    if secret:
        print ('Connecting to {}'.format(secret['host']))

        '''execute_statement("""drop table if exists trades;""", 'historical_data')
        execute_statement("""CREATE TABLE IF NOT EXISTS trades(
            id text PRIMARY KEY,
            code text,
            trade_action text,
            trade_result text,
            scanner text,
            force_closed bool,
            trade_opened decimal,
            trade_closed decimal,
            stop_loss_500 decimal,
            trade_limit decimal,
            purchase_price decimal,
            close_price decimal,
            delta decimal,
            delta_diff decimal,
            profit decimal,
            rsi decimal,
            macd decimal,
            macd_delta decimal,
            ttm_squeeze decimal,
            ttm_squeeze_delta decimal,
            sma50 decimal,
            sma20 decimal,
            e_mini_sp decimal,
            volume decimal,
            percentage_moved decimal)""", 'historical_data')'''

        sql_parameter_sets = []
        sql = 'insert into trades (id, code, trade_action, trade_result, scanner, force_closed, trade_opened, trade_closed, stop_loss_500, trade_limit, purchase_price, close_price, delta, delta_diff, profit, rsi, macd, macd_delta, ttm_squeeze, ttm_squeeze_delta, sma50, sma20, e_mini_sp, volume, percentage_moved) values (:id, :code, :trade_action, :trade_result, :scanner, :force_closed, :trade_opened, :trade_closed, :stop_loss_500, :trade_limit, :purchase_price, :close_price, :delta, :delta_diff, :profit, :rsi, :macd, :macd_delta, :ttm_squeeze, :ttm_squeeze_delta, :sma50, :sma20, :e_mini_sp, :volume, :percentage_moved) on conflict (id) do update set code = excluded.code, trade_action = excluded.trade_action, trade_result = excluded.trade_result, scanner = excluded.scanner, force_closed = excluded.force_closed, trade_opened = excluded.trade_opened, trade_closed = excluded.trade_closed, stop_loss_500 = excluded.stop_loss_500, trade_limit = excluded.trade_limit, purchase_price = excluded.purchase_price, close_price = excluded.close_price, delta = excluded.delta, delta_diff = excluded.delta_diff, profit = excluded.profit, rsi = excluded.rsi, macd = excluded.macd, macd_delta = excluded.macd_delta, ttm_squeeze = excluded.ttm_squeeze, ttm_squeeze_delta = excluded.ttm_squeeze_delta, sma50 = excluded.sma50, sma20 = excluded.sma20, e_mini_sp = excluded.e_mini_sp, volume = excluded.volume, percentage_moved = excluded.percentage_moved;'
        
        for code in historical_trades.copy().keys():
            trade = historical_trades[code]

            #if not trade['in_rds']:
            try:
                entry = [
                    {'name':'id', 'value':{'stringValue': trade['id']}},
                    {'name':'code', 'value':{'stringValue': code}},
                    {'name':'trade_action', 'value':{'stringValue': trade['trade_action']}},
                    {'name':'trade_result', 'value':{'stringValue': trade['trade_result']}},
                    {'name':'scanner', 'value':{'stringValue': trade['scanner']}},
                    {'name':'force_closed', 'value':{'booleanValue': trade['force_closed']}},
                    {'name':'trade_opened', 'value':{'doubleValue': trade['trade_opened']}},
                    {'name':'trade_closed', 'value':{'doubleValue': trade['trade_closed']}},
                    {'name':'stop_loss_500', 'value':{'doubleValue': round(trade['stop_loss_500'], 2)}},
                    {'name':'trade_limit', 'value':{'doubleValue': round(trade['limit'], 2)}},
                    {'name':'purchase_price', 'value':{'doubleValue': round(trade['purchase_price'], 2)}},
                    {'name':'close_price', 'value':{'doubleValue': round(trade['close_price'], 2)}},
                    {'name':'delta', 'value':{'doubleValue': round(trade['delta'], 2)}},
                    {'name':'delta_diff', 'value':{'doubleValue': round(trade['delta_diff'], 2)}},
                    {'name':'profit', 'value':{'doubleValue': round(trade['profit'], 2)}},
                    {'name':'rsi', 'value':{'doubleValue': round(trade['rsi'], 2)}},
                    {'name':'macd', 'value':{'doubleValue': round(trade['macd'], 2)}},
                    {'name':'macd_delta', 'value':{'doubleValue': round(trade['macd_delta'], 2)}},
                    {'name':'ttm_squeeze', 'value':{'doubleValue': round(trade['ttm_squeeze'], 2)}},
                    {'name':'ttm_squeeze_delta', 'value':{'doubleValue': round(trade['ttm_squeeze_delta'], 2)}},
                    {'name':'sma50', 'value':{'doubleValue': round(trade['sma50'], 2)}},
                    {'name':'sma20', 'value':{'doubleValue': round(trade['sma20'], 2)}},
                    {'name':'sma5', 'value':{'doubleValue': round(trade['sma5'], 2)}},
                    {'name':'e_mini_sp', 'value':{'doubleValue': round(trade['e_mini_sp'], 2)}},
                    {'name':'volume', 'value':{'doubleValue': trade['volume']}},
                    {'name':'percentage_moved', 'value':{'doubleValue': round(trade['percentage_moved'], 2)}}
                ]

                sql_parameter_sets.append(entry)

                if len(sql_parameter_sets) >= 1000:
                    print ('Inserting {} records into trades in historical data.'.format(len(sql_parameter_sets))) 
                    batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
                    sql_parameter_sets.clear()

            except Exception as e:
                print('all_trades_to_rds() - inserting trades to rds failed,', e)
                time.sleep(1)

            trade['in_rds'] = True

        # insert outstanding records
        try:
            if len(sql_parameter_sets) > 0:
                print ('Inserting {} records into trades in historical data.'.format(len(sql_parameter_sets)))  
                batch_execute_statement(sql, 'historical_data', sql_parameter_sets)
        except Exception as e:
            print('all_trades_to_rds() - inserting trades to rds failed,', e)

        historical_trades = {}

    print ('all_trades_to_rds() - complete move to historical data.', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

# init candle history

def init_candle_history(us_stocks):
    global secretManagerClient
    print ('Executing init_candle_history for stock')
    
    secret = secretManagerClient.getSecretString('levitrade_history')
    if secret:
        est_date = datetime.datetime.now(tz=eastern_time_zone)
        start_date = (est_date - timedelta(days=5))
        end_date = est_date
        print ('Retrieving history between', start_date.strftime('%Y-%m-%d'), 'and', end_date.strftime('%Y-%m-%d'))

        for stock in us_stocks:
            code = stock['code']

            try:
                select = "select * from candles_5_min where code=\'{}\' and timestamp >= \'{}\' and timestamp <= \'{}\';""".format(code, int(start_date.timestamp()) * 1000, int(end_date.timestamp()) * 1000)
                response = execute_statement(select, 'historical_data')
                candles = {}

                for record in response['records']:
                    timestamp = int(record[6]['stringValue'])
                    # [{'stringValue': 'MIME_1588343400000'}, {'stringValue': 'MIME'}, {'stringValue': '2020-05-01 10:30'}, {'stringValue': '18'}, {'stringValue': 'May'}, {'stringValue': '2020'}, {'stringValue': '1588343400000'}, {'stringValue': '39.43'}, {'stringValue': '39.51'}, {'stringValue': '39.53'}, {'stringValue': '39.43'}, {'stringValue': '3953'}]
                    candles[timestamp] = {
                        'timestamp': timestamp,
                        'date': record[2]['stringValue'],
                        'open': float(record[7]['stringValue']),
                        'close': float(record[8]['stringValue']),
                        'high': float(record[9]['stringValue']),
                        'low': float(record[10]['stringValue']),
                        'volume': float(record[11]['stringValue'])
                    }
                
                print ('Retreived {} candles for stock'.format(len(candles)), code)

            except Exception as e:
                print('init_candle_history failed for stock', code, e)
                time.sleep(1)

    print ('Complete init_candle_history at ', datetime.datetime.now(tz=eastern_time_zone).strftime('%Y-%m-%d %H:%M'))

# process raw data from socket into readable dict and makes update to live stock status

def process_ib_tick_data(ws, message):
    global pre_market_start_time
    global pre_market_stop_time
    global post_market_start_time
    global post_market_stop_time
    
    try:
        print (message)
    except Exception as e:
        print ('process_polygon_tick_data failed', e, message)

def process_polygon_tick_data(ws, message):
    global pre_market_start_time
    global pre_market_stop_time
    global post_market_start_time
    global post_market_stop_time
    
    try:
        msg_objects = json.loads(message)
        date_now = datetime.datetime.now(tz=eastern_time_zone)

        for obj in msg_objects:
            # 'T' trade subscription holds most current price
            if 'T' in obj['ev'] and 's' in obj.keys() and not polygon_connecting:
                code = obj['sym']
                price = obj['p']
                volume = obj['s']

                if code in price_ticks.keys():
                    price_ticks[code].append({
                        'id': obj['i'],
                        'code': code,
                        'timestamp_received': int(date_now.timestamp()) * 1000,
                        'timestamp': obj['t'],
                        'price': price,
                        'volume': volume
                    })

                    check_trade = False

                    if 'c' in obj:
                        if 15 not in obj['c'] and 16 not in obj['c'] and 38 not in obj['c']:
                            daily_stocks_status[code]['volume'] += volume
                            if date_now >= pre_market_start_time and date_now <= pre_market_stop_time:
                                daily_stocks_status[code]['pre_market_volume'] += volume
                            elif date_now >= post_market_start_time and date_now <= post_market_stop_time:
                                daily_stocks_status[code]['post_market_volume'] += volume

                        if 2 not in obj['c'] and 7 not in obj['c'] and 12 not in obj['c'] and 13 not in obj['c'] and 20 not in obj['c'] and 21 not in obj['c'] and 37 not in obj['c'] and 52 not in obj['c'] and 53 not in obj['c']:
                            if 38 not in obj['c']:
                                if daily_stocks_status[code]['open'] == -1:
                                    daily_stocks_status[code]['open'] = price
                            
                            if price < daily_stocks_status[code]['low']:
                                daily_stocks_status[code]['low'] = price
                            if price > daily_stocks_status[code]['high']:
                                daily_stocks_status[code]['high'] = price

                            if 5 not in obj['c'] and 6 not in obj['c'] and 10 not in obj['c'] and 22 not in obj['c'] and 29 not in obj['c'] and 33 not in obj['c']:
                                # update daily status for daily candle
                                daily_stocks_status[code]['close'] = price
                                
                            # update daily status
                            daily_stocks_status[code]['last_price'] = price
                            check_trade = True
                    else:
                        daily_stocks_status[code]['volume'] += volume
                        if date_now >= pre_market_start_time and date_now <= pre_market_stop_time:
                            daily_stocks_status[code]['pre_market_volume'] += volume
                        elif date_now >= post_market_start_time and date_now <= post_market_stop_time:
                            daily_stocks_status[code]['post_market_volume'] += volume
                            
                        if daily_stocks_status[code]['open'] == -1:
                            daily_stocks_status[code]['open'] = price
                        if price < daily_stocks_status[code]['low']:
                            daily_stocks_status[code]['low'] = price
                        if price > daily_stocks_status[code]['high']:
                            daily_stocks_status[code]['high'] = price
                        # update daily status for daily candle
                        daily_stocks_status[code]['close'] = price
                        # update daily status
                        daily_stocks_status[code]['last_price'] = price
                        check_trade = True
                            
            # printing status messages on authentication and socket open
            elif 'status' in obj['ev']:
                if 'auth_success' in obj['status']:
                    print ('Polygon socket authenticated,', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
                    polygon_authenticated.set()
                elif 'connected' in obj['status']:
                    print ('Polygon socket connected,', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))
                elif 'success' not in obj['status']:
                    print (obj)
            #else:
            #    print ('process_message() - obj issue from source message', obj)

    except Exception as e:
        print ('process_polygon_tick_data failed', e, message)

# polygon web socket

def process_polygon_on_error(ws, error):
    global polygon_connecting
    global did_action_close_polygon_connection
    print("Socket error.", error, did_action_close_polygon_connection)

    # reconnect
    if not did_action_close_polygon_connection and not polygon_connecting:
        print("Reconnecting.")
        restart_candle_creation()

def process_polygon_on_close(ws):
    global polygon_connecting
    global did_action_close_polygon_connection
    print("Socket close.", did_action_close_polygon_connection)

    # reconnect
    if not did_action_close_polygon_connection and not polygon_connecting:
        print("Reconnecting.")
        restart_candle_creation()
 
# create a single daily candle

def create_daily_candle(code):
    try:
        # set period candle to last tick close
        close_price = daily_stocks_status[code]['close']
        low_price = daily_stocks_status[code]['low']
        high_price = daily_stocks_status[code]['high']
        open_price = daily_stocks_status[code]['open']
        volume = daily_stocks_status[code]['volume']

        est_date = datetime.datetime.now(tz=eastern_time_zone)
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

def create_real_time_candle(code):
    est_date = datetime.datetime.now(tz=eastern_time_zone)
    # round up mini to nearest multiple of 5
    nearest_5_min = int((floor(est_date.minute / 5) * 5))
    current_hour = est_date.hour

    # make sure we increment hour if we hit 60 mins
    if nearest_5_min == 60:
        nearest_5_min = 0
        current_hour += 1

    est_date_nearest_5_min = est_date.replace(hour=current_hour, minute=nearest_5_min, second=0, microsecond=0)
    candle_start_timestamp = int(est_date_nearest_5_min.timestamp()) * 1000

    open_price = -1
    low_price = 10000000000000000
    high_price = 0
    close_price = 0
    volume = 0
    
    for tick in price_ticks[code]:
        # we need to ensure all ticks are exactly within bounds of the 5 min period
        if tick['timestamp'] >= candle_start_timestamp:
            if open_price == -1:
                open_price = tick['price']
            if tick['price'] < low_price:
                low_price = tick['price']
            if tick['price'] > high_price:
                high_price = tick['price']

            close_price = tick['price']
            volume += tick['volume']

    return {
        'date': est_date_nearest_5_min.strftime('%Y-%m-%d %H:%M'),
        'close': close_price,
        'low': low_price,
        'high': high_price,
        'open': open_price,
        'volume': volume,
        'in_rds': False
    }

# create a single candle 5 min

def update_5_min_candles(code, candle_start_timestamp, candle_end_timestamp, date):
    try :
        volume = 0
        open_price = -1
        close_price = -1
        low_price = 10000000000000000
        high_price = -1
        candle_complete = False

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

        # only create candles when volume is > 100
        if volume >= 100:
            stocks_5_min_candles[code][candle_start_timestamp] = {
                'timestamp': candle_start_timestamp,
                'start_timestamp': candle_start_timestamp,
                'end_timestamp': candle_end_timestamp,
                'date': date,
                'close': close_price,
                'low': low_price,
                'high': high_price,
                'open': open_price,
                'volume': volume,
                'in_rds': False
            }
            
    except Exception as e:
        print ('update_5_min_candles failed for stock'.format(code), e)
    
# remove all ticks 15 mins old

def remove_old_ticks(est_date):
    global removing_ticks_in_progress

    if not removing_ticks_in_progress:
        removing_ticks_in_progress = True
        print ('remove_old_ticks() previous to', est_date.strftime('%Y-%m-%d %H:%M:%S'))
        timestamp = est_date.timestamp() * 1000

        # process all ticks and remove all ticks that are over 15 mins old - reducing size of ticks
        for code in price_ticks.keys():
            for tick in price_ticks[code]:
                try:
                    if tick['timestamp'] < timestamp:
                        ticks_to_move.append(tick)
                        price_ticks[code].remove(tick)
                        
                except Exception as e:
                    print('remove_old_ticks() failed with stock {} - tick timestamp {}, old timestamp'.format(code, tick['timestamp'], timestamp), e) 
                    print('remove_old_ticks() invalid tick', tick) 
                    print('remove_old_ticks() from tick array', price_ticks[code]) 
        
        removing_ticks_in_progress = False
    else:
        print('remove_old_ticks() - removal in progress, skipping interval.') 

# process_price_ticks and start for all stocks 

def process_price_ticks():
    global market_direction
    global start_trading_time
    global no_more_trades_time
    global candle_start_time
    global candle_stop_time
    global open_market_trading_period_finish_time

    est_date = datetime.datetime.now(tz=eastern_time_zone)
    print ('process_price_ticks() at', est_date.strftime('%Y-%m-%d %H:%M:%S'))

    dates = []
    # round up mini to nearest multiple of 5
    nearest_5_min = int((ceil(est_date.minute / 5) * 5))
    current_hour = est_date.hour

    # make sure we increment hour if we hit 60 mins
    if nearest_5_min == 60:
        nearest_5_min = 0
        current_hour += 1

    est_date_nearest_5_min = est_date.replace(hour=current_hour, minute=nearest_5_min, second=0, microsecond=0)
    oldest_date = 0

    # create 2, 5 min period timestamps prior to current datetime
    for count in range(1, 3):
        dates.append({
            'candle_start_timestamp': int((est_date_nearest_5_min - timedelta(minutes=(count * 5))).timestamp()) * 1000,
            'candle_end_timestamp': int((est_date_nearest_5_min - timedelta(minutes=((count - 1) * 5))).timestamp()) * 1000,
            'date': (est_date_nearest_5_min - timedelta(minutes=(count * 5))).strftime('%Y-%m-%d %H:%M')
        })
        # the last entry 'candle_start_timestamp' will be 10 mins old, so we want to remove all ticks previous to this time
        oldest_date = est_date_nearest_5_min - timedelta(minutes=(count * 5))

    num_tick_stocks = 0
    
    # process all ticks
    for code in price_ticks.copy().keys():
        # if either close, low or high not assigned in time period, we had no price updates, so create no candle
        if len(price_ticks[code]) > 0:
            num_tick_stocks += 1
            # using the 5 min periods created above, we create three 5 min candle periods in the past from rounding current time min to nearest 5 min up
            for date in dates:
                update_5_min_candles(code, date['candle_start_timestamp'], date['candle_end_timestamp'], date['date'])
            candles = sorted(stocks_5_min_candles[code].values(), key = lambda i: i['timestamp'], reverse=True)

            if est_date >= start_trading_time and est_date <= no_more_trades_time:
                if code in open_trades.keys():
                    check_trade_exit(code)
                    
            if len(candles) >= 80:
                update_stock_indicators(code, candles)
                # set last volume
                daily_stocks_status[code]['last_volume'] = candles[0]['volume']
                # check stock behaviors
                check_stock_momentum(code, candles)
                check_stock_basing_and_exhaustion(code, candles)
                check_stock_ranging(code, candles)
                check_stock_trending(code, candles)

        if est_date >= start_trading_time and est_date <= no_more_trades_time:
            is_market_mover = False
            scanner = ''
            # we need to make sure volume scanner stocks are trending to market direction
            if not past_traded(code) and (daily_stocks_status[code]['last_price'] >= 4 and daily_stocks_status[code]['last_price'] <= 1000):
                if daily_stocks_status[code]['scanner_stock_momentum']:
                    # check stock is a scanner stock and has not previously traded today
                    Thread(target=check_trade_entry, args=[code, 'momentum', 'momentum']).start()
                # also check if stock was a volume mover
                if daily_stocks_status[code]['scanner_stock_basing']:
                    Thread(target=check_trade_entry, args=[code, 'basing', 'basing']).start()
                # also check if stock was a volume mover
                if daily_stocks_status[code]['scanner_stock_volume']:
                    Thread(target=check_trade_entry, args=[code, 'volume', 'volume']).start()
                # also check if stock was a volume mover
                if daily_stocks_status[code]['scanner_stock_exhausted']:
                    Thread(target=check_trade_entry, args=[code, 'exhausted', 'exhausted']).start()
                # also check if stock was a volume mover
                if daily_stocks_status[code]['scanner_stock_ranging']:
                    Thread(target=check_trade_entry, args=[code, 'ranging', 'ranging']).start()
                # buy and sell market movers, check all algorithms if market direction correct
                if daily_stocks_status[code]['scanner_stock_buy_mover'] and 'buy' in market_direction:
                    Thread(target=check_trade_entry, args=[code, 'momentum', 'buy_mover']).start()
                    # ensure stock is in basing scanner
                    if daily_stocks_status[code]['scanner_stock_basing']:
                        Thread(target=check_trade_entry, args=[code, 'basing', 'buy_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_volume']:
                        Thread(target=check_trade_entry, args=[code, 'volume', 'buy_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'exhausted', 'buy_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'ranging', 'buy_mover']).start()
                if daily_stocks_status[code]['scanner_stock_sell_mover'] and 'sell' in market_direction:
                    Thread(target=check_trade_entry, args=[code, 'momentum', 'sell_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_basing']:
                        Thread(target=check_trade_entry, args=[code, 'basing', 'sell_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_volume']:
                        Thread(target=check_trade_entry, args=[code, 'volume', 'sell_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'exhausted', 'sell_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'ranging', 'sell_mover']).start()
                # pre and post market movers, check all algorithms
                if daily_stocks_status[code]['scanner_stock_pre_mover']:
                    Thread(target=check_trade_entry, args=[code, 'momentum', 'pre_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_basing']:
                        Thread(target=check_trade_entry, args=[code, 'basing', 'pre_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_volume']:
                        Thread(target=check_trade_entry, args=[code, 'volume', 'pre_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'exhausted', 'pre_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'ranging', 'pre_mover']).start()
                if daily_stocks_status[code]['scanner_stock_post_mover']:
                    Thread(target=check_trade_entry, args=[code, 'momentum', 'post_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_basing']:
                        Thread(target=check_trade_entry, args=[code, 'basing', 'post_mover']).start()
                    if daily_stocks_status[code]['scanner_stock_volume']:
                        Thread(target=check_trade_entry, args=[code, 'volume', 'post_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'exhausted', 'post_mover']).start()
                    Thread(target=check_trade_entry, args=[code, 'ranging', 'post_mover']).start()

    print ('process_price_ticks() - total stocks moved within period {}/{}'.format(num_tick_stocks, len(price_ticks.keys())))
        
    # save daily status to text and upload to s3, this saves us time to load all US stocks status into memory
    #Thread(target=save_candles_5_min_status).start()
    Thread(target=save_intraday_movement).start()
    #Thread(target=all_5_min_candles_to_rds).start()
    Thread(target=remove_old_ticks, args=[oldest_date]).start()

# calculates trade movement and determine if stock should be added as movement stock

def check_stock_trending(code, candles):   
    try:
        trend = 'short'
        if candles[0]['di_diff'] > 0:
            trend = 'long'

        daily_stocks_status[code]['trend'] = trend
        daily_stocks_status[code]['trend_strength_dx'] = candles[0]['dx']
            
    except Exception as ex:
        print ("check_stock_trending() failed with stock", code, ex)

def check_stock_momentum(code, candles):   
    try:
        if daily_stocks_status[code]['previous_day_close_price'] > 0 and daily_stocks_status[code]['last_price'] > 0:
            previous_day_close_price = daily_stocks_status[code]['previous_day_close_price']
            last_price = daily_stocks_status[code]['last_price']
            total_daily_price_move = last_price - previous_day_close_price
            half_percent_price_value = previous_day_close_price / 100 * 0.5
            volume = candles[0]['volume']
            percentage_moved = total_daily_price_move / previous_day_close_price * 100
            three_candle_diff = candles[0]['close'] - candles[2]['close']
            quarter_percent_price_value = candles[2]['close'] / 100 * 0.25
            price_diff_large_enough = False

            if percentage_moved < 0:
                stock_move_quarter_percent_in_three_bars = three_candle_diff <= (quarter_percent_price_value * -1)
                move_to_trend_in_last_bar = candles[0]['close'] < candles[1]['close']
                price_diff_large_enough = total_daily_price_move <= (half_percent_price_value * -1)
            else:
                stock_move_quarter_percent_in_three_bars = three_candle_diff >= quarter_percent_price_value
                move_to_trend_in_last_bar = candles[0]['close'] > candles[1]['close']
                price_diff_large_enough = total_daily_price_move >= half_percent_price_value

            percent_move_in_three_bars = three_candle_diff / candles[2]['close'] * 100

            daily_stocks_status[code]['total_daily_price_move'] = total_daily_price_move
            daily_stocks_status[code]['percentage_moved'] = percentage_moved
            daily_stocks_status[code]['stock_move_quarter_percent_in_three_bars'] = stock_move_quarter_percent_in_three_bars
            daily_stocks_status[code]['move_to_trend_in_last_bar'] = move_to_trend_in_last_bar
            daily_stocks_status[code]['percent_move_in_three_bars'] = percent_move_in_three_bars
            daily_stocks_status[code]['movement_stock'] = abs(percentage_moved) >= 2 and price_diff_large_enough

    except Exception as ex:
        print ("check_stock_trending failed with stock", code, ex)

# check if stock is showing signs of basing and exhaustion

def check_stock_basing_and_exhaustion(code, candles):
    try :
        high = 0.0
        low = 10000000000.0
        price_high = 0.0
        price_low = 10000000000.0

        for i in range(0, 4):
            if candles[i]['close'] > price_high:
                price_high = candles[i]['close']
            if candles[i]['close'] < price_low:
                price_low = candles[i]['close']
            if candles[i]['price_percent_move_in_4_bars'] > high:
                high = candles[i]['price_percent_move_in_4_bars']
            if candles[i]['price_percent_move_in_4_bars'] < low: 
                low = candles[i]['price_percent_move_in_4_bars']

        price_percent_move_in_10_bars = high

        # if latest candle price action has moved
        if price_percent_move_in_10_bars < 30:
            daily_stocks_status[code]['basing_price_high'] = price_high
            daily_stocks_status[code]['basing_price_low'] = price_low

        high = 0
        low = 10000000000
        for i in range(0, 4):
            if candles[i]['rsi'] > high:
                high = candles[i]['rsi']
            if candles[i]['rsi'] < low: 
                low = candles[i]['rsi']

        # update rsi daily bounds
        if high > daily_stocks_status[code]['rsi_daily_high']:
            daily_stocks_status[code]['rsi_daily_high'] = high
        if low < daily_stocks_status[code]['rsi_daily_low']: 
            daily_stocks_status[code]['rsi_daily_low'] = low

        rsi_percent_move_in_4_bars = 0
        if daily_stocks_status[code]['rsi_daily_high'] - daily_stocks_status[code]['rsi_daily_low'] > 0:
            rsi_percent_move_in_4_bars = (abs(high - low) / abs(daily_stocks_status[code]['rsi_daily_high'] - daily_stocks_status[code]['rsi_daily_low'])) * 100

        high = 0
        low = 10000000000
        for i in range(0, 4):
            if candles[i]['macd_percent_move_in_4_bars'] > high:
                high = candles[i]['macd_percent_move_in_4_bars']
            if candles[i]['macd_percent_move_in_4_bars'] < low: 
                low = candles[i]['macd_percent_move_in_4_bars']

        macd_percent_move_in_4_bars = high

        # volume basing
        high = 0
        low = 10000000000
        for i in range(0, 2):
            if candles[i]['macd'] > high:
                high = candles[i]['macd']
            if candles[i]['macd'] < low: 
                low = candles[i]['macd']

        macd_percent_move_in_2_bars = 0
        if daily_stocks_status[code]['macd_daily_high'] - daily_stocks_status[code]['macd_daily_low'] > 0:
            macd_percent_move_in_2_bars = (abs(high - low) / abs(daily_stocks_status[code]['macd_daily_high'] - daily_stocks_status[code]['macd_daily_low'])) * 100

        high = 0
        low = 10000000000
        for i in range(0, 4):
            if candles[i]['linreg'] > high:
                high = candles[i]['linreg']
            if candles[i]['linreg'] < low: 
                low = candles[i]['linreg']

        # update ttm squeeze daily bounds
        if high > daily_stocks_status[code]['ttm_squeeze_daily_high']:
            daily_stocks_status[code]['ttm_squeeze_daily_high'] = high
        if low < daily_stocks_status[code]['ttm_squeeze_daily_low']: 
            daily_stocks_status[code]['ttm_squeeze_daily_low'] = low

        ttm_squeeze_percent_move_in_4_bars = 0
        if daily_stocks_status[code]['ttm_squeeze_daily_high'] - daily_stocks_status[code]['ttm_squeeze_daily_low'] > 0:
            ttm_squeeze_percent_move_in_4_bars = (abs(high - low) / abs(daily_stocks_status[code]['ttm_squeeze_daily_high'] - daily_stocks_status[code]['ttm_squeeze_daily_low'])) * 100
        
        # calculate total volume in last 4 bars
        volume_move_in_4_bars = 0
        for i in range(0, 4):
            volume_move_in_4_bars += candles[i]['volume']

        daily_stocks_status[code]['price_percent_move_in_10_bars'] = price_percent_move_in_10_bars
        daily_stocks_status[code]['rsi_percent_move_in_4_bars'] = rsi_percent_move_in_4_bars
        daily_stocks_status[code]['macd_percent_move_in_4_bars'] = macd_percent_move_in_4_bars
        daily_stocks_status[code]['macd_percent_move_in_2_bars'] = macd_percent_move_in_2_bars
        daily_stocks_status[code]['ttm_squeeze_percent_move_in_4_bars'] = ttm_squeeze_percent_move_in_4_bars
        daily_stocks_status[code]['volume_move_in_4_bars'] = volume_move_in_4_bars
        daily_stocks_status[code]['volume_percent_move_in_4_bars'] = 0
        if daily_stocks_status[code]['volume'] > 0:
            daily_stocks_status[code]['volume_percent_move_in_4_bars'] = volume_move_in_4_bars / daily_stocks_status[code]['volume'] * 100

    except Exception as e:
        print ('check_stock_basing_and_exhaustion() failed for stock'.format(code), e, code)

def check_stock_ranging(code, candles):
    try :
        low = 10000000000000000
        high = 0

        # check support and resistance levels
        for i in range(0, 20):
            if isSupport(candles, i):
                if candles[i]['low'] < low: 
                    low = candles[i]['low']
            elif isResistance(candles, i):
                if candles[i]['high'] > high:
                    high = candles[i]['high']

        # we need to make sure a resistance and support have been found
        if high > 0 and low > 0 and daily_stocks_status[code]['high'] > 0 and daily_stocks_status[code]['low'] > 0:
            daily_price_range = daily_stocks_status[code]['high'] - daily_stocks_status[code]['low']
            if daily_price_range > 0:
                range_size_in_period = ((high - low) / daily_price_range) * 100
                daily_stocks_status[code]['range_size_in_period'] = range_size_in_period
                daily_stocks_status[code]['ranging_price_low'] = low
                daily_stocks_status[code]['ranging_price_high'] = high

        # if latest candle price action has moved
        if daily_stocks_status[code]['range_size_in_period'] > 60:
            # we already traded so remove from scanner
            daily_stocks_status[code]['scanner_stock_ranging'] = False

    except Exception as e:
        print ('check_stock_ranging() failed for stock'.format(code), e)

# check all stocks for top 100 stocks to trade according to movement, direction, etc.

def update_scanners():
    global e_mini
    global market_direction
    global candle_start_time
    global candle_stop_time
    global scanner_tick_checks
    global pre_market_start_time
    global pre_market_stop_time
    global post_market_start_time
    global post_market_stop_time
    global appSyncClient

    est_date = datetime.datetime.now(tz=eastern_time_zone)
    # timestamp for candles represents start of a period, not end so we deduct 5 mins of current time
    candle_start_timestamp = int((est_date.replace(second=0, microsecond=0) - timedelta(minutes=1)).timestamp()) * 1000
    candle_end_timestamp = int((est_date.replace(second=0, microsecond=0)).timestamp()) * 1000

    momentum_stocks = []
    volume_stocks = []
    basing_stocks = []
    ranging_stocks = []
    exhausted_stocks = []
    buy_mover_stocks = []
    sell_mover_stocks = []
    pre_mover_stocks = []
    post_mover_stocks = []
    
    try: 
        market_direction = daily_settings_table.get_item(
            Key={
                'key': 'emini_trend',
            }
        )['Item']['value']

        e_mini = daily_settings_table.get_item(
            Key={
                'key': 'emini_value',
            }
        )['Item']['value']

        # reset all stocks
        for code in daily_stocks_status.keys():
            daily_stocks_status[code]['scanner_stock_momentum'] = False
            #daily_stocks_status[code]['scanner_stock_volume'] = False

            # if latest candle price action has moved
            '''if daily_stocks_status[code]['price_percent_move_in_10_bars'] > 50:
                daily_stocks_status[code]['basing_price_high'] = 0
                daily_stocks_status[code]['basing_price_low'] = 0
                daily_stocks_status[code]['scanner_stock_basing'] = False'''

            daily_stocks_status[code]['scanner_stock_exhausted'] = False

            # if latest candle price action has moved
            '''if daily_stocks_status[code]['range_size_in_period'] > 60:
                # we already traded so remove from scanner
                daily_stocks_status[code]['scanner_stock_ranging'] = False'''

            daily_stocks_status[code]['scanner_stock_buy_mover'] = False
            daily_stocks_status[code]['scanner_stock_sell_mover'] = False
            
            '''if est_date >= pre_market_start_time and est_date <= pre_market_stop_time:
                daily_stocks_status[code]['scanner_stock_pre_mover'] = False
            if est_date >= post_market_start_time and est_date <= post_market_stop_time:
                daily_stocks_status[code]['scanner_stock_post_mover'] = False'''
            
            if (daily_stocks_status[code]['percentage_moved'] < 0 and 'buy' in market_direction) or (daily_stocks_status[code]['percentage_moved'] > 0 and 'sell' in market_direction):
                volume_stocks.append({
                    'code': code,
                })
                daily_stocks_status[code]['scanner_stock_volume'] = False

            '''if daily_stocks_status[code]['last_price'] < 4:
                daily_stocks_status[code]['scanner_stock_momentum'] = False
                daily_stocks_status[code]['scanner_stock_volume'] = False
                daily_stocks_status[code]['scanner_stock_basing'] = False
                daily_stocks_status[code]['scanner_stock_exhausted'] = False
                daily_stocks_status[code]['scanner_stock_ranging'] = False
                daily_stocks_status[code]['scanner_stock_buy_mover'] = False
                daily_stocks_status[code]['scanner_stock_sell_mover'] = False
                daily_stocks_status[code]['scanner_stock_pre_mover'] = False
                daily_stocks_status[code]['scanner_stock_post_mover'] = False'''

        appSyncClient.mutate_batch('deleteVolumeScannerStocks', 'mutation($input:[DeleteVolumeScannerStocksInput]!){deleteVolumeScannerStocks(input:$input){code}}', volume_stocks)
        volume_stocks.clear()

        #################### - basing stocks - ####################

        num_of_basing_stocks = 0
        top_basing_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['price_percent_move_in_10_bars'])

        for stock in top_basing_stocks:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]
            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['price_percent_move_in_10_bars'] > 0 and daily_stocks_status[code]['price_percent_move_in_10_bars'] < 30 and daily_stocks_status[code]['last_volume'] >= 10000:
                num_of_basing_stocks += 1
                basing_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'price_percent_move_in_10_bars': daily_stocks_status[code]['price_percent_move_in_10_bars'],
                        'basing_price_high': daily_stocks_status[code]['basing_price_high'],
                        'basing_price_low': daily_stocks_status[code]['basing_price_low']
                    })
                daily_stocks_status[code]['scanner_stock_basing'] = True
        
        appSyncClient.mutate_batch('updateBasingScannerStocks', 'mutation($input:[UpdateBasingScannerStocksInput]!){updateBasingScannerStocks(input:$input){code}}', basing_stocks)
        basing_stocks.clear()

        #################### - basing stocks - ####################

        #################### - exhausted stocks - ####################

        num_of_exhausted_stocks = 0
        top_exhausted_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['price_percent_move_in_10_bars'], reverse=True)

        for stock in top_exhausted_stocks:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]

            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['volume_move_in_4_bars'] > 0 and daily_stocks_status[code]['last_volume'] >= 10000:
                num_of_exhausted_stocks += 1
                exhausted_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'volume_move_in_4_bars': daily_stocks_status[code]['volume_move_in_4_bars'],
                        'volume_percent_move_in_4_bars': daily_stocks_status[code]['volume_percent_move_in_4_bars'],
                        'price_percent_move_in_10_bars': daily_stocks_status[code]['price_percent_move_in_10_bars'],
                        'rsi_percent_move_in_4_bars': daily_stocks_status[code]['rsi_percent_move_in_4_bars'],
                        'macd_percent_move_in_4_bars': daily_stocks_status[code]['macd_percent_move_in_4_bars'],
                        'ttm_squeeze_percent_move_in_4_bars': daily_stocks_status[code]['ttm_squeeze_percent_move_in_4_bars'],
                    })
                daily_stocks_status[code]['scanner_stock_exhausted'] = True

        appSyncClient.mutate_batch('updateExhaustedScannerStocks', 'mutation($input:[UpdateExhaustedScannerStocksInput]!){updateExhaustedScannerStocks(input:$input){code}}', exhausted_stocks)
        exhausted_stocks.clear()

        #################### - exhausted stocks - ####################

        #################### - ranging stocks - ####################

        num_of_ranging_stocks = 0
        # sort by smallest range
        top_ranging_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['range_size_in_period'])

        for stock in top_ranging_stocks:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]

            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['price_percent_move_in_10_bars'] > 0 and daily_stocks_status[code]['price_percent_move_in_10_bars'] < 50 and daily_stocks_status[code]['range_size_in_period'] > 0 and daily_stocks_status[code]['range_size_in_period'] > 0 and daily_stocks_status[code]['range_size_in_period'] < 60 and daily_stocks_status[code]['last_volume'] >= 10000:
                num_of_ranging_stocks += 1
                ranging_stocks.append({
                    'code': code,
                    'last_price': daily_stocks_status[code]['last_price'],
                    'range_size_in_period': daily_stocks_status[code]['range_size_in_period'],
                    'ranging_price_high': daily_stocks_status[code]['ranging_price_high'],
                    'ranging_price_low': daily_stocks_status[code]['ranging_price_low']
                })
                daily_stocks_status[code]['scanner_stock_ranging'] = True

        appSyncClient.mutate_batch('updateRangingScannerStocks', 'mutation($input:[UpdateRangingScannerStocksInput]!){updateRangingScannerStocks(input:$input){code}}', ranging_stocks)
        ranging_stocks.clear()

        #################### - ranging stocks - ####################

        #################### - volume stocks - ####################

        number_of_stocks_with_ticks = 0

        # for all stocks that moved inside 1 min period, calculate total volume, and price move in this period
        for code in daily_stocks_status.keys():
            start_price = -1
            end_price = -1

            # reset volume stock items for new period
            daily_stocks_status[code]['last_price'] = 0
            daily_stocks_status[code]['volume_in_1_min'] = 0
            daily_stocks_status[code]['total_price_move'] = 0
            daily_stocks_status[code]['total_price_move_percentage'] = 0
            daily_stocks_status[code]['volume_1_min_to_percent_daily'] = 0
            daily_stocks_status[code]['volume_5_min_to_percent_daily'] = 0
            
            ticks_within_period = False

            if len(price_ticks[code]) > 0:
                for tick in price_ticks[code]:
                    if tick['timestamp_received'] >= candle_start_timestamp and tick['timestamp_received'] < candle_end_timestamp:
                        if not ticks_within_period:
                            number_of_stocks_with_ticks += 1

                        ticks_within_period = True
                        # for all ticks inside the 1 min period, record the start price, and the end price to calculate total price move in 1 min
                        if start_price == -1:
                            start_price = tick['price']
                        end_price = tick['price']
                        daily_stocks_status[code]['volume_in_1_min'] += tick['volume']
                
                if daily_stocks_status[code]['last_volume'] > 0:
                    current_candle = create_real_time_candle(code)
                    daily_stocks_status[code]['volume_current_to_previous_diff'] = current_candle['volume'] / daily_stocks_status[code]['last_volume']
                
                if daily_stocks_status[code]['volume'] > 0:
                    daily_stocks_status[code]['volume_1_min_to_percent_daily'] = daily_stocks_status[code]['volume_in_1_min'] / daily_stocks_status[code]['volume'] * 100
                    daily_stocks_status[code]['volume_5_min_to_percent_daily'] = daily_stocks_status[code]['last_volume'] / daily_stocks_status[code]['volume'] * 100

                if end_price != -1:
                    daily_stocks_status[code]['last_price'] = end_price
                    daily_stocks_status[code]['total_price_move'] += end_price - start_price
                    daily_stocks_status[code]['total_price_move_percentage'] += daily_stocks_status[code]['total_price_move'] / start_price * 100

        num_of_volume_stocks = 0
        top_volume_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['volume_current_to_previous_diff'], reverse=True)

        for stock in top_volume_stocks:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]

            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['volume_current_to_previous_diff'] > 0 and daily_stocks_status[code]['last_volume'] >= 10000:
                num_of_volume_stocks += 1
                if (daily_stocks_status[code]['percentage_moved'] > 0 and 'buy' in market_direction) or (daily_stocks_status[code]['percentage_moved'] < 0 and 'sell' in market_direction):
                    volume_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'volume_current_to_previous_diff': daily_stocks_status[code]['volume_current_to_previous_diff'],
                        'daily_price_high': daily_stocks_status[code]['high'],
                        'daily_price_low': daily_stocks_status[code]['low']
                    })
                    daily_stocks_status[code]['scanner_stock_volume'] = True

        appSyncClient.mutate_batch('updateVolumeScannerStocks', 'mutation($input:[UpdateVolumeScannerStocksInput]!){updateVolumeScannerStocks(input:$input){code}}', volume_stocks)
        volume_stocks.clear()

        #################### - volume stocks - ####################

        print ('update_scanners() - number of daily_stocks_status with ticks', number_of_stocks_with_ticks)

        if est_date.weekday() < 5 and est_date >= candle_start_time and est_date <= candle_stop_time and number_of_stocks_with_ticks == 0:
            scanner_tick_checks += 1
            print("process_price_ticks() - 0, adding to scanner_tick_checks", scanner_tick_checks)
        if scanner_tick_checks == 2:
            restart_candle_creation()

        '''num_of_volume_stocks = 0
        top_volume_mover_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['volume_in_1_min'], reverse=True)

        for stock in top_volume_mover_stocks:
            # each element is a tuple {'code':{}} 
            code = stock[0]
            stock = stock[1]

            if num_of_volume_stocks < 100 and daily_stocks_status[code]['last_price'] > 0:
                num_of_volume_stocks += 1
                daily_stocks_status[code]['scanner_stock_volume'] = True'''

        #################### - momentum stocks - ####################

        num_of_momentum_stocks = 0
        scanner_stocks_momentum = sorted(daily_stocks_status.items(), key=lambda k_v: abs(k_v[1]['percentage_moved']), reverse=True)
        
        for stock in scanner_stocks_momentum:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]

            if stock['movement_stock']:
                percent_move_in_three_bars = False 
                percentage_moved_in_trend_direction = False

                if 'sell' in market_direction:
                    percent_move_in_three_bars = stock['percent_move_in_three_bars'] <= -0.25
                    percentage_moved_in_trend_direction = stock['percentage_moved'] < 0
                elif 'buy' in market_direction:
                    percent_move_in_three_bars = stock['percent_move_in_three_bars'] >= 0.25
                    percentage_moved_in_trend_direction = stock['percentage_moved'] > 0
                elif 'both' in market_direction:
                    if stock['percentage_moved'] > 0:
                        percent_move_in_three_bars = stock['percent_move_in_three_bars'] >= 0.25
                    else:
                        percent_move_in_three_bars = stock['percent_move_in_three_bars'] <= -0.25

                    # or both days we don't care direction, we just want the largest movers in either direction
                    percentage_moved_in_trend_direction = True

                if percent_move_in_three_bars and percentage_moved_in_trend_direction and daily_stocks_status[code]['last_price'] >= 4 and daily_stocks_status[code]['last_volume'] >= 10000:
                    num_of_momentum_stocks += 1
                    momentum_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'percent_move_in_three_bars': stock['percent_move_in_three_bars'],
                        'percentage_moved': stock['percentage_moved'],
                        'total_daily_price_move': daily_stocks_status[code]['total_daily_price_move'],
                        'trend': daily_stocks_status[code]['trend'],
                        'trend_strength_dx': daily_stocks_status[code]['trend_strength_dx']
                    })
                    daily_stocks_status[code]['scanner_stock_momentum'] = True
    
        appSyncClient.mutate_batch('updateMomentumScannerStocks', 'mutation($input:[UpdateMomentumScannerStocksInput]!){updateMomentumScannerStocks(input:$input){code}}', momentum_stocks)
        momentum_stocks.clear()

        #################### - momentum stocks - ####################

        #################### - buy stocks - ####################

        num_of_buy_mover_stocks = 0
        top_buy_mover_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['percentage_moved'], reverse=True)

        for stock in top_buy_mover_stocks:
            # each element is a tuple {'code':{}} 
            code = stock[0]
            stock = stock[1]

            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['last_volume'] > 10000 and daily_stocks_status[code]['percentage_moved'] > 0:
                num_of_buy_mover_stocks += 1
                buy_mover_stocks.append({
                    'code': code,
                    'last_price': daily_stocks_status[code]['last_price'],
                    'percentage_moved': daily_stocks_status[code]['percentage_moved'],
                    'total_daily_price_move': daily_stocks_status[code]['total_daily_price_move'],
                    'last_volume': daily_stocks_status[code]['last_volume']
                })
                daily_stocks_status[code]['scanner_stock_buy_mover'] = True

        appSyncClient.mutate_batch('updateBuyMoverScannerStocks', 'mutation($input:[UpdateBuyMoverScannerStocksInput]!){updateBuyMoverScannerStocks(input:$input){code}}', buy_mover_stocks)
        buy_mover_stocks.clear()

        #################### - buy stocks - ####################

        #################### - sell stocks - ####################

        num_of_sell_mover_stocks = 0
        top_sell_mover_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['percentage_moved'], reverse=False)

        for stock in top_sell_mover_stocks:
            # each element is a tuple {'code':{}} 
            code = stock[0]
            stock = stock[1]

            if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['last_volume'] > 10000 and daily_stocks_status[code]['percentage_moved'] < 0:
                num_of_sell_mover_stocks += 1
                sell_mover_stocks.append({
                    'code': code,
                    'last_price': daily_stocks_status[code]['last_price'],
                    'percentage_moved': daily_stocks_status[code]['percentage_moved'],
                    'total_daily_price_move': daily_stocks_status[code]['total_daily_price_move'],
                    'last_volume': daily_stocks_status[code]['last_volume']
                })
                daily_stocks_status[code]['scanner_stock_sell_mover'] = True

        appSyncClient.mutate_batch('updateSellMoverScannerStocks', 'mutation($input:[UpdateSellMoverScannerStocksInput]!){updateSellMoverScannerStocks(input:$input){code}}', sell_mover_stocks)
        sell_mover_stocks.clear()

        #################### - sell stocks - ####################

        #################### - pre market stocks - ####################

        if est_date >= pre_market_start_time and est_date <= pre_market_stop_time:
            num_of_pre_mover_stocks = 0
            top_pre_mover_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['pre_market_volume'], reverse=True)

            for stock in top_pre_mover_stocks:
                # each element is a tuple {'code':{}} 
                code = stock[0]
                stock = stock[1]

                if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['pre_market_volume'] > 10000:
                    num_of_pre_mover_stocks += 1
                    pre_mover_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'percentage_moved': daily_stocks_status[code]['percentage_moved'],
                        'total_daily_price_move': daily_stocks_status[code]['total_daily_price_move'],
                        'pre_market_volume': daily_stocks_status[code]['pre_market_volume']
                    })
                    daily_stocks_status[code]['scanner_stock_pre_mover'] = True

        appSyncClient.mutate_batch('updatePreMoverScannerStocks', 'mutation($input:[UpdatePreMoverScannerStocksInput]!){updatePreMoverScannerStocks(input:$input){code}}', pre_mover_stocks)
        pre_mover_stocks.clear()

        #################### - pre market stocks - ####################

        #################### - post market stocks - ####################

        if est_date >= post_market_start_time and est_date <= post_market_stop_time:
            num_of_post_mover_stocks = 0
            top_post_mover_stocks = sorted(daily_stocks_status.items(), key=lambda k_v: k_v[1]['post_market_volume'], reverse=True)

            for stock in top_post_mover_stocks:
                # each element is a tuple {'code':{}} 
                code = stock[0]
                stock = stock[1]

                if daily_stocks_status[code]['last_price'] > 4 and daily_stocks_status[code]['post_market_volume'] > 10000:
                    num_of_post_mover_stocks += 1
                    post_mover_stocks.append({
                        'code': code,
                        'last_price': daily_stocks_status[code]['last_price'],
                        'percentage_moved': daily_stocks_status[code]['percentage_moved'],
                        'total_daily_price_move': daily_stocks_status[code]['total_daily_price_move'],
                        'post_market_volume': daily_stocks_status[code]['post_market_volume']
                    })
                    daily_stocks_status[code]['scanner_stock_post_mover'] = True
        
        appSyncClient.mutate_batch('updatePostMoverScannerStocks', 'mutation($input:[UpdatePostMoverScannerStocksInput]!){updatePostMoverScannerStocks(input:$input){code}}', post_mover_stocks)
        post_mover_stocks.clear()

        #################### - post market stocks - ####################
        
        #################### - momentum stocks - ####################

        '''num_of_momentum_stocks = 0
        scanner_stocks_momentum = sorted(daily_stocks_status.items(), key=lambda k_v: abs(k_v[1]['trend_strength_dx']), reverse=True)

        for stock in scanner_stocks_momentum:
            # each element is a tuple {'code':{}}
            code = stock[0]
            stock = stock[1]

            if num_of_momentum_stocks < 100 and daily_stocks_status[code]['last_price'] > 0 and daily_stocks_status[code]['trend_strength_dx'] > 0:
                num_of_momentum_stocks += 1

                if 'sell' in market_direction and daily_stocks_status[code]['percentage_moved'] < 0:
                    daily_stocks_status[code]['scanner_stock_momentum'] = True
                elif 'buy' in market_direction and daily_stocks_status[code]['percentage_moved'] > 0:
                    daily_stocks_status[code]['scanner_stock_momentum'] = True
                elif 'both' in market_direction:
                    daily_stocks_status[code]['scanner_stock_momentum'] = True'''

        #################### - momentum stocks - ####################

    except Exception as ex:
        print ("update_scanners() failed at", datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"), ex)
    
    print("Completed update_scanners() at", datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d  %H:%M:%S"))

# update data to live tables so website updates
def init_update_scannerss():
    # scheduler events aren't pushed to background, we move scanner updates to background so updating dynamo tables doesn't block
    Thread(target=update_scanners).start()

# start setups globals for trading day

def pre_market_clean_up():
    global sched
    global start_trading_time
    global market_close_time
    global open_market_trading_period_finish_time
    global force_close_trades_time
    global no_more_trades_time
    global candle_start_time
    global candle_stop_time
    global pre_market_start_time
    global pre_market_stop_time
    global post_market_start_time
    global post_market_stop_time
    global scheduler_started
    global polygon_connecting
    global total_trades
    global total_momentum_trades
    global total_volume_trades
    global total_basing_trades
    global total_exhausted_trades
    global did_action_close_polygon_connection
    global number_of_stocks_subscribed
    global total_stocks_in_group
    global secretManagerClient
    global appSyncClient

    try:
        polygon_connecting = True
        did_action_close_polygon_connection = False

        date_now = datetime.datetime.now(tz=eastern_time_zone)
        # set daily times
        start_trading_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=10, minute=0)
        open_market_trading_period_finish_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=10, minute=0)
        market_close_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=16, minute=0)
        force_close_trades_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=15, minute=50)
        no_more_trades_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=15, minute=0)
        candle_start_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=4, minute=0)
        candle_stop_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=21, minute=0)
        pre_market_start_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=4, minute=0)
        pre_market_stop_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=9, minute=30)
        post_market_start_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=16, minute=0)
        post_market_stop_time = datetime.datetime.now(tz=eastern_time_zone).replace(hour=21, minute=0)

        if date_now < start_trading_time:
            # clear ib order tables on market open
            if os.environ['us_stocks_group'] == '1':
                ib_orders_table_response = ib_orders_table.scan()
                for stock in ib_orders_table_response['Items']:
                    ib_orders_table.delete_item(
                        Key={
                            'key': stock['key']
                        })
                
                ib_order_failures_table_response = ib_order_failures_table.scan()
                for stock in ib_order_failures_table_response['Items']:
                    ib_order_failures_table.delete_item(
                        Key={
                            'key': stock['key']
                        })

            # clear bad open trades and all historical trades if before market open
            open_trades_response = open_trades_table.scan()
            for stock in open_trades_response['Items']:
                if stock['code'] in daily_stocks_status.keys():
                    open_trades_table.delete_item(
                        Key={
                            'code': stock['code']
                        })
        
    except Exception as e:
        print ('pre_market_clean_up() failed', e)

    # we want to complete finish start function before we allow another connection
    polygon_connecting = False

    print ('pre_market_clean_up() at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# market open

def market_open():
    global global_action

    global_action = 'market_open'
    date_now = datetime.datetime.now(tz=eastern_time_zone)

    print ('levitrade.v2.scnner: market_open() at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# market close - function called at 16:00 EST

def market_close():
    scanner_stocks_post_movers_table_response = scanner_stocks_post_movers_table.scan()
    for stock in scanner_stocks_post_movers_table_response['Items']:
        if stock['code'] in daily_stocks_status.keys():
            scanner_stocks_post_movers_table.delete_item(
                Key={
                    'code': stock['code']
                })
    
    print ('levitrade.v2.scnner: market_close() at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# clean up dynamo tables
def end_post_market_clean_up():
    try:
        # clear scanner dynamo tables
        scanner_stocks_momentum_table_response = scanner_stocks_momentum_table.scan()
        for stock in scanner_stocks_momentum_table_response['Items']:
            # make sure we only manage the stocks in our group
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_momentum_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        scanner_stocks_volume_table_response = scanner_stocks_volume_table.scan()
        for stock in scanner_stocks_volume_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_volume_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        daily_stocks_status_basing_table_response = scanner_stocks_basing_table.scan()
        for stock in daily_stocks_status_basing_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_basing_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        daily_stocks_status_ranging_table_response = scanner_stocks_ranging_table.scan()
        for stock in daily_stocks_status_ranging_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_ranging_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        scanner_stocks_exhausted_table_response = scanner_stocks_exhausted_table.scan()
        for stock in scanner_stocks_exhausted_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_exhausted_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        scanner_stocks_sell_movers_table_response = scanner_stocks_sell_movers_table.scan()
        for stock in scanner_stocks_sell_movers_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_sell_movers_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        scanner_stocks_buy_movers_table_response = scanner_stocks_buy_movers_table.scan()
        for stock in scanner_stocks_buy_movers_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_buy_movers_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

        scanner_stocks_pre_movers_table_response = scanner_stocks_pre_movers_table.scan()
        for stock in scanner_stocks_pre_movers_table_response['Items']:
            if stock['code'] in daily_stocks_status.keys():
                scanner_stocks_pre_movers_table.delete_item(
                    Key={
                        'code': stock['code']
                    })

    except Exception as ex:
        print ('levitrade.v2.scnner: end_post_market_clean_up() failed', ex)

    print ('levitrade.v2.scnner: end_post_market_clean_up() at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# function called after excel has been generated to clear tables

def post_market_clean_up():
    global sched
    global open_trades

    response = daily_settings_table.put_item(
        Item={
            'key': 'total_trades',
            'value': 0
        })

    response = daily_settings_table.put_item(
        Item={
            'key': 'total_momentum_trades',
            'value': 0
        })

    response = daily_settings_table.put_item(
        Item={
            'key': 'total_volume_trades',
            'value': 0
        })

    response = daily_settings_table.put_item(
        Item={
            'key': 'total_basing_trades',
            'value': 0
        })

    response = daily_settings_table.put_item(
        Item={
            'key': 'total_exhausted_trades',
            'value': 0
        })

    open_trades = {}
    
    print ('levitrade.v2.scnner: post_market_clean_up() at', datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))

# TEMP: implementation of socket handling, will soon move to seperate class
if __name__ == "__main__":
    try:
        global trade_quantity_dollar_value
        global open_trades_table
        global open_trades_volume_table
        global trade_history_table
        global trade_history_volume_table
        global scanner_stocks_momentum_table
        global scanner_stocks_volume_table
        global scanner_stocks_basing_table
        global scanner_stocks_exhausted_table
        global scanner_stocks_ranging_table
        global scanner_stocks_buy_movers_table
        global scanner_stocks_sell_movers_table
        global scanner_stocks_pre_movers_table
        global scanner_stocks_post_movers_table
        global status_bucket
        global db_cluster_arn
        global total_stocks_in_group
        global ib_socket_connected
        global ib_connecting
        global polygon_socket_connected
        global polygon_connecting
        global appSyncClient
        global interactiveBrokersDataClient
        global secretManagerClient
        global ibManager
        global signalRClient

        trade_quantity_dollar_value = 20000
        total_stocks_in_group = 0
        ib_socket_connected = False
        ib_connecting = False
        polygon_socket_connected = False
        polygon_connecting = False

        sched = BackgroundScheduler(timezone="US/Eastern")

        print ('Running levitrade.v2.scanner...')

        # if test environment    
        if 'polygon_auth_secret' not in os.environ.keys():
            os.environ['polygon_auth_secret'] = 'polygon_auth_key_test'
            os.environ['us_stocks_group'] = '1'
            dynamodb_environment = boto3.resource('dynamodb', region_name='us-east-1')
            status_bucket = 's3.levitrade.status-staging'
            db_cluster_arn = 'arn:aws:rds:us-east-1:080872292485:cluster:levitrade-historical-data-staging'
            async_host = '4u25g4acjveylf774pywrzec4i'
            api_key = 'x-api-key-staging'
        else:
            dynamodb_environment = boto3.resource('dynamodb', region_name='us-east-2')
            status_bucket = 's3.levitrade.status'
            db_cluster_arn = 'arn:aws:rds:us-east-1:080872292485:cluster:levitrade-historical-data'
            async_host = 'ys7qs4lqvfcbzd5vco6mlu2kym'
            api_key = 'x-api-key'

        signalRClient = SignalRClient('', '')
        appSyncClient = AppSyncClient(async_host, api_key)
        interactiveBrokersDataClient = IBWebSocketClient(on_message=process_ib_tick_data)
        secretManagerClient = SecretManagerClient('us-east-1')
        ibManager = IBManager()
        interactiveBrokersDataClient.run()

        # we split stocks over 2 servers to handle the large amount of concurrent data in peak market periods
        group = int(os.environ['us_stocks_group'])
        stocks_response = us_stock_symbols_table.scan(
            FilterExpression=Key('group').eq(group)
        )
        # open trades
        open_trades_table = dynamodb_environment.Table('open_trades')
        open_trades_volume_table = dynamodb_environment.Table('open_trades_volume')
        # trade history
        trade_history_table = dynamodb_environment.Table('trade_history')
        trade_history_volume_table = dynamodb_environment.Table('trade_history_volume')
        # scanners
        scanner_stocks_momentum_table = dynamodb_environment.Table('scanner_stocks_momentum')
        scanner_stocks_volume_table = dynamodb_environment.Table('scanner_stocks_volume')
        scanner_stocks_basing_table = dynamodb_environment.Table('scanner_stocks_basing')
        scanner_stocks_exhausted_table = dynamodb_environment.Table('scanner_stocks_exhausted')
        scanner_stocks_ranging_table = dynamodb_environment.Table('scanner_stocks_ranging')
        scanner_stocks_buy_movers_table = dynamodb_environment.Table('scanner_stocks_buy_movers')
        scanner_stocks_sell_movers_table = dynamodb_environment.Table('scanner_stocks_sell_movers')
        scanner_stocks_pre_movers_table = dynamodb_environment.Table('scanner_stocks_pre_movers')
        scanner_stocks_post_movers_table = dynamodb_environment.Table('scanner_stocks_post_movers')

        sched.add_job(init_update_scannerss, CronTrigger.from_crontab('*/1 4-21 * * MON-FRI', timezone="US/Eastern"), id='init_update_scannerss')
        sched.add_job(market_open, CronTrigger.from_crontab('0 9 * * MON-FRI', timezone="US/Eastern"), id='market_open')
        sched.add_job(market_close, CronTrigger.from_crontab('0 16 * * MON-FRI', timezone="US/Eastern"), id='market_close')
        sched.add_job(pre_market_clean_up, CronTrigger.from_crontab('58 3 * * MON-FRI', timezone="US/Eastern"), id='pre_market_clean_up')
        sched.add_job(post_market_clean_up, CronTrigger.from_crontab('10 16 * * MON-FRI', timezone="US/Eastern"), id='post_market_clean_up')
        sched.add_job(end_post_market_clean_up, CronTrigger.from_crontab('0 21 * * MON-FRI', timezone="US/Eastern"), id='end_post_market_clean_up')

        sched.configure()
        sched.start()
        scheduler_started = True

        app = web.Application()
        web.run_app(app, port=5001)

    except Exception as ex:
        print ('levitrade.v2.scnner: __main__() failed', ex, datetime.datetime.now(tz=eastern_time_zone).strftime("%Y-%m-%d %H:%M:%S"))