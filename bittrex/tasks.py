import random
import time
import requests
import datetime

from celery.signals import task_postrun
from celery.utils.log import get_task_logger
from celery.task.base import periodic_task
from celery.schedules import crontab
from sqlalchemy.exc import DatabaseError

from connections import MySqlConnection, RedisConnection
from bittrex import celery, db
from bittrex.models import oneMinTick, fiveMinTick

logger = get_task_logger(__name__)


mysqldb = MySqlConnection(host='localhost', port=3306, database='db', user='root', password='123')
redis_conn = RedisConnection(host="localhost", port=6379, database=0)


def fetch_markets():
    res = requests.get('https://bittrex.com/api/v1.1/public/getmarkets').json()
    markets = []
    if res['success'] == True:
        for _, val in enumerate(res['result']):
            markets.append(val['MarketName'])
    return markets


def store_tick_data(market):
    res = requests.get('https://bittrex.com/Api/v2.0/pub/market/GetLatestTick?marketName={0}&tickInterval=onemin&_={1}'.format(market, int(time.time()))).json()
    if res['success'] == True:
        _data = res['result'][0]
        _datetime= _data['T']
        pattern = '%Y-%m-%dT%H:%M:%S'
        timestamp = int(time.mktime(time.strptime(_datetime, pattern)))         
        one_tick = oneMinTick(Timestamp=timestamp, Market=market, Open=_data['O'],
                                Close=_data['C'], High=_data['H'], Low=_data['L'],
                                Volume=_data['V'], BaseVolume=_data['BV'])                  
        try:
            db.session.add(one_tick)
            db.session.commit()
        except DatabaseError:
            db.session.rollback()


def get_timestamp_offset(market, interval, mysqltable):
    key = "market-{0}-{1}".format(market, interval)
    try:
        offset = int(redis_conn.get_val(key))
    except:
        offset = int(redis_conn.set_val(key, 0))
    return offset

def update_timestamp_in_redis(market, interval, updated_timestamp):
    key = "market-{0}-{1}".format(market, interval)
    try:
        redis_conn.set_val(key, updated_timestamp)
    except Exception as exp:
        print "Exception {0}".format(exp)
    return 


def insert_hour_records(record):
    timestamp = time.mktime(datetime.datetime.strptime(record['ts_CEILING'], "%Y-%m-%d %H:%M:%S").timetuple())
    open_query = """Select Open From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Open = mysqldb.extract(query=open_query)[0]['Open']
    close_query = """Select Close From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Close = mysqldb.extract(query=close_query)[0]['Close']
    try:
        query = """
        INSERT INTO oneHourTick (Timestamp, Market, Volume, High, Low, Open, Close)
        VALUES ({0}, '{1}', {2}, {3}, {4}, {5}, {6})
        """.format(int(timestamp), str(record['Market']), record['SUM(Volume)'], record['MAX(High)'], record['MIN(Low)'], Open, Close)
        x = mysqldb.execute(query)
    except Exception as exp:
        print "exception {0}".format(exp)


def populate_one_hour_data(market):
    table = 'oneHourTick'
    try:
        offset_time = get_timestamp_offset(market, 'oneHour', table)
    except TypeError:
        offset_time = 0
    final_time_query = """Select MAX(Timestamp) AS final_time FROM oneMinTick Where Market='{0}'""".format(market)
    try:
        final_timestamp = int(mysqldb.extract(query=final_time_query)[0]['final_time'])
    except TypeError:
        final_timestamp = int(time.time())
    query = """
    SELECT
	    SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19) AS ts_CEILING,
        Market, SUM(Volume), MAX(High), MIN(Low), Count(*), MIN(Timestamp), MAX(Timestamp)
    FROM oneMinTick
    WHERE Market='{0}' AND Timestamp>{1} AND Timestamp<{2}
    GROUP BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19), Market
    ORDER BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19);""".format(market, int(offset_time), final_timestamp)
    records = mysqldb.extract(query=query)
    map(insert_hour_records, records)
    update_timestamp_in_redis(market, "oneHour", final_timestamp)


def insert_half_records(record):
    timestamp = time.mktime(datetime.datetime.strptime(record['ts_CEILING'], "%Y-%m-%d %H:%M:%S").timetuple())
    open_query = """Select Open From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Open = mysqldb.extract(query=open_query)[0]['Open']
    close_query = """Select Close From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Close = mysqldb.extract(query=close_query)[0]['Close']
    try:
        query = """
        INSERT INTO halfHourTick (Timestamp, Market, Volume, High, Low, Open, Close)
        VALUES ({0}, '{1}', {2}, {3}, {4}, {5}, {6})
        """.format(int(timestamp), str(record['Market']), record['SUM(Volume)'], record['MAX(High)'], record['MIN(Low)'], Open, Close)
        x = mysqldb.execute(query)
    except Exception as exp:
        print "exception {0}".format(exp)


def populate_half_hour_data(market):
    table = 'halfHourTick'
    try:
        offset_time = get_timestamp_offset(market, 'halfHour', table)
    except TypeError:
        offset_time = 0
    final_time_query = """Select MAX(Timestamp) AS final_time FROM oneMinTick Where Market='{0}'""".format(market)
    try:
        final_timestamp = int(mysqldb.extract(query=final_time_query)[0]['final_time'])
    except TypeError:
        final_timestamp = int(time.time())
    query = """
    SELECT
	    SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19) AS ts_CEILING,
        Market, SUM(Volume), MAX(High), MIN(Low), Count(*), MIN(Timestamp), MAX(Timestamp)
    FROM oneMinTick
    WHERE Market='{0}' AND Timestamp>{1} AND Timestamp<{2}
    GROUP BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19), Market
    ORDER BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/1800)*1800, '%Y-%m-%d %H:%i:%S'), 1, 19);""".format(market, int(offset_time), final_timestamp)
    records = mysqldb.extract(query=query)
    map(insert_half_records, records)
    update_timestamp_in_redis(market, "halfHour", final_timestamp)


def insert_quater_records(record):
    timestamp = time.mktime(datetime.datetime.strptime(record['ts_CEILING'], "%Y-%m-%d %H:%M:%S").timetuple())
    open_query = """Select Open From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Open = mysqldb.extract(query=open_query)[0]['Open']
    close_query = """Select Close From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Close = mysqldb.extract(query=close_query)[0]['Close']
    try:
        query = """
        INSERT INTO quaterHourTick (Timestamp, Market, Volume, High, Low, Open, Close)
        VALUES ({0}, '{1}', {2}, {3}, {4}, {5}, {6})
        """.format(int(timestamp), str(record['Market']), record['SUM(Volume)'], record['MAX(High)'], record['MIN(Low)'], Open, Close)
        x = mysqldb.execute(query)
    except Exception as exp:
        print "exception {0}".format(exp)

def populate_quater_hour_data(market):
    table = 'quaterHourTick'
    try:
        offset_time = get_timestamp_offset(market, 'quaterHour', table)
    except TypeError:
        offset_time = 0
    final_time_query = """Select MAX(Timestamp) AS final_time FROM oneMinTick Where Market='{0}'""".format(market)
    try:
        final_timestamp = int(mysqldb.extract(query=final_time_query)[0]['final_time'])
    except TypeError:
        final_timestamp = int(time.time())
    query = """
    SELECT
	    SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/900)*900, '%Y-%m-%d %H:%i:%S'), 1, 19) AS ts_CEILING,
        Market, SUM(Volume), MAX(High), MIN(Low), Count(*), MIN(Timestamp), MAX(Timestamp)
    FROM oneMinTick
    WHERE Market='{0}' AND Timestamp>{1} AND Timestamp<{2}
    GROUP BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/900)*900, '%Y-%m-%d %H:%i:%S'), 1, 19), Market
    ORDER BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/900)*900, '%Y-%m-%d %H:%i:%S'), 1, 19);""".format(market, int(offset_time), final_timestamp)
    records = mysqldb.extract(query=query)
    map(insert_quater_records, records)
    update_timestamp_in_redis(market, "quaterHour", final_timestamp)

def insert_five_min_records(record):
    timestamp = time.mktime(datetime.datetime.strptime(record['ts_CEILING'], "%Y-%m-%d %H:%M:%S").timetuple())
    open_query = """Select Open From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    Open = mysqldb.extract(query=open_query)[0]['Open']
    close_query = """Select Close From oneMinTick Where Timestamp={0} AND Market='{1}'""".format(record['MIN(Timestamp)']
                    , record['Market'])
    # import pdb; pdb.set_trace()
    Close = mysqldb.extract(query=close_query)[0]['Close']
    try:
        query = """
        INSERT INTO fiveMinTick (Timestamp, Market, Volume, High, Low, Open, Close)
        VALUES ({0}, '{1}', {2}, {3}, {4}, {5}, {6})
        """.format(int(timestamp), str(record['Market']),
            record['SUM(Volume)'], record['MAX(High)'], record['MIN(Low)'], Open, Close)
        x = mysqldb.execute(query)
    except Exception as exp:
        print "exception {0}".format(exp)


def populate_five_min_data(market):
    table = 'fiveMinTick'
    try:
        offset_time = int(get_timestamp_offset(market, 'fiveMin', table))
    except TypeError:
        offset_time = 0
    final_time_query = """Select MAX(Timestamp) AS final_time FROM oneMinTick Where Market='{0}'""".format(market)
    try:
        final_timestamp = int(mysqldb.extract(query=final_time_query)[0]['final_time'])
    except TypeError:
        final_timestamp = int(time.time())
    query = """
    SELECT
	    SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/300)*300, '%Y-%m-%d %H:%i:%S'), 1, 19) AS ts_CEILING,
        Market, SUM(Volume), MAX(High), MIN(Low), Count(*), MIN(Timestamp), MAX(Timestamp)
    FROM oneMinTick
    WHERE Market='{0}' AND Timestamp>{1} AND Timestamp<{2}
    GROUP BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/300)*300, '%Y-%m-%d %H:%i:%S'), 1, 19), Market
    ORDER BY SUBSTRING(FROM_UNIXTIME(CEILING(Timestamp/300)*300, '%Y-%m-%d %H:%i:%S'), 1, 19);""".format(market, offset_time, final_timestamp)
    records = mysqldb.extract(query=query)
    map(insert_five_min_records, records)
    update_timestamp_in_redis(market, "fiveMin", final_timestamp)


@celery.task(bind=True)
def tick_task(*args):
    """
    Background task that fetches data from 
    bittrex API and stores it in database 
    """
    markets = fetch_markets()
    map(store_tick_data, markets)
    return 


@periodic_task(run_every=crontab(minute="*/5"))
def five_min_ticker(*args):
    """
    Background task that aggregrates data from 
    oneMinTick table in 5 min interval
    """
    markets = fetch_markets()
    map(populate_five_min_data, markets)
    return


@periodic_task(run_every=crontab(minute="*/15"))
def quater_hour_ticker(*args):
    """
    Background task that aggregrates data from 
    oneMinTick table in 15 min interval
    """
    markets = fetch_markets()
    map(populate_quater_hour_data, markets)
    return


@periodic_task(run_every=crontab(minute="*/30"))
def half_hour_ticker(*args):
    """
    Background task that aggregrates data from 
    oneMinTick table in 30 min interval
    """
    markets = fetch_markets()
    map(populate_half_hour_data, markets)
    return


@periodic_task(run_every=crontab(minute="*/60"))
def one_hour_ticker(*args):
    """
    Background task that aggregrates data from 
    oneMinTick table in 60 min interval
    """
    markets = fetch_markets()
    map(populate_one_hour_data, markets)
    return
