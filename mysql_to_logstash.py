#!/usr/bin/env python
# -*- coding: utf-8 -*-

# requirements: pip install python-json-logger
# Output logstash events to the console from MySQL replication stream
#
# You can pipe it to logstash like this:
# python examples/logstash/mysql_to_logstash.py | java -jar logstash-1.1.13-flatjar.jar  agent -f examples/logstash/logstash-simple.conf

import re
import os
import datetime
import json
import sys
import time
import redis
import thread
from kafka import KafkaProducer 
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT
from pymysqlreplication.event import (RotateEvent)
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime.datetime) else None)

MYSQL_SETTINGS = {
    "host": os.environ['MYSQL_HOST'],
    "port": int(os.environ['MYSQL_PORT']),
    "user": os.environ['MYSQL_USER'],
    "passwd": os.environ['MYSQL_PASS']
}

EVENT_LAST_SEEN = None
LOG_POS = None
LOG_FILE = None

def str2bool(v):
    try:
        return v.lower() in ("yes", "true", "t", "1")
    except:
        return False

# ONLY_SCHEMAS = ["stock24h_masterdb","MasterDB24H","hoangta"]
ONLY_SCHEMAS = os.environ.get('ONLY_SCHEMAS', '').replace(' ', '').split(',')
# IGNORED_TABLES = ["elasticsearch_reindex_queue","migrations","schema_migrations","tags_regenerator_queue"]
IGNORED_TABLES = os.environ.get('IGNORED_TABLES', '').replace(' ', '').split(',')
# ARRAY_FIELDS = ["deleted_tags", "com_tags", "stock_tags","tags"]
ARRAY_FIELDS = os.environ.get('ARRAY_FIELDS', '').replace(' ', '').split(',')

OUTPUT_TYPE = os.environ.get('OUTPUT_TYPE', 'file')
OUTPUT_PATH = os.environ.get('OUTPUT_PATH', '')
OUTPUT_HOST = os.environ.get('OUTPUT_HOST', '')
OUTPUT_PORT = os.environ.get('OUTPUT_PORT', '')

DUMP_JSON = str2bool(os.environ.get('DUMP_JSON', 'False'))
DUMP_KAFKA = str2bool(os.environ.get('DUMP_KAFKA','False'))
DUMP_REDIS = str2bool(os.environ.get('DUMP_REDIS', 'False'))

KAFKA_PRODUCER = None
REDIS_PRODUCER = None

def tracking(trackingInterval):
    while True:
        time.sleep(trackingInterval)
        if EVENT_LAST_SEEN:
            with open('tracking.time', 'w') as f:
                f.write(str(EVENT_LAST_SEEN) + ' ' + str(LOG_POS) + ' ' + str(LOG_FILE))

def connectRedis():
    global REDIS_PRODUCER
    try:
        REDIS_PRODUCER = redis.Redis(host=OUTPUT_HOST, port=OUTPUT_PORT)
        REDIS_PRODUCER.ping()
    except Exception as e:
        print (e)

def connectKafka():
    global KAFKA_PRODUCER
    try:
        KAFKA_PRODUCER = KafkaProducer(bootstrap_servers=(OUTPUT_HOST+':'+OUTPUT_PORT))
    except Exception as e:
        print (e)

def dumpJson(schema, jsonstr):
    filename = schema + "__data.json"
    with open(os.path.join(OUTPUT_PATH, filename), 'a') as f:
        f.write(jsonstr + os.linesep)

def dumpKafka(schema, jsonstr):
    if KAFKA_PRODUCER is None:
        return True
    KAFKA_PRODUCER.send('queue_'+schema, jsonstr)


def dumpRedis(schema, jsonstr):
    if REDIS_PRODUCER is None:
        return True
    REDIS_PRODUCER.rpush('queue_'+schema, jsonstr)

def connect():
    global EVENT_LAST_SEEN
    global LOG_FILE
    global LOG_POS
    try:
        with open('tracking.time', 'r') as r:
            EVENT_LAST_SEEN, LOG_POS, LOG_FILE = r.readline().split()
            if LOG_POS: 
                LOG_POS = int(LOG_POS)
            if EVENT_LAST_SEEN: 
                EVENT_LAST_SEEN = int(EVENT_LAST_SEEN)
    except Exception as e:
        print (e)
    print ("start stream with EVENT_LAST_SEEN=" + str(EVENT_LAST_SEEN))
    sys.stdout.flush()
    try:
        stream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            slave_heartbeat=20,
            resume_stream=(EVENT_LAST_SEEN is not None),
            log_file=LOG_FILE,
            log_pos=LOG_POS,
            blocking=True,
            server_id=3,
            skip_to_timestamp=EVENT_LAST_SEEN,
            only_schemas=ONLY_SCHEMAS,
            ignored_tables=IGNORED_TABLES,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RotateEvent])
        # stream._BinLogStreamReader__connect_to_stream()
        # print stream.__dict__.get("_stream_connection")

        for binlogevent in stream:
            EVENT_LAST_SEEN = binlogevent.timestamp
            if binlogevent.event_type == ROTATE_EVENT:
                LOG_POS = binlogevent.position
                LOG_FILE = binlogevent.next_binlog
                continue
            elif stream.log_pos:
                LOG_POS = stream.log_pos
            for row in binlogevent.rows:
                
                event = {"schema": binlogevent.schema, "table": binlogevent.schema.lower()+"__"+binlogevent.table}

                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event = dict(event.items() + row["values"].items())
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event = dict(event.items() + row["after_values"].items())
                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "index"
                    event = dict(event.items() + row["values"].items())

                for field in ARRAY_FIELDS:
                    if event is not None and event.get(field, '___') is not None:
                        if not event.get(field, '___').startswith('___'):
                            tags = event[field].split(",")
                            arrTags = []
                            for tag in tags:
                                strTag = tag.strip()
                                if len(strTag) > 0:
                                    arrTags.append(strTag);
                            event[field] = arrTags

                jsonstr = json.dumps(event, ensure_ascii=False, encoding="utf-8").encode('utf-8')
                if DUMP_JSON:
                    dumpJson(binlogevent.schema.lower(), jsonstr)
                if DUMP_KAFKA:
                    dumpKafka(binlogevent.schema.lower(), jsonstr)
                if DUMP_REDIS:
                    dumpRedis(binlogevent.schema.lower(), jsonstr)
                # print (jsonstr + os.linesep)
                # sys.stdout.flush()
        stream.close()
        print ("close stream")        
    except Exception as e:
        print (e)
    sys.stdout.flush()
    return True;

def main():
    global DUMP_REDIS
    global DUMP_KAFKA
    global DUMP_JSON
    if OUTPUT_TYPE == 'file':
        DUMP_JSON = True
        if OUTPUT_PATH and not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
    elif OUTPUT_TYPE == 'redis':
        DUMP_REDIS = True
    elif OUTPUT_TYPE == 'kafka':
        DUMP_KAFKA = True
    if DUMP_KAFKA:
        connectKafka()
    if DUMP_REDIS:
        connectRedis()
    try:
        thread.start_new_thread( tracking, (30, ) )
    except Exception as e:
        print (e)
        sys.stdout.flush()
    while connect():
        time.sleep(5);

if __name__ == "__main__":
    main()
