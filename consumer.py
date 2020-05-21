#/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import datetime
import json
import mysql.connector
import os

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

####################################################################################################
VER_MAJOR = 0
VER_MINOR = 6
VER_PATCH = 0

####################################################################################################
def save_to_csv(filepath, record):
    payload = b'' if record.value is None else base64.b64encode(record.value)
    with open(filepath, 'a') as fp:
        fp.write(f'{record.topic},{record.partition},{record.offset},{record.timestamp},'
                 f'{record.timestamp_type},{record.key},{payload},{record.headers},'
                 f'{record.headers},{record.checksum},{record.serialized_key_size},'
                 f'{record.serialized_value_size},{record.serialized_header_size}\n')

####################################################################################################
def save_to_mysql(cnx, record):
    id = datetime.datetime.now().strftime('%Y%m%d%H%M%S.%f')
    payload = '' if record.value is None else base64.b64encode(record.value).decode('utf-8')
    q = ( "INSERT INTO kafka.general ("
          "id, topic, partition, offset, timestamp, timestamp_type, value, "
          "serialized_key_size, serialized_value_size, serialized_header_size) "
         f"VALUES ('{id}', '{record.topic}', '{record.partition}', '{record.offset}', "
         f"'{record.timestamp}', '{record.timestamp_type}', '{payload}', "
         f"'{record.serialized_key_size}', {record.serialized_value_size}, "
         f"'{record.serialized_header_size}')")
    cursor = cnx.cursor()
    try:
        cursor.execute(q)
    except mysql.connector.errors.ProgrammingError:
        print(q)
        raise
    except Exception as ex:
        raise
    cnx.commit()
    cursor.close()

####################################################################################################
def show_record(record, save_type=None, **kwargs):
    """Display and save record to specified file/database.

    Args:
        record: A ConsumeRecord from a Kafka cluster.
        save_type: 'mysql' or 'csv' (defaul None)
        kwargs:
            cnx: A CMySQLConnection instance when save_type is 'mysql'.
            filepath: Path to CSV file when save_type is 'csv'.
    """
    if save_type == 'mysql':
        assert 'cnx' in kwargs
        save_to_mysql(kwargs['cnx'], record)
    elif save_type == 'csv':
        assert 'filepath' in kwargs
        save_to_mysql(kwargs['filepath'], record)

    payload = '' if record.value is None else record.value.decode('utf-8')
    print('-'*30)
    print(f'Topic: {record.topic}')
    print(f'Message: "{payload}"')

####################################################################################################
def main(args):
    save_type = None

    if args.csv:
        save_type='csv'
        args.csv = os.path.expanduser(args.csv)
        dirname = os.path.dirname(args.csv)
        os.makedirs(dirname, exist_ok=True)
        with open(args.csv, 'w') as fp:
            fp.write('topic,partition,offset,timestamp,timestamp_type,key,value,headers,checksum,'
                     'serialized_key_size,serialized_value_size,serialized_header_size\n')

    cnx = None
    if args.mysql:
        save_type='mysql'
        if args.delete:
            cnx = mysql.connector.connect(user='root', password='0000', host=args.mysql)
            cnx.cmd_query('DROP DATABASE IF EXISTS kafka')
            cnx.close()

        try:
            cnx = mysql.connector.connect(user='root', password='0000', host=args.mysql, database='kafka')
        except mysql.connector.errors.ProgrammingError:
            cnx = mysql.connector.connect(user='root', password='0000', host=args.mysql)
            cnx.cmd_query('CREATE DATABASE kafka')
            cnx.cmd_query('USE kafka')
            cnx.cmd_query('CREATE TABLE `kafka`.`general` ('
                          '  `id` CHAR(21) NOT NULL,'
                          '  `topic` TEXT NOT NULL,'
                          '  `partition` DECIMAL UNSIGNED NOT NULL,'
                          '  `offset` DECIMAL UNSIGNED NOT NULL,'
                          '  `timestamp` BIGINT UNSIGNED NOT NULL,'
                          '  `timestamp_type` DECIMAL UNSIGNED NULL,'
                          '  `key` TEXT NULL,'
                          '  `value` TEXT NOT NULL,'
                          '  `header` TEXT NULL,'
                          '  `checksum` TEXT NULL,'
                          '  `serialized_key_size` DECIMAL NOT NULL,'
                          '  `serialized_value_size` DECIMAL NOT NULL,'
                          '  `serialized_header_size` DECIMAL NOT NULL,'
                          '  PRIMARY KEY (`id`))'
                          'ENGINE = InnoDB'
                          'DEFAULT CHARACTER SET = utf8')

    consumer = KafkaConsumer(bootstrap_servers=args.brokers, auto_offset_reset=args.policy)
    consumer.subscribe(args.topics)
    print(f'Subscribe to {consumer.subscription()}')

    if args.policy=='earliest':
        for msg in consumer:
            show_record(msg, save_type=save_type, cnx=cnx, filepath=args.csv)

    messages = 0
    for msg in consumer:
        show_record(msg, save_type=save_type, cnx=cnx, filepath=args.csv)
        if args.count <= 0: continue
        messages += 1
        if messages >= args.count:
            break

    if args.metrics:
        print('='*30)
        print(json.dumps(consumer.metrics(), indent=2))

    consumer.close()
    if args.mysql: cnx.close()

####################################################################################################
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Tool for Kafka consumer.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('-V', '--version', action='version', version=f'{VER_MAJOR}.{VER_MINOR}.'
                                                                     f'{VER_PATCH}')
    parser.add_argument('-b', '--brokers', type=str, nargs='+', required=True,
        help="A list of brokers' host[:port] that the consumer should contact to bootstrap initial "
             'cluster metadata. This does not have to be the full node list. It just needs to have '
             'at least one broker that will respond to a Metadata API Request. Default port is '
             '9092.')
    parser.add_argument('-t', '--topics', type=str, nargs='+', required=True,
        help='A list of topics to subscribe to.')
    parser.add_argument('--cid', type=str, default='nexcom-kafka-consumer-demo',
        help='Customer ID. A name for this client. This string is passed in each request to '
             'servers and can be used to identify specific server-side log entries that correspond '
             'to this client. Also submitted to GroupCoordinator for logging with respect to '
             'consumer group administration.')
    parser.add_argument('--gid', type=str,
        help='Group ID. The name of the consumer group to join for dynamic partition assignment '
             '(if enabled), and to use for fetching and committing offsets. If None, '
             'auto-partition assignment (via group coordinator) and offset commits are disabled.')
    parser.add_argument('-c', '--count', type=int, default=1, 
        help='How many message(s) should be received. A zero value to receive forever until press '
             'Ctrl-C.')
    parser.add_argument('--policy', type=str, choices=['earliest', 'latest'], default='earliest',
        help='A policy for resetting offsets on OffsetOutOfRange errors: "earliest" will move to '
             'the oldest available message, "latest" will move to the most recent. Any other value '
             'will raise the exception.')
    parser.add_argument('-m', '--metrics', action='store_true', 
        help='Print consumer metrics in JSON format.')
    parser.add_argument('--csv', type=str,
        help='Path to csv file.')
    parser.add_argument('--mysql', type=str, 
        help='MySQL server IP address.')
    parser.add_argument('--delete', action='store_true',
        help='Erase MySQL database at the beginning.')

    args = parser.parse_args()
    #import sys; print(args); sys.exit(0)

    main(args)
