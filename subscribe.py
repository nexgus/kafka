#/usr/bin/env python3
# -*- coding: utf-8 -*-
import kafka.errors
import logging
import mysql.connector
import time

from datetime import datetime
from kafka import KafkaConsumer
from multiprocessing import Process

####################################################################################################
VER_MAJOR = 0
VER_MINOR = 1
VER_PATCH = 0

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

logger = logging.getLogger('nexgus-publish')
logger.setLevel(logging.INFO)

##############################################################################
def consume(cid, broker, topics, timeout=None, mysql_host=None):
    """ Create a consumer and consumes specified topics.

    Args:
        cid (str): A name for this client. This string is passed in each 
            request to servers and can be used to identify specific 
            server-side log entries that correspond to this client. Also 
            submitted to GroupCoordinator for logging with respect to 
            consumer group administration.
        broker (str): The 'host[:port]' string that the producer should 
            contact to bootstrap initial cluster metadata.
        topics (list): List of topics for subscription.
        timeout (float): Number of seconds to block during message iteration 
            before close (default None).
        mysql_host (str): Host name or IP address of MySQL server. If this is 
            specified, all messages will be saved in it (default None).
    """
    if mysql_host:
        cnx = mysql.connector.connect(
            user='root', 
            password='0000', 
            host=mysql_host, 
            database='kafka'
        )

    if isinstance(timeout, float):
        # Convert to milliseconds.
        timeout = int(args.timeout * 1000)

    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        client_id=cid,
        group_id='nexgus-consumer-group',
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout,
    )
    consumer.subscribe(topics)
    logger.info(f'{cid}: subscribes to {topics}.')

    # indices stores received current index for each topic
    indices = {}
    for topic in topics:
        indices[topic] = 0

    for record in consumer:
        # A record is an instance of <kafka.consumer.fetcher.ConsumerRecord>
        time_str = datetime.fromtimestamp(record.timestamp / 1000).strftime(
            '%Y/%m/%d %H:%M:%S.%f'
        )
        logger.info(f'{cid}: [{time_str}] {record.topic} '
                    f'RX #{indices[record.topic]} {record.serialized_value_size}')
        if mysql_host:
            save_to_mysql(cnx, message)
            logger.info(f'{cid}: Save message to MySQL ({mysql_host}).')

        indices[record.topic] += 1

    consumer.close()
    logger.info(f'{cid}: Close.')
    if mysql_host:
        cnx.close()
        logger.info(f'{cid}: Close MySql ({mysql_host})')

    time.sleep(0.000001)

##############################################################################
def save_to_mysql(cnx, record, cid=''):
    """Save a record to specified MySQL server.

    Args:
        cnx (mysql.connector.connection_cext.CMySQLConnection): A connector 
            is connected to the MySQL server.
        record (kafka.consumer.fetcher.ConsumerRecord): An object to 
            represent received Kafka message.
        cid (str): A consumer's name. This name is used to be associated with 
            id to prevent duplicated index since each consumer is in an 
            independant process.
    """
    pkey = hash(cid + datetime.datetime.now().strftime('%Y%m%d%H%M%S.%f'))
    payload = ''
    if record.value: pkey = base64.b64encode(record.value).decode('utf-8')
    q = ( "INSERT INTO kafka.general (pkey, topic, partition, offset, "
          "timestamp, timestamp_type, value, serialized_key_size, "
          "serialized_value_size, serialized_header_size) VALUES ('{pkey}', "
         f"'{record.topic}', '{record.partition}', '{record.offset}', "
         f"'{record.timestamp}', '{record.timestamp_type}', '{payload}', "
         f"'{record.serialized_key_size}', '{record.serialized_value_size}, "
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

##############################################################################
def main(args):
    if args.mysql:
        cnx = mysql.connector.connect(
            user='root', 
            password='0000', 
            host=args.mysql
        )
        cnx.cmd_query('DROP DATABASE IF EXISTS kafka')
        logger.warning('Erase existed MySQL database.')
        cnx.cmd_query('CREATE DATABASE kafka')
        cnx.cmd_query('USE kafka')
        cnx.cmd_query('CREATE TABLE `kafka`.`general` ('
                      '  `pkey` DECIMAL NOT NULL,'
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
        cnx.close()

    processes = []
    for partition in range(args.consumers):
        p = Process(
            target=consume, 
            args=(
                f'nexgus-consumer-{partition+1}', # cid
                args.broker, # broker
                args.topics, # topics
                args.timeout, # timeout
                args.mysql, # mysql_host
            ), 
        )
        p.start()

    for p in processes:
        p.join()

##############################################################################
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Kafka subscribe example.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('-V', '--version', 
        action='version',
        version=f'{VER_MAJOR}.{VER_MINOR}.{VER_PATCH}')
    parser.add_argument('-b', '--broker',
        type=str, required=True,
        help='Broker HOST[:PORT]. Default PORT is 9092.')
    parser.add_argument('-t', '--topics',
        type=str, nargs='+', required=True,
        help='Topics.')
    parser.add_argument('-c', '--consumers',
        type=int, default=1,
        help='Number of consumers.')
    parser.add_argument('--timeout',
        type=float, default=10.0,
        help='Number of seconds to stop after nothing received.')
    parser.add_argument('--mysql',
        type=str,
        help='MySQL server IP address.')
    parser.add_argument('-l', '--level',
        type=str, choices=['debug', 'info', 'warning'], default='warning',
        help='Log level.')
    args = parser.parse_args()

    args.level = LOG_LEVELS[args.level]
    ch = logging.StreamHandler() # https://docs.python.org/3.7/howto/logging.html#useful-handlers
    ch.setLevel(args.level)
    ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(ch)

    main(args)

'''
python subscribe.py -b nexgus1.westus2.cloudapp.azure.com -t this_is_a_topic -c 3
'''
