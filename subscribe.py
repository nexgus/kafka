#/usr/bin/env python3
# -*- coding: utf-8 -*-
import kafka.errors
import logging
import mysql.connector
import time

from datetime import datetime
from kafka import KafkaConsumer
from kafka import TopicPartition
from multiprocessing import Process

##############################################################################
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
def consume(cid, broker, topic, partition=None,
            timeout=None, mysql_host=None, from_start=False):
    """ Create a consumer and consumes specified topics.

    Args:
        cid (str): A name for this client. This string is passed in each 
            request to servers and can be used to identify specific 
            server-side log entries that correspond to this client. Also 
            submitted to GroupCoordinator for logging with respect to 
            consumer group administration.
        broker (str): The 'host[:port]' string that the producer should 
            contact to bootstrap initial cluster metadata.
        topic (str): Topic.
        partition (int): The partition number of specified topic (default 
            None).
        timeout (float): Number of seconds to block during message iteration 
            before close (default None).
        mysql_host (str): Host name or IP address of MySQL server. If this is 
            specified, all messages will be saved in it (default None).
        from_start (bool): Receive message from the beginning (default False)
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

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            client_id=cid,
            group_id='nexgus-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=timeout,
        )
    except kafka.errors.NoBrokersAvailable:
        logger.warning(f'Cannot establish connection to {broker}. '
                        f'Producer {cid} is not created.')
        return
    except Exception as ex:
        print(f'----------> {cid}')
        raise

    # Subscribe
    if isinstance(partition, int):
        tp_partition = TopicPartition(topic, partition)
        consumer.assign([tp_partition])
    else:
        tp_partition = None
        consumer.subscribe([topic])
        partition = consumer.assignment().pop()
    logger.info(f'{cid}: subscribes to "{topic}" @ partition {partition}.')

    # Seek to beginning
    if from_start and tp_partition:
        consumer.seek_to_beginning()
        logger.info(f'{cid}: Seek to beginning.')

    index = 0
    for record in consumer:
        # A record is an instance of <kafka.consumer.fetcher.ConsumerRecord>
        time_str = datetime.fromtimestamp(record.timestamp / 1000).strftime(
            '%Y/%m/%d %H:%M:%S.%f'
        )
        logger.info(f'{cid}: [{time_str}] {record.topic} '
                    f'RX #{index} {record.serialized_value_size}')
        if mysql_host:
            save_to_mysql(cnx, message)
            logger.info(f'{cid}: Save message to MySQL ({mysql_host}).')
        index += 1

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
def topic_partitions(topic, broker):
    """Get partitions of a topic.

    Args:
        topic (str): Topic.
        broker (str): The 'host[:port]' string that the producer should 
            contact to bootstrap initial cluster metadata.

    Returns:
        A None is returned if there is not specified topic.
        If it exists, return a set which incluing all partion numbers.
    """
    try:
        consumer = KafkaConsumer(bootstrap_servers=[broker])
    except kafka.errors.NoBrokersAvailable:
        logger.warning(f'Cannot establish connection to {broker}.')
        return
    except Exception as ex:
        print('----------> topic_partitions()')
        raise

    topics = consumer.topics()
    if topic not in topics:
        partitions = None
    else:
        partitions = consumer.partitions_for_topic(topic)
    consumer.close()

    return partitions

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

    partitions = topic_partitions(args.topic, args.broker)
    if partitions is None:
        pass

    processes = []
    for idx in range(args.consumers):
        partition = None
        if len(partitions) > 0:
            partition = partitions.pop()

        p = Process(
            target=consume, 
            args=(
                f'nexgus-consumer-{idx+1}', # cid
                args.broker, # broker
                args.topic, # topics
                partition, # partition
                args.timeout, # timeout
                args.mysql, # mysql_host,
                args.from_start, # from_start
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
    parser.add_argument('broker',
        type=str,
        help='Broker HOST[:PORT]. Default PORT is 9092.')
    parser.add_argument('topic',
        type=str,
        help='Topics.')
    parser.add_argument('-c', '--consumers',
        type=int, default=1,
        help='Number of consumers.')
    parser.add_argument('--timeout',
        type=float, default=10.0,
        help='Number of seconds to stop after nothing received.')
    parser.add_argument('-fs', '--from-start',
        action='store_true',
        help='Receive from the start.')
    parser.add_argument('--mysql',
        type=str,
        help='MySQL server IP address.')
    parser.add_argument('-l', '--level',
        type=str, choices=['debug', 'info', 'warning'], default='info',
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
