#/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import hashlib
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

MYSQL_CREATE_TABLE = (
    'CREATE TABLE `kafka`.`general` ('
    '  `sha256` CHAR(64) NOT NULL,'
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
    '  PRIMARY KEY (`sha256`))'
    'ENGINE = InnoDB'
    'DEFAULT CHARACTER SET = utf8'
)

##############################################################################
def consume(cid, broker, topic, partition,
            timeout=1.0, mysql_host=None, from_start=False, 
            auto_subscribe=True):
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
        partition (int): The partition number of specified topic.
        timeout (float): Number of seconds to block during message iteration 
            before close (default 1.0).
        mysql_host (str): Host name or IP address of MySQL server. If this is 
            specified, all messages will be saved in it (default None).
        from_start (bool): Receive message from the beginning (default False)
    """
    if mysql_host:
        mysql_port = 3306
        if ':' in mysql_host:
            mysql_host, mysql_port = mysql_host.split(':')
            mysql_port = int(mysql_port)

        # to-do: What if MySQL server doesn't exist?
        cnx = mysql.connector.connect(
            user='root', 
            password='0000', 
            host=mysql_host, 
            port=mysql_port, 
            database='kafka'
        )
        logger.info(f'Connection to MySQL server {mysql_host}:{mysql_port} '
                     'is established.')

    # Convert to milliseconds.
    timeout = int(timeout * 1000)

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            client_id=cid,
            group_id='nexgus-consumer-group',
            consumer_timeout_ms=30000, # 30 seconds
        )
    except kafka.errors.NoBrokersAvailable:
        logger.warning(f'Cannot establish connection to {broker}. '
                        f'Producer {cid} is not created.')
        return
    except Exception as ex:
        print(f'----------> {cid}')
        raise

    # Subscribe to the topic
    # We don't use dynamically partition assignment (subscribe()) since we 
    # have to use seed_to_beginning().
    tp_partition = TopicPartition(topic, partition)
    consumer.assign([tp_partition])
    logger.info(f'{cid}: Subscribes to "{topic}" @ partition {partition}.')

    # Seek to beginning
    if from_start:
        consumer.seek_to_beginning(tp_partition)
        logger.info(f'{cid}: Seek to beginning.')

    index = 0
    for record in consumer:
        # A record is an instance of <kafka.consumer.fetcher.ConsumerRecord>
        timestamp = datetime.fromtimestamp(record.timestamp / 1000).strftime(
            '%Y/%m/%d %H:%M:%S.%f'
        )
        logger.info(f'{cid}: [{timestamp}] "{record.topic}" '
                    f'RX #{index} {record.serialized_value_size}')
        if mysql_host:
            save_to_mysql(cnx, record)
            logger.debug(f'{cid}: Save message to MySQL '
                         f'({mysql_host}:{mysql_port}).')
        index += 1
        time.sleep(0.000001)
        if consumer.config['consumer_timeout_ms'] != timeout:
            consumer.config['consumer_timeout_ms'] = timeout
            logger.debug(f'{cid}: Set consumer.timeout.ms to {timeout}.')

    consumer.close()
    logger.info(f'{cid}: Close.')
    if mysql_host:
        cnx.close()
        logger.info(f'{cid}: Close MySQL ({mysql_host}:{mysql_port})')

##############################################################################
def save_to_mysql(cnx, record):
    """Save a record to specified MySQL server.

    Args:
        cnx (mysql.connector.connection_cext.CMySQLConnection): A connector 
            is connected to the MySQL server.
        record (kafka.consumer.fetcher.ConsumerRecord): An object to 
            represent received Kafka message.
    """
    key = '-'.join([
        f'{record.topic}',
        f'{record.partition}',
        datetime.fromtimestamp(
            record.timestamp/1000
        ).strftime('%Y/%m/%d %H:%M:%S.%f'),
    ])
    sha256 = hashlib.sha256(key.encode('utf-8')).hexdigest()
    if record.value:
        payload = base64.b64encode(record.value).decode('utf-8')
    else:
        payload = ''
    q = ( "INSERT INTO kafka.general ("
            "sha256, topic, partition, offset, timestamp, timestamp_type, "
            "value, serialized_key_size, serialized_value_size, "
            "serialized_header_size) "
          "VALUES ("
            f"'{sha256}', '{record.topic}', '{record.partition}', "
            f"'{record.offset}', '{record.timestamp}', "
            f"'{record.timestamp_type}', '{payload}', "
            f"'{record.serialized_key_size}', "
            f"'{record.serialized_value_size}', "
            f"'{record.serialized_header_size}')"
    )
    cursor = cnx.cursor()
    try:
        cursor.execute(q)
    except mysql.connector.errors.ProgrammingError:
        logger.error(q)
        raise
    except mysql.connector.errors.IntegrityError as ex:
        # The primary key exists. Maybe it is a duplication. Skip it.
        pass
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
        if ':' in args.mysql:
            mysql_host, mysql_port = args.mysql.split(':')
            mysql_port = int(mysql_port)
        else:
            mysql_host, mysql_port = args.mysql, 3306

        try:
            cnx = mysql.connector.connect(
                user='root', 
                password='0000', 
                host=mysql_host,
                port=mysql_port,
                database='kafka',
            )
        except mysql.connector.errors.ProgrammingError:
            logger.warning('Database does not exist. Create database.')
            cnx = mysql.connector.connect(
                user='root', 
                password='0000', 
                host=mysql_host,
                port=mysql_port,
            )
            cnx.cmd_query('CREATE DATABASE kafka')
            cnx.cmd_query('USE kafka')
            cnx.cmd_query(MYSQL_CREATE_TABLE)
        else:
            if args.delete:
                logger.warning('Erase existed database.')
                cnx.cmd_query('DROP DATABASE kafka')
                cnx.cmd_query('CREATE DATABASE kafka')
                cnx.cmd_query('USE kafka')
                cnx.cmd_query(MYSQL_CREATE_TABLE)
        finally:
            cnx.close()

    partitions = topic_partitions(args.topic, args.broker)
    if partitions is None:
        pass

    processes = []
    for idx in range(args.consumers):
        logger.debug(f'--- len(partitions)={len(partitions)}')
        partition = None if len(partitions)==0 else partitions.pop()
        logger.debug(f'+++ len(partitions)={len(partitions)}')
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
        processes.append(p)
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
        help='A topic to subscribe to.')
    parser.add_argument('-c', '--consumers',
        type=int, default=1,
        help='Number of consumers.')
    parser.add_argument('--timeout',
        type=float, default=1.0,
        help='Number of seconds to stop after nothing received.')
    parser.add_argument('-fs', '--from-start',
        action='store_true',
        help='Receive from the start.')
    parser.add_argument('--mysql',
        type=str,
        help='MySQL server IP host[:port]. Default port is 3306.')
    parser.add_argument('-del', '--delete',
        action='store_true',
        help='Delete existed database anyway.')
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
