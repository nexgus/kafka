#/usr/bin/env python3
# -*- coding: utf-8 -*-
import kafka.errors
import logging
import os
import random
import string
import time

from datetime import datetime
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.admin import NewTopic
from multiprocessing import Process

# Since we use KafkaAdminClient, and that is not stable. So we have to fix 
# the version at 2.0.1.

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
logger.setLevel(logging.DEBUG)

##############################################################################
def create_topic(broker, topic, partitions=1, replicas=1):
    """Create a topic if it does not exist in the cluster.

    Args:
        broker (str): The 'host[:port]' string that the producer should 
            contact to bootstrap initial cluster metadata.
        topic (str): Topic where the message will be published.
        partitions (int): Partion count (default 1).
        replicas (int): Replica count (default 1).
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=[args.broker])
    except kafka.errors.NoBrokersAvailable:
        logger.warning(f'Cannot establish connection to {broker} while '
                       f'creating topic "{topic}".')
        return
    except kafka.errors.NodeNotReadyError:
        logger.critical(f'Got a NodeNotReadyError. Please check the broker '
                         'configuration.')
        return
    except Exception as ex:
        print('----------> create_topic()')
        raise

    try:
        admin.create_topics([
            NewTopic(
                name=topic, 
                num_partitions=partitions, 
                replication_factor=replicas,
            ),
        ])
        # create_topics() should be blocking. See 
        # https://github.com/dpkp/kafka-python/blob/2.0.1/kafka/admin/client.py#L373
        # for detail.
    except kafka.errors.TopicAlreadyExistsError:
        logger.warning(f'Cannot create topic {topic} since it exists.')

    admin.close()

##############################################################################
def produce(cid, broker, topic, 
            partition=0, acks=1, iterations=10, size=None, max_size=4096, 
            binary=False):
    """Create a producer and generate required messages.

    Args:
        cid (str): A name for this client. This string is passed in each 
            request to servers and can be used to identify specific 
            server-side log entries that correspond to this client.
        broker (str): The 'host[:port]' string that the producer should 
            contact to bootstrap initial cluster metadata.
        topic (str): Topic where the message will be published.
        partition (int): Specified partition number (default 0).
        acks (int or str): The number of acknowledgments the producer requires 
            the leader to have received before considering a request complete. 
            This controls the durability of records that are sent. The 
            following settings are common:
            0: Producer will not wait for any acknowledgment from the server.
            1: Wait for leader to write the record to its local log only.
            'all': Wait for the full set of in-sync replicas to write the 
                   record.
        iterations (int): How many messages does this prducer will produce (
            default 10).
        size (int): Message length in bytes. A value of None to use random 
            length (default None).
        max_size (int): Maximum message length in bytes for random generated 
            message (default 4096).
        binary (bool): Generate binary data (default False).
    """
    logger.debug(f'Creating {cid}.')
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            client_id=cid,
            acks=acks,
        )
    except kafka.errors.NoBrokersAvailable:
        logger.warning(f'Cannot establish connection to {broker}. '
                        f'Producer {cid} is not created.')
        return
    except Exception as ex:
        print(f'----------> {cid}')
        raise

    random.seed(int(time.time()*1000000))
    source = string.ascii_letters + string.digits + string.punctuation
    for iteration in range(iterations):
        msg_size = random.randint(1, max_size) if size is None else size
        if binary:
            msg = os.urandom(msg_size)
        else:
            msg = ''.join(random.choice(source) for _ in range(msg_size))
            msg = msg.encode('utf-8')

        # In order to compare timestamp in subscriber, we add timestamp manually
        # Note that a timestamp in Kafka is in milliseconds, and Python 
        # time.time() return a float in seconds.
        timestamp = int(time.time() * 1000)
        future = producer.send( # <class 'kafka.producer.future.FutureRecordMetadata'>
            topic=topic, 
            value=msg,
            partition=partition,
            timestamp_ms=timestamp,
        )
        r = future.get() # <class 'kafka.producer.future.RecordMetadata'>
        if future.succeeded():
            time_str = datetime.fromtimestamp(r.timestamp / 1000).strftime(
                '%Y/%m/%d %H:%M:%S.%f'
            )
            logger.info(
                f'{cid}: [{time_str}] {r.topic} '
                f'TX #{iteration} {r.serialized_value_size}'
            )
        else:
            logger.warning(f'{cid}: TX #{iteration} failed.')
        time.sleep(0.000001) # Should be fair enough for logging

    producer.close()
    logger.info(f'{cid}: Close')

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
    partitions = topic_partitions(args.topic, args.broker)
    if partitions is None:
        create_topic(args.broker, args.topic, args.partitions, 
                     args.replication_factor)
        partitions = set([p for p in range(args.partitions)])
        logger.info(f'Topic "{args.topic}" created (partition={partitions}, '
                    f'replication factor={args.replication_factor}).')
    else:
        logger.warning(f'Topic "{args.topic}" exists.')
        if args.producers > len(partitions):
            logger.warning(f'Since only {len(partitions)} for topic, the '
                           f'producer count is reduced to {len(partitions)}.')
        else:
            logger.info(f'{args.producers}-producer will be created.')

    processes = []
    for idx in range(args.producers):
        partition = partitions.pop()
        logger.debug(f'Producer for partition {partition}.')
        p = Process(
            target=produce, 
            args=(
                f'nexgus-producer-{idx+1}',
                args.broker,
                args.topic,
                partition,
                args.acks,
                args.iter_per_producer,
                args.size,
                args.max_size,
                args.binary,
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
        description='Kafka publish example.',
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
        help='Topic.')
    parser.add_argument('-a', '--acks',
        choices=['0', '1', 'all'], default='1',
        help='The number of acknowledgments the producer requires the leader '
             'to have received before considering a request complete. This '
             'controls the durability of records that are sent. The '
             'following settings are common: 0: Producer will not wait for '
             'any acknowledgment from the server. 1: Wait for leader to '
             'write the record to its local log only. all: Wait for the full '
             'set of in-sync replicas to write the record.')
    parser.add_argument('-pd', '--producers',
        type=int, default=1,
        help='Producer count.')
    parser.add_argument('-pt', '--partitions',
        type=int, default=1,
        help='Partition count.')
    parser.add_argument('-rf', '--replication-factor',
        type=int, default=1,
        help='Replication factor.')
    parser.add_argument('-i', '--iter-per-producer',
        type=int, default=10,
        help='Iterations. How many record will be published.')
    parser.add_argument('--binary',
        action='store_true',
        help='Send data binary data.')
    parser.add_argument('-s', '--size',
        type=int,
        help='Message length in bytes.')
    parser.add_argument('-max', '--max-size',
        type=int, default=4096,
        help='Maximum message length in bytes.')
    parser.add_argument('-l', '--level',
        type=str, choices=['debug', 'info', 'warning'], default='info',
        help='Log level.')
    args = parser.parse_args()

    if args.acks and args.acks in ('0', '1'):
        args.acks = int(args.acks)

    args.level = LOG_LEVELS[args.level]
    ch = logging.StreamHandler() # https://docs.python.org/3.7/howto/logging.html#useful-handlers
    ch.setLevel(args.level)
    ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(ch)

    logger.debug(f'{args}')
    main(args)

'''
python publish.py -b nexgus1.westus2.cloudapp.azure.com -t this_is_a_topic -pd 3 -pt 3 -rf 3 -i 10
'''
