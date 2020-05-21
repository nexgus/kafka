#/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import random
import string
import time
from kafka import KafkaProducer

####################################################################################################
def publish(producer, topic, message, timeout=0):
    future = producer.send(topic, message.encode('utf-8'))
    if timeout==0:
        result = None
    else:
        result = future.get(timeout=timeout)

    return result

####################################################################################################
def main(args):
    producer = KafkaProducer(bootstrap_servers=args.broker, acks=args.acks)
    if args.message:
        result = publish(producer, args.topic, args.message, timeout=10)
        print(result)
    else:
        random.seed(args.seed)
        for idx in range(args.count):
            msg_size = random.randint(args.min, args.max)
            source = string.ascii_letters + string.digits + string.punctuation
            msg = ''.join(random.choice(source) for _ in range(args.min, msg_size+1))
            result = publish(producer, args.topic, msg, timeout=10)
            print(result)
            if idx < args.count - 1:
                time.sleep(args.interval)

    if args.metrics:
        print('-'*30)
        print(json.dumps(producer.metrics(), indent=2))

    producer.close()

####################################################################################################
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Tool for Kafka producer.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('broker', type=str, 
        help='Broker IP[:PORT].')
    parser.add_argument('topic', type=str, 
        help='Topic.')
    parser.add_argument('message', type=str, nargs='?', 
        help='Message.')
    parser.add_argument('-a', '--acks', type=str, choices=['0', '1', 'all'], default='1', 
        help='The number of acknowledgments the producer requires the leader to have received '
             'before considering a request complete. This controls the durability of records that '
             'are sent. The following settings are common: "0": Producer will not wait for any '
             'acknowledgment from the server. "1": Wait for leader to write the record to its '
             'local log only. "all": Wait for the full set of in-sync replicas to write the '
             'record.')
    parser.add_argument('-i', '--interval', type=float, default=1.0, 
        help='Interval (in second) between two messages.')
    parser.add_argument('-c', '--count', type=int, default=10, 
        help='Message send count.')
    parser.add_argument('--min', type=int, default=1, 
        help='Minimum size (in byte) of the message.')
    parser.add_argument('--max', type=int, default=2048, 
        help='Maximum size (in byte) of the message.')
    parser.add_argument('-s', '--seed', type=int, default=44, 
        help='Random seed.')
    parser.add_argument('-m', '--metrics', action='store_true', 
        help='Print producer metrics in JSON format.')
    args = parser.parse_args()
    #import sys; print(args); sys.exit(0)

    if args.acks in ('0', '1'):
        args.acks = int(args.acks)

    main(args)
