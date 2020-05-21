#/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import mysql.connector
import time

####################################################################################################
VER_MAJOR = 0
VER_MINOR = 0
VER_PATCH = 0

MSG_OK   = ' \033[92mok\033[0m.'
MSG_FAIL = ' \033[91mfail\033[0m.'

####################################################################################################
def main(args):
    row = 0
    row_count = 0

    while True:
        try:
            cnx = mysql.connector.connect(user='root', password='0000', 
                                          host=args.server, database=args.database)
        except Exception as ex:
            cnx.close()
            time.sleep(1)
            continue

        cursor = cnx.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM {args.table}')
        latest_row_count = cursor.fetchone()[0]
        if latest_row_count == row_count:
            cursor.close()
            cnx.close()
            time.sleep(args.interval)
            continue

        diff = latest_row_count - row_count
        cursor.execute(f'SELECT * FROM (SELECT id, topic, value FROM {args.table} ORDER BY id DESC LIMIT {diff}) AS T ORDER BY id ASC')
        for record in cursor:
            print( '-'*30)
            print(f'Row: {row}')
            print(f'ID: {record[0]}')
            print(f'Topic: {record[1]}')
            print(f'Value: {base64.b64decode(record[2])}')
            row += 1
        cursor.close()
        cnx.close()
        row_count = latest_row_count
        time.sleep(args.interval)

####################################################################################################
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Tool for MySQL Database Table Monitor.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('-V', '--version', action='version', version=f'{VER_MAJOR}.{VER_MINOR}.'
                                                                     f'{VER_PATCH}')
    parser.add_argument('--server',   type=str, default='localhost', help='MySQL server IP address.')
    parser.add_argument('--database', type=str, default='kafka',     help='Database name.')
    parser.add_argument('--table',    type=str, default='general',   help='Table name.')
    parser.add_argument('--interval', type=float, default=1.0,       help='Query interval.')
    args = parser.parse_args()

    main(args)
