#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Max Bohnet
#
# Created:     04/10/2012
# Copyright:   (c) Max Bohnet 2012
#-------------------------------------------------------------------------------
#!/usr/bin/python
# -*- coding: utf-8 -*-

import zipfile
import psycopg2
from psycopg2.extras import DictCursor

def convert(schema='mv'):
    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( 'bahn_db',
                                                                              'postgres',
                                                                              '',
                                                                              '192.168.198.24',
                                                                              '5432'
                                                                                )
    conn = psycopg2.connect(psql_connect_string)
    cur = conn.cursor(cursor_factory = DictCursor)
    afz = 'route_short_name,route_long_name,stop_name'.split(',')
    url = 'agency_url'
    times = 'arrival_time,departure_time'.split(',')
    files = 'agency, calendar, calendar_dates, frequencies, routes, shapes, stop_times, stops, transfers, trips'.split(', ')
    with zipfile.ZipFile(r'D:\temp\feed.zip', 'w') as zf:
        for file in files:
            fn= r'D:\temp\%s.txt' %file
            print(fn)
            with open(fn, 'w') as f:
                gtfs_name = 'gtfs_'+file
                sql = 'SELECT * FROM {0}.{1} LIMIT 0'.format(schema, gtfs_name)
                cur.execute(sql)
                names = [col.name for col in cur.description]
                header = ','.join(names)
                f.write(header + '\n')
                sql = 'SELECT * FROM {0}.{1}'.format(schema, gtfs_name)
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    r = []
                    for col in names:
                        field = row[col]
                        if col in afz:
                            field = '"%s"' %field
                        if field is None:
                            field = ''
                        if col == url:
                            if field == '':
                                field = 'http://www.bahn.de'
                        field = '%s' %field
                        r.append(field)
                    f.write(','.join(r) + '\n')
            zf.write(fn, arcname='%s.txt' %file)





def main():
    convert()

if __name__ == '__main__':
    main()
