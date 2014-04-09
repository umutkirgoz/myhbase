#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Apr 9, 2014
# @author Umut KIRGÖZ

import argparse,happybase

parser=argparse.ArgumentParser(description='MyHBASE is a simple Python script to handle batch operations in HBASE tables', usage='delete --host=hadoop.server.com --table=table_name --row_prefix=abc_')

parser.add_argument('--host', help='Hadoop Server Host',dest = 'host',type=str,required=True)
parser.add_argument('--port', help = 'Hadoop Server Port',dest = 'port',type=int, default=9090)
parser.add_argument('--transport', help = 'Thrift transport mode',dest = 'transport',type=str, default='framed')

parser.add_argument('-t', '--table', help = 'HBase table name',dest = 'table',type=str,required=True)
parser.add_argument('--row_prefix', help = 'Row prefix',type=str,required=True)
parser.add_argument('-l','--limit',help = 'Limit', type=int, default=None)
parser.add_argument('--version', action='version', version='%(prog)s 0.1')
args=vars(parser.parse_args())

#Opening connection to hadoop server via happybase
c= happybase.Connection(host=args['host'],port=args['port'],transport=args['transport'])

#Getting table instance
t = c.table(args['table'])

#Getting table batch instance
b = t.batch()

#Setting batch limit
batchLimit = args['limit']
if batchLimit is None:
    batchLimit = 500

counter = 0
total = 0
rows = t.scan(row_prefix=args['row_prefix'], limit=args['limit'])

for key, data in rows:        
    b.delete(key)
    counter += 1
    total += 1    
    if (counter > batchLimit):       
        b.send()
        counter = 0
b.send()        
c.close()
print "%s rows deleted" %(total)