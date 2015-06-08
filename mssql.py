# -*- coding: utf-8 -*-
import pymssql

conn = pymssql.connect('10.200.66.149', 'admin', '12345', 'agz')
cursor = conn.cursor()
cursor.execute('SELECT * FROM week_union2')
print cursor.fetchall()
