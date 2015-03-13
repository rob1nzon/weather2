# -*- coding: utf-8 -*-
import pymssql

conn = pymssql.connect('10.200.10.69', 'admin', 'admin', 'agz')
cursor = conn.cursor()
cursor.execute('SELECT * FROM week_union')
print cursor.fetchall()
