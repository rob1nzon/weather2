# -*- coding: utf-8 -*-
import psycopg2

try:
    conn2 = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the local database"

cur2 = conn2.cursor()

f=open('temp2.data', 'r')
for a in f:
    row = a.split('#')
    sql = """ INSERT INTO agz_.term2(
            id_term, tmin_, fn_, tmax_, area_, harea_, outline_, center_,
            fname_, sname_, rname_, forest_, date_, day_)
    VALUES ( """
    for b in row:
        sql=sql+"'"+b+"',"
    sql=sql[:-1]+');'
    print sql
    cur2.execute(sql)
conn2.commit()

