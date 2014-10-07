# -*- coding: utf-8 -*-
import psycopg2
import sys

try:
    conn = psycopg2.connect("""dbname='agz'
                            user='agz'
                            host='10.115.127.11'
                            password='zagaz'""")

    # conn2 = psycopg2.connect("""dbname='postgres'
    #                         user='postgres'
    #                         host='127.0.0.1'
    #                         password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
f=open('dump.csv', 'w')
cur.copy_to(f, """(SELECT * FROM agz_.s WHERE date_ >= '2009-01-01' AND date_ < '2009-02-01')""", sep=",")
cur.copy_to(f, """(SELECT * FROM agz_.s WHERE date_ >= '2009-02-01' AND date_ < '2009-03-01')""", sep=",")


#cur2 = conn2.cursor()
# cur.execute("""
#             SELECT
#             *
#             FROM agz_.s
#             WHERE
#             date_ >= '2009-01-01' AND date_ < '2009-02-01'
#             """)
#
# records = cur.fetchall()

#for str in records:
#    print str

