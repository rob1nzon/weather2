# -*- coding: utf-8 -*-
import psycopg2
import sys

try:
    conn = psycopg2.connect("""dbname='agz'
                            user='agz'
                            host='10.115.127.11'
                            password='zagaz'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
f=open('dump_s_real.csv', 'w')


list_date = ['2009-01-01',  '2009-04-01',  '2009-05-01',  '2009-06-01',  '2009-07-01',  '2009-08-01',  '2009-09-01',  '2009-10-01',  '2010-01-01',  '2010-04-01',  '2010-05-01',  '2010-06-01',  '2010-07-01',  '2010-08-01',  '2010-09-01',  '2010-10-01',  '2011-01-01',  '2011-04-01',  '2011-05-01',  '2011-06-01',  '2011-07-01',  '2011-08-01',  '2011-09-01',  '2011-10-01',  '2012-01-01',  '2012-04-01',  '2012-05-01',  '2012-06-01',  '2012-07-01',  '2012-08-01',  '2012-09-01',  '2012-10-01',  '2013-01-01',  '2013-04-01',  '2013-05-01',  '2013-06-01',  '2013-07-01',  '2013-08-01',  '2013-09-01',  '2013-10-01',  '2014-01-01',  '2014-04-01',  '2014-05-01',  '2014-06-01',  '2014-07-01',  '2014-08-01',  '2014-09-01',  '2014-10-01']
for i in xrange(len(list_date)-1):
    q = """(SELECT sn_, tmin_, fn_, tmax_, area_, harea_, outline_, center_, fname_, sname_, rname_, forest_, date_, day_ FROM agz_.s WHERE date_ >= '%(fd)s' AND date_ < '%(sd)s')""" % {'fd': list_date[i],'sd': list_date[i+1]}
    cur.copy_to(f, q, sep="#")
    print q
#cur.copy_to(f, '(SELECT pplace_, fname_, sname_, rname_, name_, point_ FROM agz_.pp)', sep="#")

