# -*- coding: utf-8 -*-
import psycopg2

import time

class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print "Elapsed time: {:.3f} sec".format(time.time() - self._startTime)

try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
sql = """ SELECT id_gar, tmin_, tmax_, area_, outline_, center_, fname_, sname_,
       rname_, forest_, date_, day_
  FROM agz_.gar WHERE sname_ LIKE 'Ярос%' LIMIT 10"""
cur.execute(sql)
results1 = cur.fetchall()
#print results1[0][5]
with Profiler() as p:
    for term in results1:
        sql3 = """SELECT wmid, data, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg
          FROM agz_.weather WHERE (wmid = (SELECT id FROM agz_.mstations
          ORDER BY loc <-> '%(p)s'::geometry
          LIMIT 1)) AND (data::text LIKE '%(d)s'); """ % {'p': term[5], 'd': str(term[2])[:10]}
        #print sql3
        cur.execute(sql3)
        results3 = cur.fetchall()
        print term, results3
        #print term[0],term[1],term[2],term[3],term[4],term[5],term[6],term[7],term[8],term[9],term[10],term[11]

