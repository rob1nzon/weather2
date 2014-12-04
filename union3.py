# -*- coding: utf-8 -*-
import psycopg2
import datetime
import time
try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"
cur = conn.cursor()

def classification(gar_data, interval, start_data):
    #print 'gar DATA'
    print start_data.date()
    if interval=='day':
        int_per = [start_data.replace(hour=0, minute=0, second=0),
              start_data.replace(hour=0, minute=0, second=0)+datetime.timedelta(days=1)]
    elif interval=='week':
        print 'week'
    inv_gar_data = gar_data[::-1]
    spos = gar_data.index((start_data,))
    inv_spos = inv_gar_data.index((start_data,))
    print 'Position: '+str(spos)+' on '+str(len(gar_data))+' elements'
    #### свойства
    left_g  = False
    right_g = False
    not_g = False
    the_one = False
    ####
    if len(gar_data) == 1:
        not_g = True
    for a in gar_data[spos+1:]:
        if a[0] > int_per[1]: right_g = True
    for b in inv_gar_data[inv_spos+1:]:
        if b[0] < int_per[0]: left_g = True
    if not not_g:
        if right_g and left_g:
            return 3
        elif right_g:
            return 2
        elif left_g:
            return 1
        else:
            return 4
    else:
        return 5











s_data='2014-07-30'
sql = ''' SELECT DISTINCT ON(id_gar) id_gar, tmin_, tmax_
  FROM agz_.gar
  WHERE tmax_ BETWEEN '%(d)s'::timestamp
  and '%(d)s'::timestamp + '1 day'::interval''' % {'d': s_data}
cur.execute(sql)
god = cur.fetchall()
for g in god:
    #print g[0], g[1]
    q = ''' SELECT DISTINCT ON (tmax_) tmax_
    FROM agz_.gar
    WHERE id_gar = %(id)s AND
    tmin_ = '%(date)s'::timestamp; ''' % {'id': g[0], 'date': g[1]}
    cur.execute(q)
    print classification(cur.fetchall(), 'day', g[2])


