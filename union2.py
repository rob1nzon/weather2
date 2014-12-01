# -*- coding: utf-8 -*-
import psycopg2
import time

try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
# ## get last date



###

sql = """ SELECT id_gar, to_char(tmin_,'YYYY-MM-DD'), area_, outline_, center_, fname_, sname_,
       rname_, forest_, day_
       FROM agz_.gar WHERE sname_ LIKE 'Ярос%'"""
cur.execute(sql)
results1 = cur.fetchall()

region_r = ['Большесельский', 'Борисоглебский', 'Брейтовский',
            'Гаврилов-Ямский', 'Даниловский', 'Любимский', 'Мышкинский',
            'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
            'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
            'Ярославский', 'Сергиево-Посадский', 'Угличский']

for row in results1:
    if region_r.count(row[7]) > 0: # только районы
        sql = '''SELECT count(*), AVG(area_)
              FROM agz_.term
              WHERE (fn_={fn:s}) AND (sname_ LIKE 'Ярос%')
              AND (to_char(tmin_,'YYYY-MM-DD')
              LIKE '{d:s}') AND rname_='{r:s}' '''.format(**{'fn': str(row[0]), 'd': str(row[1][:10]), 'r': str(row[7])})
        cur.execute(sql)
        list = cur.fetchall()
        # list[0][0], list[0][1]
        isql = ''' INSERT INTO agz_."temp_union"(
                    id_gar, tmin_, tmax_, fname_, sname_, rname_, nterm, hareasum_)
                 VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');''' %\
               (str(row[0]), str(row[1]), str(row[1]), str(row[5]), str(row[6]), str(row[7]), str(list[0][0]), str(list[0][1])[:str(list[0][1]).rfind('.')])
        print '*',
        cur.execute(isql)
        #for a in list:
        #    if region_r.count(a[10]) > 0:  # only region
conn.commit()
