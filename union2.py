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

    sql = '''SELECT count(*),AVG(area_)
          FROM agz_.term
          WHERE (fn_={fn:s}) AND (sname_ LIKE 'Ярос%')
          AND (to_char(tmin_,'YYYY-MM-DD')
          LIKE '{d:s}')'''.format(**{'fn': str(row[0]), 'd': str(row[1][:10])})
    print sql
    cur.execute(sql)
    list = cur.fetchall()
    print list[0][0], list[0][1]
    cr = 0
    cc = 0
    isql = ''' INSERT INTO agz_."temp_union"(
                id_gar, tmin_, tmax_, area_, fname_, sname_, rname_, forest_,
            day_, nterm, hareasum_)
            SELECT '''
    #for a in list:
    #    if region_r.count(a[10]) > 0:  # only region









    # results2=cur.fetchall()
    # print row[1],'date #####'
    # for row2 in results2:
    #     for i,b in enumerate(row2):
    #         print i,b
    #
    # print '####'
