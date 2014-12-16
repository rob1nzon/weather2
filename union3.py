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

def get_area(t, id):
    sql= '''
    SELECT DISTINCT ON (tmax_) area_
    FROM agz_.gar
    WHERE id_gar = %(id)s AND
    tmax_ = '%(t)s'::timestamp;
    ''' % {'id': id, 't': t}
    #print sql
    cur.execute(sql)
    return cur.fetchall()[0][0]

def push_day(d,f,s,r,a):
    sql = '''
    SELECT count(day)
    FROM agz_.day_union WHERE day='%(d)s'::timestamp AND fname_='%(f)s' AND sname_='%(s)s' AND rname_='%(r)s'
    ''' % {'d': d, 'f': f, 's': s, 'r': r}
    #print sql
    cur.execute(sql)
    if cur.fetchall()[0][0]==0:
        sql='''INSERT INTO agz_.day_union(
        day, fname_, sname_, rname_, areasum_)
        VALUES ('%(d)s', '%(f)s', '%(s)s', '%(r)s', '%(a)s');''' % {'d': d, 'f': f, 's': s, 'r': r, 'a': a}
        cur.execute(sql)
        conn.commit()
    else:
        sql='''UPDATE agz_.day_union SET
        areasum_=areasum_+%(a)s
        WHERE day='%(d)s'::timestamp AND fname_='%(f)s' AND sname_='%(s)s' AND rname_='%(r)s' ''' % {'d': d, 'f': f, 's': s, 'r': r, 'a': a}
        #print sql
        cur.execute(sql)
        conn.commit()


    #print sql
    #cur.execute(sql)

def classification(gar_data, interval, start_data):
    #print 'gar DATA'
    #print start_data.date()
    if interval=='day':
        int_per = [start_data.replace(hour=0, minute=0, second=0),
                   start_data.replace(hour=0, minute=0, second=0)+datetime.timedelta(days=1)]
    elif interval=='week':
        print 'week'
    inv_gar_data = gar_data[::-1]  # данные в обратном порядке
    spos = gar_data.index((start_data,))  # позиция заданной гари
    inv_spos = inv_gar_data.index((start_data,))  # позиция заданной гари в обратном порядке
    #### свойства
    left_g  = False
    right_g = False
    not_g = False
    the_one = False
    ####
    if len(gar_data) == 1:
        not_g = True
    [prev_a, prev_b] = [start_data, start_data]
    for a in gar_data[spos+1:]:
        if a[0] > int_per[1]:  # если дата этого момента гари больше
            right_g = True
            break
        prev_a=a[0]
    for b in inv_gar_data[inv_spos+1:]:
        if b[0] < int_per[0]:
            left_g = True
            break
        prev_b=b[0]
    t = [prev_a, prev_b]
    return 5,t

def union_dw(gar_data, interval):
    print "dd"


region_r = ['Большесельский', 'Борисоглебский', 'Брейтовский',
            'Гаврилов-Ямский', 'Даниловский', 'Любимский', 'Мышкинский',
            'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
            'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
            'Ярославский', 'Сергиево-Посадский', 'Угличский']

s_d=datetime.datetime.strptime('2009-07-30', "%Y-%m-%d")
delta = datetime.timedelta(days=1)


for dz in range(356*5):
    s_d=s_d+delta
    s_data = (s_d).strftime("%Y-%m-%d")
    print s_data

    sql = ''' SELECT DISTINCT ON(id_gar) id_gar, tmin_, tmax_, fname_, sname_, rname_
      FROM agz_.gar
      WHERE tmax_ BETWEEN '{d:s}'::timestamp
      and '{d:s}'::timestamp + '1 day'::interval AND sname_ LIKE 'Ярос%' '''.format(**{'d': s_data})

    cur.execute(sql)
    god = cur.fetchall()
    try:
        print god[0][0]
        print sql
    except:
        print ''
    for g in god:
        q = ''' SELECT DISTINCT ON (tmax_) tmax_
        FROM agz_.gar
        WHERE id_gar = %(id)s AND
        tmin_ = '%(date)s'::timestamp; ''' % {'id': g[0], 'date': g[1]}
        #print q
        cur.execute(q)
        t = classification(cur.fetchall(), 'day', g[2])
        push_day(s_data,g[3],g[4],g[5],get_area(t[1][0], g[0])-get_area(t[1][1], g[0]))

conn.commit()







