# -*- coding: utf-8 -*-
import psycopg2
import datetime
import StringIO
from psql import datebase_connect

def connect():
    '''
    Подключиться к базе
    :return: s2, conn2, cur2, s, conn, cur
    '''
    s2, conn2, cur2 = datebase_connect('localhost')
    s, conn, cur = datebase_connect('ncuks')
    return s2, conn2, cur2, s, conn, cur

def get_last_date(cur, cur2):
    '''
    Получить последние даты
    :param cur:
    :param cur2:
    :return: now_d, old_d
    '''
    print "Get last date in local base"
    sql = """ SELECT date_ FROM agz_.gar ORDER BY date_ DESC LIMIT 1; """
    cur2.execute(sql)
    old_d = cur2.fetchone()[0]

    print old_d
    print "Get last date in NCUKS base"
    sql = """ SELECT date_ FROM agz_.f WHERE date_ > '%(d)s' ORDER BY date_ DESC LIMIT 1; """ % {'d': old_d}
    cur.execute(sql)
    try:
        now_d = cur.fetchone()[0]
    except:
        now_d = old_d
    print now_d
    return now_d, old_d

def update_gar(conn2, cur2, conn, cur, now_d, old_d):
    '''
    Обновить гари за указанный промежуток времени
    :param conn2:
    :param cur2:
    :param conn:
    :param cur:
    :param now_d:
    :param old_d:
    :return:
    '''

    print old_d
    print "Update data: %(a)s - %(b)s" % {'a': old_d, 'b': now_d}
    sql = """(SELECT fn_, tmin_, tmax_, area_, outline_, center_, fname_, sname_, rname_, forest_, date_, day_
    FROM agz_.f WHERE (date_ >= '%(o)s') AND (date_ < '%(n)s'))""" % {'o': old_d, 'n': now_d}
    print sql
    cur.execute(sql)
    data = StringIO.StringIO()
    f = open('temp3.data', 'w')
    cur.copy_to(f, sql, sep="#")
    f.close()
    f = open('temp3.data', 'r')
    for a in f:
        row = a.split('#')
        sql = """ INSERT INTO agz_.gar(
        id_gar, tmin_, tmax_, area_, outline_, center_, fname_, sname_,
        rname_, forest_, date_, day_)
    VALUES ( """
        for b in row:
            sql = sql + "'" + b + "',"
        sql = sql[:-1] + ');'
        #print sql
        try:
            cur2.execute(sql.replace("'\N'", 'NULL'))
        except:
            print 'Error'
            print sql
        print '*',
        conn2.commit()


def update():
    s2, conn2, cur2, s, conn, cur = connect()
    now_d, old_d = get_last_date(cur, cur2)
    print type(now_d), type(old_d)
    print now_d, old_d

    if old_d != now_d:
        delta_d = datetime.timedelta(days=30)
        (f_d, l_d) = (old_d, old_d+delta_d)
        while f_d < now_d:
            update_gar(conn2, cur2, conn, cur, l_d, f_d)
            f_d += delta_d
            l_d += delta_d
        #ow_date = datetime.date.today()-datetime.timedelta(days=1)
        #1 = str(now_date.month)
        #s_d = old_d





if __name__ == '__main__':
    update()

#f.close()
#cur2.copy_expert("COPY agz_.term2 FROM STDIN (FORMAT 'csv', DELIMITER '#', NULL '/N', ENCODING 'UTF8')", data)
#conn2.commit



