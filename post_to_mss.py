# -*- coding: utf-8 -*-
import psycopg2
import datetime
import StringIO
from psql import datebase_connect
import pymssql


def connect():
    """
    Подключиться к базе
    :return: s2, conn2, cur2, s, conn, cur
    """
    #s2, conn2, cur2 = datebase_connect('localhost')
    s2 = 1
    conn2 = pymssql.connect('10.200.66.149', 'admin', '12345', 'fires')
    cur2 = conn2.cursor()
    s, conn, cur = datebase_connect('localhost')
    return s2, conn2, cur2, s, conn, cur



def transact(conn2, cur2, conn, cur):
    sql = """(SELECT start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_,
       t, tmax, taday, po, u, umin, ff, n, td, rrr, t1, tmax1, taday1,
       po1, u1, umin1, ff1, n1, td1, rrr1, t2, tmax2, taday2, po2, u2,
       umin2, ff2, n2, td2, rrr2, t3, tmax3, taday3, po3, u3, umin3,
       ff3, n3, td3, rrr3, delta, delta1, delta2, delta3
       FROM agz_.week_union_norm)"""
    print sql
    cur.execute(sql)
    f = open('temp5.data', 'w')
    cur.copy_to(f, sql, sep="#")
    f.close()
    f = open('temp5.data', 'r')
    for a in f:
        row = a[:-1].split('#')
        sql = """ INSERT INTO week_union_norm(
            start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_,
            t, tmax, taday, po, u, umin, ff, n, td, rrr, t1, tmax1, taday1,
            po1, u1, umin1, ff1, n1, td1, rrr1, t2, tmax2, taday2, po2, u2,
            umin2, ff2, n2, td2, rrr2, t3, tmax3, taday3, po3, u3, umin3,
            ff3, n3, td3, rrr3, delta, delta1, delta2, delta3)
    VALUES ( """
        #print row
        for b in row:
            if '\N' in b:
                sql = sql + "Null, "
            else:
                sql = sql + "'" + b + "',"
        #sql = sql +"geometry::STGeomFromText('"+ row[-1]+"',4326),"
        sql = sql.replace('\\n','')
        sql = sql[:-2] + "');"
        # print sql
        try:
            #sql = sql.replace("\\N\n", 'Null')
            cur2.execute(sql)
        except:
            print 'Error'
            print sql
        print '*',
        conn2.commit()


def update():
    s2, conn2, cur2, s, conn, cur = connect()

    transact(conn2, cur2, conn, cur)


if __name__ == '__main__':
    update()

# f.close()
#cur2.copy_expert("COPY agz_.term2 FROM STDIN (FORMAT 'csv', DELIMITER '#', NULL '/N', ENCODING 'UTF8')", data)
#conn2.commit



