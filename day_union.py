#! /usr/bin/python
# -*- coding: utf-8 -*-
import logging
from psql import datebase_connect
import update_weather
import random
from datetime import date, timedelta, datetime

ccc, conn, cur = datebase_connect('localhost')


def get_region_list():
    sql = '''
    SELECT DISTINCT ON(rname_) rname_ FROM agz_.waring_week
    '''
    cur.execute(sql)
    return [a[0] for a in cur.fetchall()]


def get_waring_week(region):
    sql = '''SELECT w1
            FROM agz_.waring_week
            WHERE rname_ = '{r:s}' '''.format(**{'r': region})
    cur.execute(sql)
    return [a[0] for a in cur.fetchall()]


def get_strend_week(year, week):
    d = date(year, 1, 1)
    d = d - timedelta(d.weekday())
    dlt = timedelta(days=(week - 1) * 7)
    return d + dlt, d + dlt + timedelta(days=6)


def get_all_term(int_sdate,reg):
    sql = '''
    SELECT count(id_term), sum(area_), sum(harea_), ST_Centroid(ST_Union(center_))
    FROM agz_.term
    WHERE tmin_ BETWEEN '{sd}' AND '{ed}' AND rname_ = '{reg}'
    '''.format(**{'sd': int_sdate, 'ed': int_sdate + timedelta(days=1), 'reg': reg})
    cur.execute(sql)
    print sql
    res = cur.fetchall()
    return res


def push_empty_day(day, fname_, sname_, rname_):
    sql = '''INSERT INTO agz_.day_union(
            day, fname_, sname_, rname_, ngar, areasum_)
    VALUES ('{day}', '{fname_}', '{sname_}', '{rname_}', 0, 0);
    '''.format(**{'day': day, 'fname_': fname_, 'sname_': sname_, 'rname_': rname_})
    cur.execute(sql)

def add_new_day(day, fname_, sname_, rname_, dt):
    sql= '''INSERT INTO agz_.day_union(
            day, fname_, sname_, rname_, ngar, areasum_, center)
    VALUES ('{day}', '{fname_}', '{sname_}', '{rname_}', {ngar}, {areasum}, '{center}');
    '''.format(**{'day': day, 'fname_': fname_, 'sname_': sname_, 'rname_': rname_,
                  'ngar': dt[0], 'areasum': dt[1], 'center': dt[3]})
    cur.execute(sql)



def day_union():
    region_r = get_region_list()
    for region in region_r:
        print region
        wap = get_waring_week(region)
        year_list = [2009, 2010, 2011, 2012, 2013, 2014]
        for y in year_list:
            print y, region
            p1 = get_strend_week(y, wap[0])[0]
            p2 = get_strend_week(y, wap[-1])[1]
            dt = p2 - p1
            for sel_dt in [p1 + timedelta(days=d) for d in range(dt.days)]:
                dt = get_all_term(sel_dt, region)[0]
                print dt
                if dt[0] == 0:
                    push_empty_day(sel_dt, 'Сибирский', 'Красноярский край', region)
                else:
                    add_new_day(sel_dt, 'Сибирский', 'Красноярский край', region, dt)

            conn.commit()




if __name__ == '__main__':
    day_union()