# -*- coding: utf-8 -*-
from pandas import DataFrame
from psql import datebase_connect
import psycopg2
ccc, conn, cur = datebase_connect('localhost')

def get_max_min():
    """

    {'tempmin': r[0], 'tempmax': r[1],
            'pamin': r[2], 'pamax': r[3],
            'vlmin': r[4], 'vlmax': r[5],
            'ffmin': r[6], 'ffmax': r[7],
            'nmin': r[8], 'nmax': r[9],
            'tdmin': r[10], 'tdmax': r[11],
            'rrrmin': r[12], 'rrrmax': r[13],
            'tgmin': r[14], 'tgmax': r[15]}
    :return:
    """
    sql = '''SELECT MIN(temp), MAX(temp), MIN(pa), MAX(pa), MIN(vl), MAX(vl), MIN(ff), MAX(ff), MIN(n), MAX(n),MIN(td),MAX(td), MIN(rrr),MAX(rrr), MIN(tg),MAX(tg)
  FROM agz_.day_union;'''
    cur.execute(sql)
    r = cur.fetchone()
    temp = [r[0], r[1]]
    pa = [r[2], r[3]]
    vl = [r[4], r[5]]
    ff = [r[6], r[7]]
    n = [r[8], r[9]]
    td = [r[10], r[11]]
    rrr = [r[12], r[13]]
    tg = [r[14], r[15]]
    return {'tempmin': r[0], 'tempmax': r[1],
            'pamin': r[2], 'pamax': r[3],
            'vlmin': r[4], 'vlmax': r[5],
            'ffmin': r[6], 'ffmax': r[7],
            'nmin': r[8], 'nmax': r[9],
            'tdmin': r[10], 'tdmax': r[11],
            'rrrmin': r[12], 'rrrmax': r[13],
            'tgmin': r[14], 'tgmax': r[15]}


def normalization():
    """
    Норамлизация
    :return:
    """
    cur.execute('''
    CREATE TEMPORARY TABLE temp_day_union (day date, fname_ text,  sname_ text,  rname_ text,
    nterm integer,  hareasum_ real,  ngar integer,  areasum_ integer,  temp real,
    pa real,  vl real,  ff real,  n real,  td real, rrr real);
    INSERT INTO temp_day_union(
    day, fname_, sname_, rname_, nterm, hareasum_, ngar, areasum_,
    temp, pa, vl, ff, n, td, rrr)
    SELECT day, fname_, sname_, rname_, nterm, hareasum_, ngar, areasum_,
    (temp+ABS({tempmin}))/(ABS({tempmin})+{tempmax}), (pa+ABS({pamin}))/(ABS({pamin})+{pamax}),
    (vl+ABS({vlmin}))/(ABS({vlmin})+{vlmax}), (ff+ABS({ffmin}))/(ABS({ffmin})+{ffmax}),
    (n+ABS({nmin}))/(ABS({nmin})+{nmax}), (td+ABS({tdmin}))/(ABS({tdmin})+{tdmax}),
    (rrr+ABS({rrrmin}))/(ABS({rrrmin})+{rrrmax}) FROM agz_.day_union;'''.format(**get_max_min()))


def get_waring_period(interval='week'): #WHERE areasum_ > 0
    sql = '''SELECT rname_, MIN(EXTRACT(%s FROM day)), MAX(EXTRACT(%s FROM day))FROM agz_.day_union  GROUP BY rname_ ''' % (interval, interval)
    cur.execute(sql)
    return {a[0]: [a[1], a[2]] for a in cur.fetchall()}



def week_group():
    for region in region_r:
        for year in range(int(period_r_year[region][0]), int(period_r_year[region][1]) + 1):
            for week in range(int(period_r_week[region][0]) + 3, int(period_r_week[region][1]) + 1):
                cur.execute('''SELECT MIN(day) FROM temp_day_union'''.format(**{'week': week, 'year': year, 'region': region}))
                print cur.fetchall()
                if cur.fetchone() is not None:
                    sql = '''
                    WITH week1 AS (SELECT SUM(areasum_), AVG(temp) AS temp1, AVG(pa) AS pa1, AVG(vl) AS vl1, AVG(ff) AS ff1, AVG(n) AS n1, AVG(td) AS td1, AVG(rrr) AS rrr1
                    FROM temp_day_union WHERE EXTRACT(WEEK FROM day) = {week}-1 AND EXTRACT(YEAR FROM day) ={year} AND rname_ = '{region}'),
                    week2 AS (SELECT SUM(areasum_), AVG(temp) AS temp2, AVG(pa) AS tpa2, AVG(vl) AS vl2, AVG(ff) AS ff2, AVG(n) AS n2, AVG(td) AS td2, AVG(rrr) AS rrr2
                    FROM temp_day_union WHERE EXTRACT(WEEK FROM day) = {week}-2 AND EXTRACT(YEAR FROM day) ={year}  AND rname_ = '{region}'),
                    week3 AS (SELECT SUM(areasum_), AVG(temp) AS temp3, AVG(pa) AS pa3, AVG(vl) AS vl3, AVG(ff) AS ff3, AVG(n) AS n3, AVG(td) AS td3, AVG(rrr) AS rrr3
                    FROM temp_day_union WHERE EXTRACT(WEEK FROM day) = {week}-3 AND EXTRACT(YEAR FROM day) ={year}  AND rname_ = '{region}')

                    INSERT INTO agz_.week_union(
                                start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_,
                                temp, pa, vl, ff, n, td, rrr, temp1, pa1, vl1, ff1, n1, td1,
                                rrr1, temp2, pa2, vl2, ff2, n2, td2, rrr2, temp3, pa3, vl3, ff3,
                                n3, td3, rrr3, delta, delta1, delta2, delta3)

                    SELECT MIN(day), MAX(day), {week}, 'fname_','sname_','{region}', SUM(ngar), SUM(areasum_),
                    AVG(temp), AVG(pa), AVG(vl), AVG(ff), AVG(n), AVG(td), AVG(rrr),
                    AVG(temp1), AVG(pa1), AVG(vl1), AVG(ff1), AVG(n1), AVG(td1), AVG(rrr1),
                    AVG(temp2), AVG(tpa2), AVG(vl2), AVG(ff2), AVG(n2), AVG(td2), AVG(rrr2),
                    AVG(temp3), AVG(pa3), AVG(vl3), AVG(ff3), AVG(n3), AVG(td3), AVG(rrr3),
                    ABS(AVG(vl)-AVG(temp)),ABS(AVG(vl1)-AVG(temp1)),ABS(AVG(vl2)-AVG(temp2)),ABS(AVG(vl3)-AVG(temp3))
                    FROM temp_day_union,week1,week2,week3 WHERE EXTRACT(WEEK FROM day) = {week} AND EXTRACT(YEAR FROM day) ={year}  AND rname_ = '{region}'
                    '''.format(**{'week': week, 'year': year, 'region': region})
                    #print sql
                    cur.execute(sql)
                    #print cur.statusmessage
        conn.commit()




if __name__ == '__main__':
    global region_r, period_r_mth, period_r_week, period_r_year
    region_r = list(get_waring_period().viewkeys())
    period_r_mth = get_waring_period(interval='MONTH')
    period_r_week = get_waring_period(interval='WEEK')
    period_r_year = get_waring_period(interval='YEAR')

    #normalization()
    #print get_waring_period()

    normalization()
    week_group()

#df = DataFrame({'Deep 0': colums[0], 'Deep 1': colums[1], 'Deep 2': colums[2], 'Deep 3': colums[3],'Deep 4': colums[4]})
#df.to_csv('Tree.csv')

