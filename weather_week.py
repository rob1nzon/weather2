# -*- coding: utf-8 -*-
from psql import datebase_connect, get_region_list
from week_union2 import get_region_list, get_strend_week
import re
from update_weather import load_data
import datetime

ccc, conn, cur = datebase_connect('localhost')


def get_max_min():
    """
    """
    col_name = [a + b for a in ['t','taday', 'tmax', 'po', 'u', 'umin', 'ff', 'n', 'td', 'rrr'] for b in ['', '1', '2', '3']]
    sql = 'SELECT '
    for c in col_name:
        sql += 'MIN(' + c + '), MAX(' + c + ') ,'
    sql = sql[:-1] + 'FROM agz_.week_union;'
    cur.execute(sql)
    r = cur.fetchone()
    col_name_m = [nc + m for nc in col_name for m in ['min', 'max']]
    d = {nc: r[i] for i, nc in enumerate(col_name_m)}
    return d, col_name


def normalization():
    """
    Норамализация
    :return:
    """
    d, col = get_max_min()
    print col
    sql = '''
    DROP TABLE agz_.week_union_norm;
    CREATE TABLE agz_.week_union_norm
    (start_day date,  end_day date,  now integer,  fname_ text,  sname_ text,  rname_ text,
    ngar integer,  areasum_ bigint,  t real,  tmax real,  taday real,  po real,  u real, umin real,  ff real,
    n real,  td real,  rrr real,  t1 real,  tmax1 real,  taday1 real,  po1 real,  u1 real, umin1 real,
    ff1 real,  n1 real,  td1 real,  rrr1 real,  t2 real,  tmax2 real,  taday2 real,  po2 real,
    u2 real, umin2 real, ff2 real,  n2 real,  td2 real,  rrr2 real,  t3 real,  tmax3 real,  taday3 real,
    po3 real,  u3 real, umin3 real,  ff3 real,  n3 real,  td3 real,  rrr3 real,  delta real,  delta1 real,
    delta2 real,  delta3 real,  center geometry,  cl integer,  region geometry(MultiPolygon,4326),
    pcl integer,  pcl1 integer);

    INSERT INTO agz_.week_union_norm(
    start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_, center,
            {w_c}, delta, delta1, delta2, delta3 )

    SELECT start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_, center,'''.format(**{'w_c': ' ,'.join(col)})
    sql += ', '.join(col)
    sql += ', ABS((u-({umin}))/(abs({umax})-({umin})) - (t-({tmin}))/(abs({tmax})-({tmin}))),' \
           ' ABS((u1-({u1min}))/(abs({u1max})-({u1min})) - (t-({t1min}))/(abs({t1max})-({t1min}))),' \
           ' ABS((u2-({u2min}))/(abs({u2max})-({u2min})) - (t-({t2min}))/(abs({t2max})-({t2min}))),' \
           ' ABS((u3-({u3min}))/(abs({u3max})-({u3min})) - (t-({t3min}))/(abs({t3max})-({t3min}))) FROM agz_.week_union;'
    return sql.format(**d)


def check_id(period, id):
    id2 = list(id)
    id3 = []
    for i in id2:
        sql = '''SELECT count(*) FROM agz_.new_weather
        WHERE wmid={wmid} AND {not_null} is not Null
        AND data BETWEEN '{st}' AND '{ed}'
        '''.format(**{'wmid': i, 'not_null': ' is not Null AND '.join(['t', 'po', 'u', 'ff', 'n', 'rrr']), 'st': period[0],
                      'ed': period[1]})
        #print sql
        cur.execute(sql)
        res = cur.fetchone()[0]
        #print 'count', res
        if res > 0:
            id3.append(str(i))
        #print res
    # print 'id3', id3
    return id3


def get_weather(period, id):
    sql = '''
    SELECT data, t, po, u,  ff, n, td, rrr
    FROM agz_.new_weather
    WHERE (wmid={wmid} ) AND data BETWEEN '{st}' AND '{ed}'
    '''.format(**{'wmid': ' OR wmid='.join(id), 'st': period[0], 'ed': period[1]})
    #print(sql)
    cur.execute(sql)
    wh_data = cur.fetchall()
    w = {'t': [], 'tmax': [-999], 'taday': [], 'po': [], 'u': [], 'umin': [1000], 'ff': [], 'n': [], 'td': [], 'rrr': []}

    so = lambda d: float(re.findall('(\d+.\d+)', d)[0])
    sd = lambda d: float(re.findall('(\d+)', d)[0]) if d else None

    for wh in wh_data:
        if wh[1]:
            w['t'].append(float(wh[1]))
            if float(wh[1]) > w['tmax'][0]:
                w['tmax'][0] = float(wh[1])
            if 6 <= wh[0].hour <= 20:
                w['taday'].append(float(wh[1]))  ## дневная температура
        if wh[2]: w['po'].append(float(wh[2]))
        if wh[3]:
            w['u'].append(float(wh[3]))
            if float(wh[3]) < w['umin']:
                w['umin'][0] = float(wh[3])
        if wh[4]: w['ff'].append(float(wh[4]))
        try:
            if wh[5]: w['n'].append(sd(wh[5]))
        except:
            pass
        if wh[6]: w['td'].append(float(wh[6]))
        try:
            if wh[7]:
                if not so(wh[7]):
                    w['rrr'].append(so(wh[7]))
                else:
                    w['rrr'].append(0)

        except:
            if wh[7]: w['rrr'].append(0)
    return {a: sum(w[a]) / len(w[a]) if len(w[a]) else 'Null' for a in w.keys()}

def check_wmid_null(wmid):
    for w in wmid:
        sql = """ SELECT count(t)  FROM agz_.new_weather WHERE wmid = %s """ % w
        cur.execute(sql)
        if cur.fetchone()[0] == 0:
            try:
                load_data(w, (datetime.datetime(2008, 12, 1),))
            except:
                print 'Load fail ' + w


def get_wmid_on_name(name):
    sql = '''
    SELECT id FROM agz_.mstations WHERE region LIKE '{region}%'
    '''.format(**{'region': name[:-2]})
    cur.execute(sql)
    #print sql
    r = [str(a[0]) for a in cur.fetchall()]
    check_wmid_null(r)
    try:
        return r
    except:
        return None


def get_wmid_on_cord(cord, n=20):
    sql = '''SELECT id FROM agz_.mstations
       ORDER BY loc <-> '{p:s}'::geometry
       LIMIT {n}'''.format(**{'p': cord, 'n': n})
    cur.execute(sql)
    #print sql
    wmid_list = (str(a[0]) for a in cur.fetchall())
    check_wmid_null(wmid_list)
    return wmid_list


def push_weather(sd, ed, fn, sn, rn, wd, num):
    if num == 0:
        num = ''
    #print wd
    sql = '''
    UPDATE agz_.week_union
    SET  t{num}={t}, tmax{num}={tmax} ,taday{num}={taday}, po{num}={po}, u{num}={u},
    umin{num}={umin}, ff{num}={ff}, n{num}={n}, td{num}={td}, rrr{num}={rrr}
    WHERE start_day='{sd}' AND end_day='{ed}' AND
    fname_='{fn}' AND sname_='{sn}' AND rname_='{rn}'
    '''.format(**dict({'num': num, 'sd': sd,
                  'ed': ed, 'fn': fn, 'sn': sn, 'rn': rn}, **wd))
    sql = sql.replace('None', 'Null')
    #print sql
    #print (sql)
    cur.execute(sql)
    conn.commit()


def get_cord_by_name(name):
    sql = '''
    SELECT ST_Centroid(geom)
    FROM agz_."boundary-polygon"
    WHERE name LIKE '{name}%'
    '''.format(**{'name': name})
    print sql
    cur.execute(sql)
    return cur.fetchone()[0]


def get_prev_week(now, st):
    """
    Получить список недель до укзанной даты
    """
    week_n = []
    for i in range(0, 4):
        if now - i < 0:
            week_n.append([st.year - 1, 52 + (now - i)])
        else:
            week_n.append([st.year, now - i])
    week_d = (get_strend_week(per[0], per[1]) for per in week_n)
    return week_d


def get_wmid_list(ct, rn, week_d):
    for i, a in enumerate(week_d):
        wmid = check_id(a, get_wmid_on_name(rn))
        if not wmid:
            if ct is None:
                ct = get_cord_by_name(rn)
            wmid = check_id(a, get_wmid_on_cord(ct))[:3]
    #print('wmid %s' % wmid)
    return wmid


def get_weather_from_name(st, ed, now, fn, sn, rn, ct):
    week_d = list(get_prev_week(now, ed))
    # print st, now, week_d
    wmid = get_wmid_list(ct, rn, week_d)
    print wmid
    for i, a in enumerate(week_d):
        #print i
        w = get_weather(a, wmid)
        push_weather(st, ed, fn, sn, rn, w, i)


def region_weather():
    region_r = get_region_list()
    #region_r = ['Рыбинский']
    for region in region_r:
        print region
        sql = '''
        SELECT start_day, end_day, now, fname_, sname_, rname_, center
        FROM agz_.week_union
        WHERE rname_='{region}' AND t is null ORDER BY start_day '''.format(**{'region': region})
        cur.execute(sql)
        #print sql
        data = cur.fetchall()
        for regdt in data:
            get_weather_from_name(regdt[0], regdt[1], regdt[2], regdt[3], regdt[4], regdt[5], regdt[6])


if __name__ == '__main__':
    region_weather()  # bv
    sql = normalization()
    print sql
    #cur.execute(sql)
    #conn.commit()