# -*- coding: utf-8 -*-
from psql import datebase_connect
from week_union2 import get_region_list, get_strend_week
ccc, conn, cur = datebase_connect('localhost')


def get_max_min():
    """
    """
    col_name = [a+b for a in ['temp', 'pa', 'vl', 'ff', 'n', 'td', 'rrr'] for b in ['','1','2','3']]
    sql = 'SELECT '
    for c in col_name:
        sql += 'MIN(' + c + '), MAX(' + c + ') ,'
    sql= sql[:-1]+ 'FROM agz_.week_union;'
    cur.execute(sql)
    r = cur.fetchone()
    col_name_m = [nc+m for nc in col_name for m in ['min','max']]
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
    ngar integer,  areasum_ bigint,  temp real,  pa real,  vl real,  ff real,  n real,
    td real,  rrr real,  temp1 real,  pa1 real,  vl1 real,  ff1 real,  n1 real,  td1 real,
    rrr1 real,  temp2 real,  pa2 real,  vl2 real,  ff2 real,  n2 real,  td2 real,  rrr2 real,
    temp3 real,  pa3 real,  vl3 real,  ff3 real,  n3 real,  td3 real,  rrr3 real,  delta real,
    delta1 real,  delta2 real,  delta3 real,  center geometry,  cl integer,  region geometry(MultiPolygon,4326),
    pcl integer,  pcl1 integer);

    INSERT INTO agz_.week_union_norm(
    start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_, center,
            {w_c} delta, delta1, delta2, delta3 )

    SELECT start_day, end_day, now, fname_, sname_, rname_, ngar, areasum_, center,'''.format(**{'w_c': ' ,'.join(col)})
    for c in col:
        sql += '('+c+'+ABS({'+c+'min}))/(ABS({'+c+'min})+{'+c+'max}) ,'
    sql +=  ' ABS(vl-temp),ABS(vl1-temp1),ABS(vl2-temp2),ABS(vl3-temp3) FROM agz_.week_union;'
    return sql.format(**d)


def check_id(period, id):
    id2 = list(id)
    id3 = []
    for i in id2:
        sql = '''SELECT count(*)
        FROM agz_.weather
        WHERE wmid={wmid} AND {not_null} is not Null
        AND data BETWEEN '{st}' AND '{ed}'
        '''.format(**{'wmid': i, 'not_null': ' is not Null AND '.join(['temp','pa','vl','ff','n']) ,'st': period[0], 'ed': period[1]})
        #print sql
        cur.execute(sql)
        res = cur.fetchone()[0]
        #print 'count', res
        if res > 0:
            id3.append(str(i))
    #print 'id3', id3
    return id3


def get_weather(period, id):
    sql = '''
    SELECT AVG(temp), AVG(pa), AVG(vl), AVG(ff), AVG(n), AVG(td), AVG(rrr)
    FROM agz_.weather
    WHERE (wmid={wmid} ) AND temp is not null
    AND data BETWEEN '{st}' AND '{ed}'
    '''.format(**{'wmid': ' OR wmid='.join(id), 'st': period[0], 'ed': period[1]})
    #print(sql)
    cur.execute(sql)
    return cur.fetchone()


def get_wmid_on_name(name):
    sql = '''
    SELECT id FROM agz_.mstations WHERE region LIKE '{region}%'
    '''.format(**{'region': name[:-2]})
    #print sql
    cur.execute(sql)
    try:
        return [str(a[0]) for a in cur.fetchall()]
    except:
        return None


def get_wmid_on_cord(cord, n=10):
    sql = '''SELECT id FROM agz_.mstations
       ORDER BY loc <-> '{p:s}'::geometry
       LIMIT {n}'''.format(**{'p': cord,'n': n})
    cur.execute(sql)
    wmid_list = (str(a[0]) for a in cur.fetchall())
    return wmid_list


def push_weather(sd, ed, fn, sn, rn, wd, num):
    if num == 0:
        num = ''
    sql = '''
    UPDATE agz_.week_union
    SET  temp{num}={temp}, pa{num}={pa}, vl{num}={vl},
    ff{num}={ff}, n{num}={n}, td{num}={td}, rrr{num}={rrr}
    WHERE start_day='{sd}' AND end_day='{ed}' AND
    fname_='{fn}' AND sname_='{sn}' AND rname_='{rn}'
    '''.format(**{'num': num, 'temp': wd[0], 'pa': wd[1],
                  'vl': wd[2], 'ff': wd[3], 'n': wd[4],
                  'td': wd[5], 'rrr': wd[6], 'sd': sd,
                  'ed': ed, 'fn': fn, 'sn': sn, 'rn': rn})
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
    #print st, now, week_d
    wmid = get_wmid_list(ct, rn, week_d)
    #print wmid, week_d
    for i, a in enumerate(week_d):
        #print i
        w = get_weather(a, wmid)
        push_weather(st, ed, fn, sn, rn, w, i)


def region_weather():
    region_r = get_region_list()
    #region_r = ['Ачинский']
    for region in region_r:
        print region
        sql = '''
        SELECT start_day, end_day, now, fname_, sname_, rname_, center
        FROM agz_.week_union
        WHERE rname_='{region}' ORDER BY start_day '''.format(**{'region': region})
        cur.execute(sql)
        #print sql
        data = cur.fetchall()
        for regdt in data:
            get_weather_from_name(regdt[0], regdt[1], regdt[2], regdt[3], regdt[4], regdt[5], regdt[6])


if __name__ == '__main__':
    #region_weather()
    print normalization()