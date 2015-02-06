# -*- coding: utf-8 -*-
import datetime
import logging
from psql import datebase_connect

ccc, conn, cur = datebase_connect('localhost')

def get_area(t, id):
    sql= '''
    SELECT DISTINCT ON (tmax_) area_
    FROM agz_.gar
    WHERE id_gar = %(id)s AND
    tmax_ = '%(t)s'::timestamp;
    ''' % {'id': id, 't': t}
    logging.info(sql)
    cur.execute(sql)
    return cur.fetchall()[0][0]


def push_day(day, fname_, sname_, rname_, areasum, nterm, hareasum_, center_):
    sql = '''
    SELECT count(day)
    FROM agz_.day_union WHERE day='%(day)s'::timestamp
    AND fname_='%(fname_)s' AND sname_='%(sname_)s' AND rname_='%(rname_)s'
    ''' % {'day': day, 'fname_': fname_, 'sname_': sname_, 'rname_': rname_}
    logging.info(sql)
    cur.execute(sql)
    if cur.fetchall()[0][0] == 0:
        w = d_weather(center_, day)
        try:
            sql = '''INSERT INTO agz_.day_union
                    (day,
                    fname_,
                    sname_,
                    rname_,
                    nterm,
                    hareasum_,
                    ngar,
                    areasum_,
                    temp, pa, pa2, pd, vl, ff, n, td, rrr, tg, center)
                VALUES (
                    '{day:s}',
                    '{fname_:s}',
                    '{sname_:s}',
                    '{rname_:s}',
                    '{nterm:s}',
                    '{hareasum_:s}',
                    '{ngar}',
                    '{areasum}',
                    {temp},{pa},{pa2},{pd},{vl},{ff},{n2},{tg},
                    {td},{rrr},'{center_}');'''.format(**{
            'day': day,
            'fname_': fname_,
            'sname_': sname_,
            'rname_': rname_,
            'areasum': str(areasum),
            'ngar': 1,
            'hareasum_': str(hareasum_),
            'nterm': str(nterm),
            'temp': w[1],
            'pa': w[2],
            'pa2': w[3],
            'pd': w[4],
            'vl': w[5],
            'ff': w[6],
            'n2': w[7],
            'td': w[8],
            'rrr': w[9],
            'tg': w[10],
            'center_': center_})
        except:
            sql = '''INSERT INTO agz_.day_union
                    (day,
                    fname_,
                    sname_,
                    rname_,
                    nterm,
                    hareasum_,
                    ngar,
                    areasum_,
                    temp, pa, pa2, pd, vl, ff, n, td, rrr, tg, center)
                VALUES (
                    '{day:s}',
                    '{fname_:s}',
                    '{sname_:s}',
                    '{rname_:s}',
                    '{nterm:s}',
                    '{hareasum_:s}',
                    '{ngar}',
                    '{areasum}',
                    {temp},{pa},{pa2},{pd},{vl},{ff},{n2},{tg},
                    {td},{rrr},'{center_}');'''.format(**{
                'day': day,
                'fname_': fname_,
                'sname_': sname_,
                'rname_': rname_,
                'areasum': str(areasum),
                'ngar': 1,
                'hareasum_': str(hareasum_),
                'nterm': str(nterm),
                'temp': 'Null',
                'pa': 'Null',
                'pa2': 'Null',
                'pd': 'Null',
                'vl': 'Null',
                'ff': 'Null',
                'n2': 'Null',
                'td': 'Null',
                'rrr': 'Null',
                'tg':  'Null',
                'center_': center_})
            logging.error('None weather')
        print sql.replace('None', '0')
        print areasum
        cur.execute(sql.replace('None', '0'))
        conn.commit()
    else:
        cur.execute('''SELECT center
        FROM agz_.day_union WHERE day='%(day)s'::timestamp
        AND fname_='%(fname_)s' AND sname_='%(sname_)s' AND rname_='%(rname_)s'
        ''' % {'day': day, 'fname_': fname_, 'sname_': sname_, 'rname_': rname_})
        center_old = cur.fetchall()[0][0]
        center_new = 'SELECT '+"ST_ASText(ST_Centroid(ST_Union(ST_ASText('"+center_old+"')::geometry,ST_ASText('"+center_+"')::geometry)))"
        w = d_weather(center_, day)
        logging.info(center_new)
        cur.execute(center_new)
        c_new = cur.fetchall()[0][0]
        #print c_new
        print w
        if hareasum_== None : hareasum_ = '0'
        sql= '''UPDATE agz_.day_union SET
        areasum_=areasum_+{areasum}, hareasum_=hareasum_+{hareasum_}, nterm=nterm+{nterm}, ngar=ngar+1,
        temp={temp}, pa={pa}, pa2={pa2}, pd={pd}, vl={vl}, ff={ff}, n={n2}, td={td},
        rrr={rrr}, tg={tg}, center='{center_}'
        WHERE day='{day:s}'::timestamp AND fname_='{fname_}' AND sname_='{sname_}' AND rname_='{rname_}'
        '''.format(**{'day': day,
                      'fname_': fname_,
                      'sname_': sname_,
                      'rname_': rname_,
                      'areasum': areasum,
                      'nterm': nterm,
                      'hareasum_': hareasum_,
                      'temp': w[1],
                      'pa': w[2],
                      'pa2': w[3],
                      'pd': w[4],
                      'vl': w[5],
                      'ff': w[6],
                      'n2': w[7],
                      'td': w[8],
                      'rrr': w[9],
                      'tg': w[10],
                      'center_': str(c_new)})
        print sql
        cur.execute(sql.replace('None', 'Null'))
        conn.commit()

    #print sql
    #cur.execute(sql)


def d_therm(id_gar, date, region):
    sql= '''
    SELECT COUNT(*) FROM agz_.gar WHERE (id_gar={id:s}) AND (rname_='{r:s}') AND (tmax_::text LIKE  '{date:s}%')
    '''.format(**{'id': str(id_gar), 'date': date, 'r': region})
    logging.info(sql)
    cur.execute(sql)
    try:
        return cur.fetchall()[0]
    except:
        return [0, 0]

def d_weather(center, date):
    #### at first get wmid
    sql='''SELECT id FROM agz_.mstations
      ORDER BY loc <-> '{p:s}'::geometry
      LIMIT 1000'''.format(**{'p': center})
    logging.debug(sql)
    cur.execute(sql)
    wmid_list = cur.fetchall()
    for w in wmid_list:
        sql = '''SELECT wmid, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg
        FROM agz_.weather WHERE wmid = {w:s} AND (data::text LIKE '{d:s}');
        '''.format(**{'w': str(w[0]), 'd': date})
        logging.debug(sql)
        cur.execute(sql)
        try:
            return cur.fetchall()[0]
            break
        except:
            pass


def classification(gar_data, start_data):
    '''
    Определяем тип гари. Есть два типа:
        1 - есть занчения за предыдущий день
        2 - нет значений за предыдущий день
    :param gar_data:
    :param start_data:
    :return:
    '''
    int_per = [start_data.replace(hour=0, minute=0, second=0),
                start_data.replace(hour=0, minute=0, second=0)+datetime.timedelta(days=1)]# промежуток между началом этого дня и началом следующего дня
    len_data = len(gar_data)
    #print len_data
    if len_data < 1:
       return 1, start_data
    else:
        inv_gar_data = gar_data[::-1]  # данные в обратном порядке
        spos = gar_data.index((start_data,))  # позиция заданной гари
        inv_spos = inv_gar_data.index((start_data,))  # позиция заданной гари в обратном порядке
        now = start_data  # начальная дата наблюдения гари
        yesterday = 0
        for b in inv_gar_data[inv_spos+1:]:
            if b[0] < int_per[0]:
                yesterday = b[0]
                break

        for a in gar_data[spos+1:]:
            if a[0] > int_per[1]:
                break
            now = a[0]

        if yesterday == 0:
            return 1, now
        else:
            return 2, [now, yesterday]

# 'Даниловский',
region_r = ['Большесельский', 'Борисоглебский', 'Брейтовский',
            'Гаврилов-Ямский',  'Любимский', 'Мышкинский',
            'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
            'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
            'Ярославский', 'Угличский']
period_r = {'Брейтовский': [4, 9],
            'Даниловский': [4, 4],
            'Первомайский': [4, 6],
            'Некрасовский': [3, 11],
            'Ярославский': [4, 11],
            'Любимский': [5, 10],
            'Рыбинский': [4, 9],
            'Пошехонский': [4, 8],
            'Большесельский': [4, 10],
            'Ростовский': [3, 8],
            'Угличский': [4, 11],
            'Некоузский': [4, 7],
            'Тутаевский': [4, 10],
            'Гаврилов-Ямский': [4 ,8],
            'Борисоглебский': [4, 9],
            'Переславский': [4, 9]}


#region_r = ['Ростовский']

delta = datetime.timedelta(days=1)
datebase_connect('localhost')
logging.basicConfig(level=logging.NOTSET)
logging.getLogger('tipper')

for region in region_r:
    sql = "SELECT day FROM agz_.day_union WHERE (sname_ LIKE 'Яросл%') AND (rname_='{r:s}') ORDER BY day DESC LIMIT 1".format(
            **{'r': region})
    logging.debug(sql)
    cur.execute(sql)
    last_date = 0
    try:
        last_date = cur.fetchall()[0]
        s_d = last_date[0]+delta
    except:
        sql ="SELECT tmin_ FROM agz_.gar WHERE (sname_ LIKE 'Яросл%') AND (rname_='{r:s}') ORDER BY tmin_ ASC LIMIT 1".format(
                **{'r': region})
        logging.debug(sql)
        cur.execute(sql)
        try:
            last_date = datetime.datetime.strptime(cur.fetchall()[0][0].strftime("%Y-%m-%d"), "%Y-%m-%d").date()
        except:
            pass
        s_d = last_date
    logging.debug(last_date)

    now_date = datetime.date.today()
    #s_d = datetime.datetime.strptime('2014-04-10', "%Y-%m-%d").date()
    #now_date = datetime.datetime.strptime('2014-04-11', "%Y-%m-%d").date()

    if last_date != 0:
        while s_d <= now_date:
            mth = int((s_d).strftime("%m"))
            print mth, period_r[region][0], period_r[region][1]
            if (mth >= int(period_r[region][0])) and (mth <= int(period_r[region][1])):
                s_data = (s_d).strftime("%Y-%m-%d")
                #print s_data
                sql = ''' SELECT DISTINCT ON(id_gar) id_gar, tmin_, tmax_, fname_, sname_, rname_, center_
                  FROM agz_.gar
                  WHERE tmax_ BETWEEN '{d:s}'::timestamp
                  and '{d:s}'::timestamp + '1 day'::interval AND sname_ LIKE 'Ярос%' AND rname_  = '{r:s}'
                 '''.format(**{'d': s_data, 'r': region})
                logging.info(sql)
                cur.execute(sql)
                list_gar_one_day = cur.fetchall()
                if list_gar_one_day == []:
                    cur.execute('''SELECT ST_Centroid(geom)
                     FROM agz_."boundary-polygon" WHERE name LIKE '{r:s}%' LIMIT 1'''.format(**{'r':region}))
                    centroid = cur.fetchone()[0]
                    push_day(s_data, 'Центральный', 'Ярославская обл.', region, 0, 0, 0, centroid)
                #print '####LIST_GAR####'
                for g in list_gar_one_day:
                    sql = ''' SELECT DISTINCT ON (tmax_) tmax_
                    FROM agz_.gar
                    WHERE id_gar = %(id)s AND
                    tmin_ = '%(date)s'::timestamp; ''' % {'id': g[0], 'date': g[1]}
                    #print g
                    cur.execute(sql)
                    list_one_gar = cur.fetchall()
                    t = classification(list_one_gar, g[2])
                    logging.debug('T='+str(t))
                    therm = d_therm(g[0], g[2].strftime("%Y-%m-%d"), region)
                    if t[0] == 1:
                        push_day(s_data, g[3], g[4], g[5], get_area(t[1], g[0]), therm[0], therm[0], g[6])
                    else:
                        push_day(s_data, g[3], g[4], g[5], get_area(t[1][0], g[0])-get_area(t[1][1], g[0]), therm[0], therm[0], g[6])
            s_d = s_d+delta
        conn.commit()







