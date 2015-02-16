# -*- coding: utf-8 -*-
import datetime
import logging
from psql import datebase_connect
import update_weather


ccc, conn, cur = datebase_connect('localhost')

def get_area(t, id):
    """
    Получить площадь гари
    :param t:
    :param id:
    :return:
    """
    sql= '''
    SELECT DISTINCT ON (tmax_) area_
    FROM agz_.gar
    WHERE id_gar = %(id)s AND
    tmax_ = '%(t)s'::timestamp;
    ''' % {'id': id, 't': t}
    logging.info(sql)
    cur.execute(sql)
    return cur.fetchall()[0][0]


def first_day(day, fname_, rname_, sname_):
    """
    Определяем существует ли запись за этот день
    :param day:
    :param fname_:
    :param rname_:
    :param sname_:
    :return:
    """
    sql = '''
    SELECT count(day)
    FROM agz_.day_union WHERE day='%(day)s'::timestamp
    AND fname_='%(fname_)s' AND sname_='%(sname_)s' AND rname_='%(rname_)s'
    ''' % {'day': day, 'fname_': fname_, 'sname_': sname_, 'rname_': rname_}
    logging.info(sql)
    cur.execute(sql)
    if cur.fetchone()[0] is 0:
        return True
    else:
        return False


def null_day(areasum):
    """
    Пустой день
    :param areasum:
    :return:
    """
    if areasum > 0:
        ngar = 1
    else:
        ngar = 0
    return ngar


def needprog(day, need_prog, rname_):
    """
    Прогнозируемый ли это день
    :param day:
    :param need_prog:
    :param rname_:
    :return:
    """
    if int(str(day)[5:7]) is not period_r[rname_][0]:
        return True
    else:
        return False


def add_new_day(areasum, center_, day, fname_, rname_, sname_):
    """
    Добавить новый день
    :param areasum:
    :param center_:
    :param day:
    :param fname_:
    :param rname_:
    :param sname_:
    :return:
    """
    w = d_weather(center_, day)
    sql = '''INSERT INTO agz_.day_union
                    (day, fname_, sname_, rname_, ngar, areasum_,
                    temp, pa, pa2, pd, vl, ff, n, td, rrr, tg, center, wmid, need_prog)
                VALUES ('{day:s}', '{fname_:s}', '{rname_:s}', '{ngar}', '{areasum}',
                    {temp},{pa},{pa2},{pd},{vl},{ff},{n2},{tg},{td},{rrr}, '{center_}',
                    '{wmid}','{need_prog}');'''.format(
        **dict(day=day, fname_=fname_, sname_=sname_, rname_=rname_, areasum=str(areasum), ngar=str(null_day(areasum)),
               temp=w[1],
               pa=w[2], pa2=w[3], pd=w[4], vl=w[5], ff=w[6], n2=w[7], td=w[8], rrr=w[9], tg=w[10], center_=center_,
               wmid=w[0], need_prog=needprog(day, need_prog, rname_)))
    cur.execute(sql.replace('None', '0'))
    conn.commit()


def get_new_center(center_, day, fname_, rname_, sname_):
    """
    Получить новый центр гари
    :param center_:
    :param day:
    :param fname_:
    :param rname_:
    :param sname_:
    :return:
    """
    cur.execute('''SELECT center
        FROM agz_.day_union WHERE day='{day:s}'::timestamp
        AND fname_='{fname_:s}' AND sname_='{sname_:s}' AND rname_='{rname_:s}'
        '''.format(day=day, fname_=fname_, sname_=sname_, rname_=rname_))
    center_old = cur.fetchall()[0][0]
    center_new = "SELECT ST_ASText(ST_Centroid(ST_Union(ST_ASText('" + center_old + "')::geometry,ST_ASText('" + center_ + "')::geometry)))"
    logging.info(center_new)
    cur.execute(center_new)
    c_new = cur.fetchall()[0][0]
    return c_new


def update_current_day(areasum, center_, day, fname_, rname_, sname_):
    """
    Обновить данные существующего дня
    :param areasum:
    :param center_:
    :param day:
    :param fname_:
    :param rname_:
    :param sname_:
    :return:
    """
    w = d_weather(center_, day)
    c_new = get_new_center(center_, day, fname_, rname_, sname_)
    sql = '''UPDATE agz_.day_union SET
        areasum_=areasum_+{areasum}, ngar=ngar+1,
        temp={temp}, pa={pa}, pa2={pa2}, pd={pd}, vl={vl}, ff={ff}, n={n2}, td={td},
        rrr={rrr}, tg={tg}, center='{center_}'
        WHERE day='{day:s}'::timestamp AND fname_='{fname_}' AND sname_='{sname_}' AND rname_='{rname_}'
        '''.format(**dict(day=day, fname_=fname_, sname_=sname_, rname_=rname_, areasum=areasum, temp=w[1], pa=w[2],
                          pa2=w[3], pd=w[4], vl=w[5], ff=w[6], n2=w[7], td=w[8], rrr=w[9], tg=w[10],
                          center_=str(c_new)))
    # print sql
    cur.execute(sql.replace('None', 'Null'))
    conn.commit()


def push_day(day, fname_, sname_, rname_, areasum, nterm, hareasum_, center_):
    """
    Добавить день, если новый, если существует обновить
    :param day:
    :param fname_:
    :param sname_:
    :param rname_:
    :param areasum:
    :param nterm:
    :param hareasum_:
    :param center_:
    :return:
    """
    if first_day(day, fname_, rname_, sname_):
        add_new_day(areasum, center_, day, fname_, rname_, sname_)
    else:
        update_current_day(areasum, center_, day, fname_, rname_, sname_)



def d_therm(id_gar, date, region):
    """

    :param id_gar:
    :param date:
    :param region:
    :return:
    """
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
    """
    Загрузить погоду
    :param center:
    :param date:
    :return:
    """
    #### at first get wmid
    sql='''SELECT id FROM agz_.mstations
      ORDER BY loc <-> '{p:s}'::geometry
      LIMIT 100'''.format(**{'p': center})
    logging.debug(sql)
    cur.execute(sql)
    wmid_list = cur.fetchall()
    for w in wmid_list:
        #update_weather.load_data(w[0], date)
        sql = '''SELECT wmid, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg
        FROM agz_.weather WHERE wmid = {w:s} AND (data::text LIKE '{d:s}') LIMIT 1;
        '''.format(**{'w': str(w[0]), 'd': date})
        logging.debug(sql)
        cur.execute(sql)
        try:
            return cur.fetchall()[0]
            break
        except:
            pass


def classification(gar_data, start_data):
    """
    Определяем тип гари. Есть два типа:
        1 - есть занчения за предыдущий день
        2 - нет значений за предыдущий день
    :param gar_data:
    :param start_data:
    :return:
    """
    int_per = [start_data.replace(hour=0, minute=0, second=0),
                start_data.replace(hour=0, minute=0, second=0) + datetime.timedelta(days=1)]# промежуток между началом этого дня и началом следующего дня
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


def union_day(c = object):
    global period_r
    region_r = ['Большесельский', 'Борисоглебский', 'Брейтовский',
                'Гаврилов-Ямский', 'Любимский', 'Мышкинский',
                'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
                'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
                'Ярославский', 'Угличский']

    period_r = {'Брейтовский': [3, 9],
                'Даниловский': [3, 4],
                'Мышкинский': [3, 6],
                'Первомайский': [3, 6],
                'Некрасовский': [2, 11],
                'Ярославский': [3, 11],
                'Любимский': [4, 10],
                'Рыбинский': [3, 9],
                'Пошехонский': [3, 8],
                'Большесельский': [3, 10],
                'Ростовский': [2, 8],
                'Угличский': [3, 11],
                'Некоузский': [3, 7],
                'Тутаевский': [3, 10],
                'Гаврилов-Ямский': [3, 8],
                'Борисоглебский': [3, 9],
                'Переславский': [3, 9]}
    # region_r = ['Ростовский']
    delta = datetime.timedelta(days=1)
    datebase_connect('localhost')
    logging.basicConfig(level=logging.NOTSET)
    logging.getLogger('tipper')
    for region in region_r:
        print region
        sql = "SELECT day FROM agz_.day_union WHERE (sname_ LIKE 'Яросл%') AND (rname_='{r:s}') ORDER BY day DESC LIMIT 1".format(
            **{'r': region})
        logging.debug(sql)
        cur.execute(sql)
        last_date = 0
        try:
            last_date = cur.fetchall()[0]
            s_d = last_date[0] + delta
        except:
            last_date = datetime.datetime.strptime('2009-' + str(period_r[region][0]) + '-01', "%Y-%m-%d").date()
            #print region, str(period_r[region][0])
            s_d = last_date
        logging.debug(last_date)

        now_date = datetime.date.today() - delta

        if last_date != 0:
            while s_d <= now_date:
                mth = int((s_d).strftime("%m"))
                #print mth, period_r[region][0], period_r[region][1]
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
                         FROM agz_."boundary-polygon" WHERE name LIKE '{r:s}%' LIMIT 1'''.format(**{'r': region}))
                        centroid = cur.fetchone()[0]
                        push_day(s_data, 'Центральный', 'Ярославская обл.', region, 0, 0, 0, centroid)

                    for g in list_gar_one_day:
                        sql = ''' SELECT DISTINCT ON (tmax_) tmax_
                        FROM agz_.gar
                        WHERE id_gar = %(id)s AND
                        tmin_ = '%(date)s'::timestamp; ''' % {'id': g[0], 'date': g[1]}
                        cur.execute(sql)
                        list_one_gar = cur.fetchall()
                        t = classification(list_one_gar, g[2])
                        logging.debug('T=' + str(t))
                        therm = d_therm(g[0], g[2].strftime("%Y-%m-%d"), region)
                        if t[0] == 1:
                            push_day(s_data, g[3], g[4], g[5], get_area(t[1], g[0]), therm[0], therm[0], g[6])
                        else:
                            push_day(s_data, g[3], g[4], g[5], get_area(t[1][0], g[0]) - get_area(t[1][1], g[0]),
                                     therm[0], therm[0], g[6])
                s_d = s_d + delta
            conn.commit()


if __name__ == '__main__':
    # 'Даниловский',
    union_day()







