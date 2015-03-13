# -*- coding: utf-8 -*-
import datetime
import logging
from psql import datebase_connect
import update_weather
import random
from forecast import



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
    try:
        return cur.fetchall()[0][0]
    except:
        return 0


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
    logging.info(cur.query)
    try:
        center_old = cur.fetchall()[0][0]
        center_new = "SELECT ST_ASText(ST_Centroid(ST_Union(ST_ASText('" + center_old + "')::geometry,ST_ASText('" + center_ + "')::geometry)))"
        logging.info(center_new)
        cur.execute(center_new)
        c_new = cur.fetchall()[0][0]
    except:
        sql = '''SELECT ST_ASText(loc) FROM agz_.mstations
        WHERE region LIKE '{region}%' LIMIT 1'''.format(**{'region': rname_})
        logging.info(sql)
        cur.execute(sql)
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
    sql='''
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
    sql = '''SELECT id FROM agz_.mstations
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

    period_r = {'Партизанский': [2, 44],
                'Идринский': [11, 44],
                'Шушенский': [12, 46],
                'Дзержинский': [14, 52],
                'Мазановский': [3, 47],
                'Козульский': [5, 46],
                'Константиновский': [12, 46],
                'Зейский': [1, 52],
                'Боготольский': [4, 42],
                'Магдагачинский': [1, 52],
                'Рыбинский': [1, 42],
                'Михайловский': [12, 47],
                'Селемджинский': [1, 52],
                'Эвенкийский': [1, 52],
                'Шарыповский': [2, 49],
                'Туруханский': [2, 52],
                'Тюхтетский': [2, 42],
                'Ромненский': [4, 49],
                'Тындинский': [1, 50],
                'Балахтинский': [2, 50],
                'Сковородинский': [1, 50],
                'Большемуртинский': [2, 51],
                'Казачинский': [6, 44],
                'Большеулуйский': [14, 42],
                'Березовский': [1, 52],
                'Абанский': [15, 49],
                'Игарка': [22, 36],
                'Ужурский': [4, 51],
                'Саянский': [12, 44],
                'Архаринский':[2,52],
                'Тамбовский':[9,51],
                'Тасеевский':[8,48],
                'Шимановский':[10,51],
                'Кежемский':[1,52],
                'Таймырский':[2,52],
                'Благовещенский':[3,49],
                'Северо-Енисейский':[22,52],
                'Иланский':[11,48],
                'Мотыгинский':[1,52],
                'Канский':[2,51],
                'Бурейский':[9,48],
                'Курагинский':[8,46],
                'Серышевский':[6,49],
                'Енисейский':[4,51],
                'Завитинский':[5,46],
                'Ачинский':[1,50],
                'Сухобузимский':[15,43],
                'Каратузский':[14,46],
                'Нижнеингашский':[14,52],
                'Минусинский':[12,49],
                'Уярский':[4,42],
                'Белогорский':[4,47],
                'Новоселовский':[12,50],
                'Октябрьский':[12,46],
                'Пировский':[2,52],
                'Богучанский':[1,52],
                'Бирилюсский':[12,48],
                'Ермаковский':[13,47],
                'Краснотуранский':[13,46],
                'Емельяновский':[2,52],
                'Ивановский':[12,46],
                'Назаровский':[2,50],
                'Ирбейский':[5,44],
                'Манский':[2,51],
                'Свободненский':[1,52]}

    # region_r = ['Ростовский']
    delta = datetime.timedelta(days=1)
    datebase_connect('localhost')
    logging.basicConfig(level=logging.NOTSET)
    logging.getLogger('tipper')
    for region in region_r:
        print region
        sql = "SELECT day FROM agz_.day_union WHERE ((sname_ = 'Ярославская обл.') OR (sname_ = 'Амурская обл.') OR (sname_ = 'Красноярский край'))  AND (rname_='{r:s}') ORDER BY day DESC LIMIT 1".format(
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
                      and '{d:s}'::timestamp + '1 day'::interval AND ((sname_ = 'Амурская обл.') OR (sname_ = 'Красноярский край')) AND rname_  = '{r:s}'
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
                        gid =  g[0]
                        if g[0] == None:
                            gid = random.randint(1000000,9999999)

                        sql = ''' SELECT DISTINCT ON (tmax_) tmax_
                        FROM agz_.gar
                        WHERE id_gar = %(id)s AND
                        tmin_ = '%(date)s'::timestamp; ''' % {'id': gid, 'date': g[1]}
                        cur.execute(sql)
                        list_one_gar = cur.fetchall()
                        t = classification(list_one_gar, g[2])
                        logging.debug('T=' + str(t))
                        therm = d_therm(gid, g[2].strftime("%Y-%m-%d"), region)
                        if t[0] == 1:
                            push_day(s_data, g[3], g[4], g[5], get_area(t[1], gid), therm[0], therm[0], g[6])
                        else:
                            push_day(s_data, g[3], g[4], g[5], get_area(t[1][0], gid) - get_area(t[1][1], gid),
                                     therm[0], therm[0], g[6])
                s_d = s_d + delta
            conn.commit()


if __name__ == '__main__':
    # 'Даниловский',
    union_day()







