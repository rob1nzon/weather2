# -*- coding: utf-8 -*-
import datetime
import logging
from psql import datebase_connect
import update_weather
import random

global region_r
region_r_yar = ['Большесельский', 'Борисоглебский', 'Брейтовский',
                'Гаврилов-Ямский', 'Любимский', 'Мышкинский',
                'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
                'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
                'Ярославский', 'Угличский']


#region_r = ['Зейский', 'Магдагачинский', 'Сковородинский', 'Селемджинский', 'Мазановский',
#'Архаринский', 'Тындинский', 'Свободненский', 'Бурейский', 'Серышевский',
#'Шимановский', 'Благовещенский', 'Михайловский', 'Белогорский', 'Ивановский',
#'Ромненский', 'Октябрьский', 'Завитинский', 'Тамбовский', 'Константиновский']

region_r = ['Абанский', 'Архаринский', 'Ачинский', 'Балахтинский', 'Белогорский',
             'Березовский', 'Бирилюсский', 'Благовещенский', 'Боготольский',
             'Богучанский', 'Большемуртинский', 'Большесельский', 'Большеулуйский', 'Борисоглебский',
             'Брейтовский', 'Бурейский', 'Гаврилов-Ямский', 'Даниловский', 'Дзержинский', 'Емельяновский',
             'Енисейский', 'Ермаковский', 'Завитинский', 'Зейский', 'Ивановский', 'Идринский',
             'Иланский', 'Ирбейский', 'Казачинский', 'Канский', 'Каратузский', 'Кежемский', 'Козульский',
             'Константиновский', 'Краснотуранский', 'Курагинский', 'Любимский', 'Магдагачинский',
             'Мазановский', 'Манский', 'Минусинский', 'Михайловский', 'Мотыгинский', 'Назаровский', 'Некоузский',
             'Некрасовский', 'Нижнеингашский', 'Новоселовский', 'Октябрьский', 'Партизанский', 'Первомайский',
             'Переславль-Залесский', 'Переславский', 'Пировский', 'Пошехонский', 'Ромненский',
             'Ростовский', 'Рыбинский', 'Саянский', 'Свободненский', 'Северо-Енисейский', 'Селемджинский',
             'Серышевский', 'Сковородинский', 'Сухобузимский', 'Таймырский', 'Тамбовский', 'Тасеевский', 'Туруханский',
             'Тутаевский', 'Тындинский', 'Тюхтетский', 'Угличский', 'Ужурский', 'Уярский', 'Шарыповский',
             'Шимановский', 'Шушенский', 'Эвенкийский', 'Ярославский']


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
    result = cur.fetchone()[0]
    print result
    if result == 0:
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
    sql = '''INSERT INTO agz_.day_union
                    (day, fname_, sname_, rname_, ngar, areasum_, need_prog)
                VALUES ('{day:s}', '{fname_:s}', '{rname_:s}', '{sname_:s}', '{ngar}', '{areasum}',
                    '{need_prog}');'''.format(
        **dict(day=day, fname_=fname_, sname_=sname_, rname_=rname_, areasum=str(areasum), ngar=str(null_day(areasum)),
               need_prog=False))
    logging.info(sql)
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
        sql = '''SELECT ST_ASText(geom)
          FROM agz_."boundary-polygon"
          WHERE name LIKE '{region}%' LIMIT 1'''.format(**{'region': rname_})
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
    c_new = get_new_center(center_, day, fname_, rname_, sname_)
    sql = '''UPDATE agz_.day_union SET
        areasum_=areasum_+{areasum}, ngar=ngar+1
        WHERE day='{day:s}'::timestamp AND fname_='{fname_}' AND sname_='{sname_}' AND rname_='{rname_}'
        '''.format(**dict(day=day, fname_=fname_, sname_=sname_, rname_=rname_, areasum=areasum,
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
        print 'NEW DAY'
        add_new_day(areasum, center_, day, fname_, rname_, sname_)
    else:
        print 'OLD DAY'
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
    period_r = {'Брейтовский': [4, 9],
                'Партизанский': [1, 11],
                'Идринский': [3, 11],
                'Шушенский': [3, 11],
                'Дзержинский': [4, 12],
                'Мазановский': [1, 11],
                'Ярославский': [4, 11],
                'Козульский': [1, 11],
                'Ростовский': [3, 8],
                'Константиновский': [3, 11],
                'Зейский': [1, 12],
                'Лесосибирск': [1, 12],
                'Некоузский': [4, 7],
                'Боготольский': [1, 10],
                'Магдагачинский': [1, 12],
                'Рыбинский': [1, 10],
                'Михайловский': [3, 11],
                'Углич': [5, 5],
                'Селемджинский': [1, 12],
                'Тутаево': [7, 7],
                'Эвенкийский': [1, 12],
                'Шарыповский': [1, 12],
                'Ростов': [4, 8],
                'Туруханский': [1, 11],
                'Тюхтетский': [1, 10],
                'Ромненский': [1, 12],
                'Пошехонский': [4, 8],
                'Тындинский': [1, 12],
                'Угличский': [4, 11],
                'Балахтинский': [1, 12],
                'Сковородинский': [1, 12],
                'Большесельский': [4, 10],
                'Большемуртинский': [1, 12],
                'Казачинский': [2, 10],
                'Большеулуйский': [4, 10],
                'Райчихинск': [3, 11],
                'Рыбинск': [4, 9],
                'Переславский': [4, 9],
                'Березовский': [1, 12],
                'Абанский': [4, 12],
                'Игарка': [5, 9],
                'Даниловский': [4, 4],
                'Ужурский': [1, 12],
                'Саянский': [3, 11],
                'Благовещенск': [1, 12],
                'Архаринский': [1, 12],
                'Тамбовский': [2, 12],
                'Тасеевский': [2, 11],
                'Шимановский': [3, 12],
                'Кежемский': [1,12],
                'Таймырский': [1,12],
                'Тутаевский': [4,10],
                'Переславль-Залесский': [4,4],
                'Благовещенский': [1,12],
                'Ярославль': [4,7],
                'Некрасовский': [3,11],
                'Северо-Енисейский': [1,9],
                'Иланский': [3,12],
                'Мотыгинский': [1,12],
                'Канский': [1,12],
                'Бурейский': [2,11],
                'Курагинский': [2,11],
                'Серышевский': [2,12],
                'Первомайский': [4,6],
                'Енисейский': [1,12],
                'Завитинский': [2,11],
                'Ачинский': [1,12],
                'Сухобузимский': [4,10],
                'Каратузский': [4,11],
                'Нижнеингашский': [4,12],
                'Минусинский': [3,12],
                'Уярский': [1,10],
                'Любимский': [5,10],
                'Белогорский': [1,11],
                'Гаврилов-Ямский': [4,8],
                'Новоселовский': [3,12],
                'Октябрьский': [3,11],
                'Пировский': [1,12],
                'Богучанский': [1,12],
                'Бирилюсский': [3,11],
                'Ермаковский': [3,11],
                'Краснотуранский': [3,11],
                'Емельяновский': [1,12],
                'Борисоглебский': [4,9],
                'Ивановский': [3,11],
                'Назаровский': [1,12],
                'Ирбейский': [1,11],
                'Манский': [1,12],
                'Свободненский': [1,12]}

    #region_r = ['Ростовский']
    delta = datetime.timedelta(days=1)
    datebase_connect('localhost')
    logging.basicConfig(level=logging.NOTSET)
    logging.getLogger('tipper')
    for region in region_r:
        print region
        sql = "SELECT day FROM agz_.day_union WHERE" \
              " ((sname_ = 'Ярославская обл.') OR (sname_ = 'Амурская обл.')" \
              " OR (sname_ = 'Красноярский край'))  AND (rname_='{r:s}')" \
              " ORDER BY day DESC LIMIT 1".format(**{'r': region})
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







