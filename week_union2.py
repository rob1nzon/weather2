# -*- coding: utf-8 -*-
import logging

from psql import  datebase_connect,get_region_list

#import update_weather
import random
from datetime import date, timedelta, datetime

global region_r

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
             'Переславский', 'Пировский', 'Пошехонский', 'Ромненский',
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
    sql = sql.replace('None','Null')
    logging.info(sql)
    cur.execute(sql)
    try:
        return cur.fetchall()[0][0]
    except:
        return 0





def first_week(start_day, end_day, now, fname, sname, rname):
    sql = '''
    SELECT count(start_day)
    FROM agz_.week_union
    WHERE start_day = '{start_day}' AND end_day = '{end_day}'
    AND fname_='{fname}' AND sname_='{sname}' AND rname_='{rname}'
    '''.format(**{'start_day': start_day, 'end_day': end_day,
                  'fname': fname, 'sname': sname, 'rname': rname})
    logging.info(sql)
    cur.execute(sql)
    result = cur.fetchone()[0]
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


def add_new_week(areasum, ngar, start_day, end_day, now, fname_, sname_, rname_, center):
    """
    Добавить новую неделю
    """
    if center == 0:
        center = 'Null'
    else:
        center = "'" + center + "'"

    sql = '''INSERT INTO agz_.week_union(start_day, end_day, now,
        fname_, sname_, rname_, ngar, areasum_, center)
        VALUES ('{start_day}','{end_day}','{now}','{fname}','{sname}',
        '{rname}', '{ngar}' , '{areasum}', {center})
    '''.format(**{'start_day': str(start_day), 'end_day': end_day,
                  'now': now, 'fname': fname_, 'sname': sname_,
                  'rname': rname_, 'areasum': areasum, 'ngar': ngar, 'center': center})
    logging.info(sql)
    cur.execute(sql)
    conn.commit()




def get_new_center(center_, day, fname_, rname_, sname_):
    """
    Получить новый центр гари
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


def upd_cur_week(areasum, start_day, end_day, fname_, sname_, rname_, center):
    """
    Обновить данные существующей недели
    """
    cnew = 0 #ToDo: центр!!!
    sql = '''
    UPDATE agz_.week_union
    SET ngar=ngar+1, areasum_=areasum_+{areasum}, center='{center}'
    WHERE start_day='{start_day}' AND end_day='{end_day}' AND fname_='{fname}' AND
    sname_='{sname}' AND rname_='{rname}'
    '''.format(**{'areasum': areasum, 'start_day': start_day, 'end_day': end_day,
                'fname': fname_, 'sname': sname_, 'rname': rname_, 'center': center})
    cur.execute(sql.replace('None', 'Null'))
    conn.commit()


def update_current_day(areasum, center_, day, fname_, rname_, sname_):
    """
    Обновить данные существующего дня
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



def push_week(start_day, end_day, now, fname_, sname_, rname_, areasum_, center):
    if first_week(start_day, end_day, now, fname_, sname_, rname_):
        add_new_week(areasum_, 1, start_day, end_day, now, fname_, sname_, rname_, center)
    else:
        upd_cur_week(areasum_, start_day, end_day, fname_, sname_, rname_, center)



def get_waring_week(region):
    sql = '''SELECT w1
            FROM agz_.waring_week
            WHERE rname_ = '{r:s}' '''.format(**{'r': region})
    cur.execute(sql)
    return [a[0] for a in cur.fetchall()]


def classification(gar_data, start_data, end_data):
    """
    Определяем тип гари. Есть два типа:
        1 - есть занчения за предыдущий день
        2 - нет значений за предыдущий день
    :param gar_data:
    :param start_data:
    :return:
    """
    int_per = [start_data.replace(hour=0, minute=0, second=0),
                end_data.replace(hour=0, minute=0, second=0)]# промежуток между началом этого дня и началом следующего дня
    len_data = len(gar_data)
    if len_data < 1:
       return 1, start_data
    else:
        inv_gar_data = gar_data[::-1]  # данные в обратном порядке
        spos = gar_data.index((start_data,))  # позиция заданной гари
        inv_spos = inv_gar_data.index((start_data,))  # позиция заданной гари в обратном порядке
        now = start_data  # начальная дата наблюдения гари
        yesterday = 0
        for b in inv_gar_data[inv_spos + 1:]:
            if b[0] < int_per[0]:
                yesterday = b[0]
                break

        for a in gar_data[spos + 1:]:
            if a[0] > int_per[1]:
                break
            now = a[0]

        if yesterday == 0:
            return 1, now
        else:
            return 2, [now, yesterday]


def get_last_date(delta, period_r, region):
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
        last_date = strptime('2009-' + str(period_r[region][0]) + '-01', "%Y-%m-%d").date()
        # print region, str(period_r[region][0])
        s_d = last_date
    return last_date, s_d


def get_all_gar_on_period(region, start_date, end_date):
    sql = ''' SELECT DISTINCT ON(id_gar) id_gar, tmin_, tmax_, fname_, sname_, rname_, center_
              FROM agz_.gar
              WHERE tmax_ BETWEEN '{st:s}'::timestamp and '{ed:s}'::timestamp
              AND ((sname_ = 'Ярославская обл.') OR (sname_ = 'Амурская обл.') OR (sname_ = 'Красноярский край'))
              AND rname_  = '{r:s}'
              '''.format(**{'st': start_date.strftime("%Y-%m-%d"),
                            'ed': end_date.strftime("%Y-%m-%d"), 'r': region})
    logging.info(sql)
    cur.execute(sql)
    list_gar_one_day = cur.fetchall()
    return list_gar_one_day


def fix_null_id(g):
    gid = g[0]
    if g[0] == None:
        gid = random.randint(1000000, 9999999)
    return gid


def get_strend_week(year, week):
    d = date(year, 1, 1)
    d = d - timedelta(d.weekday())
    dlt = timedelta(days=(week - 1) * 7)
    return d + dlt, d + dlt + timedelta(days=6)


def get_all_gar_note(idgar, tmin):
    sql = ''' SELECT DISTINCT ON (tmax_) tmax_
               FROM agz_.gar
               WHERE id_gar = %(id)s AND
               tmin_ = '%(date)s'::timestamp; ''' % {'id': idgar, 'date': tmin}
    cur.execute(sql.replace('None', 'Null'))
    return cur.fetchall()


def expd_gar_list(list_of_gar, str, end, week):
    for one_gar in list_of_gar:
        one_gar_data = get_all_gar_note(one_gar[0], one_gar[1])
        start_data = one_gar[2]
        end_data = datetime.combine(end, datetime.min.time())
        k = classification(one_gar_data, start_data, end_data)
        fn, sn, rn, center = one_gar[3], one_gar[4], one_gar[5], one_gar[6]
        if k[0] == 1:
            area = get_area(k[1], one_gar[0])
        else:
            area = get_area(k[1][0], one_gar[0]) - get_area(k[1][1], one_gar[0])
        push_week(str, end, week, fn, sn, rn, area, center)


def get_subname(region):
    sql = '''
                    SELECT fname_, sname_, rname_
                    FROM agz_.gar WHERE rname_='{region}' LIMIT 1
                    '''.format(**{'region': region})
    cur.execute(sql)
    fn, sn, rn = cur.fetchone()
    return fn, rn, sn


def union_week(c = object):
    delta = timedelta(days=7)
    datebase_connect('localhost')
    #logging.basicConfig(level=logging.NOTSET)
    #logging.getLogger('tipper')

    region_r = get_region_list()
    for region in region_r:
        print region
        wap = get_waring_week(region)
        year_list = [2009, 2010, 2011, 2012, 2013, 2014, 2015]
        for y in year_list:
            for w in wap:
                period = get_strend_week(y, w)
                sd, ed = period[0], period[1]
                gop = get_all_gar_on_period(region, sd, ed)
                if gop:
                    expd_gar_list(gop, sd, ed, w)
                else:
                    # Null week
                    fn, rn, sn = get_subname(region)
                    add_new_week(0, 0, sd, ed, w, fn, sn, rn, 0)


if __name__ == '__main__':
    union_week()
    #print normalization()








