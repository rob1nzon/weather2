# -*- coding: utf-8 -*-
import psycopg2



def datebase_connect(host):
    '''
    Возвращает объекты для доступа к базе
    Name of host loaclhost or ncuks :param host:
    :return: True, conn, cur
    '''
    if host == 'localhost':
        try:
            conn = psycopg2.connect("""dbname='postgis_21_sample'
                                    user='postgres'
                                    host='127.0.0.1'
                                    password='root'""")
        except:
            return False, 0, 0
        cur = conn.cursor()
        return True, conn, cur
    elif host == 'ncuks':
        try:
            conn = psycopg2.connect("""dbname='agz'
                            user='agz'
                            host='10.115.127.11'
                            password='zagaz'""")
        except:
            return False, 0, 0
        cur = conn.cursor()
        return True, conn, cur


def load_data(table_name='week_union_norm', column_name=['start_day', 'rname_', 'cl',
       'temp', 'pa', 'ff', 'n', 'td', 'rrr', 'pa1', 'ff1', 'n1', 'td1',
       'rrr1', 'pa2', 'ff2', 'n2', 'td2', 'rrr2', 'pa3', 'ff3',
       'n3', 'td3', 'rrr3', 'delta', 'delta1', 'delta2', 'delta3', 'now'], order_b=True, add_filter='delta is not null AND delta3 is not null', order='rname_'):
    """
    Загрузить данные из БД
    :return:
    """
    f, conn, cur = datebase_connect('localhost')
    if order_b:
        or_by = 'ORDER BY'
    else:
        or_by = ''
    cur.execute('''
    SELECT {col_name}
    FROM agz_.{table_name} WHERE {add_filter} {or_by} {order}
    '''.format(**{'col_name': ', '.join(column_name), 'table_name': table_name, 'add_filter': add_filter, 'order': order, 'or_by': or_by}))
    data = cur.fetchall()
    conn.close()
    return column_name, data


def get_colum_name(table_name):
    """
    Получить список столбцов таблицы
    :param table_name:
    :return:
    """
    f, conn, cur = datebase_connect('localhost')
    sql = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='agz_' AND table_name='{tablename}'
    '''.format(**dict(tablename=table_name))
    cur.execute(sql)

    return [a[0] for a in cur.fetchall()]


def sel_colm(mass, n):
    l = []
    for a in mass:
        temp = []
        for i in n:
            temp.append(a[i])
        l.append(temp)
    return l


def get_region_list():
    f, conn, cur = datebase_connect('localhost')
    sql = '''
    SELECT DISTINCT ON(rname_) rname_ FROM agz_.waring_week
    '''
    cur.execute(sql)
    return [a[0] for a in cur.fetchall()]