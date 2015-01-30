# -*- coding: utf-8 -*-
import psycopg2
def datebase_connect(host):
    '''
    Возвращает объекты для доступа к базе
    Name of host loaclhost or ncuks :param host:
    True, conn, cur :return:
    '''
    if host == 'localhost':
        try:
            conn = psycopg2.connect("""dbname='postgis_21_sample'
                                    user='postgres'
                                    host='127.0.0.1'
                                    password='root'""")
        except:
            print 'Ошибка доступа к БД'
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
            print 'Ошибка доступа к БД НЦУКСА'
            return False, 0, 0

        cur = conn.cursor()
        return True, conn, cur

