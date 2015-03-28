# -*- coding: utf-8 -*-
import urllib2
import logging
import datetime
import gzip
import re
import csv
from StringIO import StringIO
import requests


from psql import datebase_connect


def expnda(base, wm):
    head = csv.DictReader(base.splitlines(1)[6:7], delimiter=';').fieldnames
    head[0] = 'data'
    head[-2] = 'Ed'
    dt = csv.DictReader(base.splitlines(1)[7:], delimiter=';', fieldnames=head)
    sql = '''INSERT INTO agz_.new_weather(wmid, {name})
    VALUES ({wmid}, %({val})s)   '''.format(**{'name': ', '.join(head), 'wmid': wm, 'val': ')s, %('.join(head)})
    #print dt
    try:
        cursor.executemany(sql, dt)
    except:
        pass


def load_data(wmid, gdate):
    """
    Загрузка архива погоды с сайта
    :param wmid: id метеостанции
    :param gdate:
    :return:
    """
    # metar=5001&a_date1=15.06.2014&a_date2=16.06.2014&f_ed3=6&f_ed4=6&f_ed5=15&f_pe=1&f_pe1=3&lng_id=2
    # http://rp5.ru/inc/f_metar.php?

    now_date = datetime.date.today()
    delta = datetime.timedelta(days=1)
    old_date = now_date - delta

    m2 = str(now_date.month)
    d2 = str(now_date.day)
    y2 = str(now_date.year)

    dt1 = gdate[0].strftime('%Y.%m.%d')
    print dt1
    #dt2 = d1 + '.' + m1 + '.' + y1
    dt2 = d2 + '.0' + m2 + '.' + y2
    print dt1, dt2
    data = {
        'a_date1': dt1,
        'a_date2': dt2,
        'f_ed3': m2,
        'f_ed4': m2,
        'f_ed5': d2,
        'f_pe': '1',
        'f_pe1': '2',
        'lng_id': '2',
        'wmo_id': str(wmid)
    }
    url = 'http://rp5.ru/inc/f_archive.php'
    logging.info(wmid)
    try:
        r = requests.post(url, data)
    except ValueError:
        logging.warning('Url error ' + url)
    except requests.exceptions.ConnectionError:
        scs = True
        while scs:
            try:
                r = requests.post(url, data)
            except:
                logging.info('Retry...')
            else:
                scs = False

    else:
        s = r.text
        a = s.find('http://')
        b = s.rfind('csv.gz') + 6
        surl = s[a:b]
        print surl
        zname = surl[surl.rfind('/') + 1:b]
        request = urllib2.Request(surl)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request)
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
        tdata = data.decode('utf8')
        expnda(data, wmid)
        db.commit()


def last_date_id(wmid):
    sql = 'SELECT data FROM agz_.new_weather WHERE wmid = {wn} ORDER BY data DESC LIMIT 1'.format(**{'wn': wmid})
    cursor.execute(sql)
    return cursor.fetchone()


def update_weather():
    global f, db, cursor
    f, db, cursor = datebase_connect('localhost')
    now_date = datetime.date.today()
    sqlid = """SELECT DISTINCT ON (id) id
                 FROM agz_.mstations
                 WHERE state LIKE 'Красноя%' """  # Только Ярославская область
    cursor.execute(sqlid)
    results = cursor.fetchall()
    for row in results:
        print row[0]
        date_bd = last_date_id(row[0])
        print date_bd
        try:
            load_data(row[0], date_bd)
        except:
            pass
    db.commit()
    db.close()


if __name__ == '__main__':
    update_weather()





