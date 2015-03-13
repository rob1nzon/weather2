#! /usr/bin/python
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from psql import datebase_connect
from datetime import datetime, timedelta, time
import urllib2
import re
from forecast import load_data



def get_url(html):
    soup = BeautifulSoup(html)
    url = soup.findAll('a', href=True, text='Прогноз погоды')
    return url[0].get('href')


def get_page(url='http://rp5.ru/%D0%9F%D0%BE%D0%B3%D0%BE%D0%B4%D0%B0_%D0%B2_%D0%9C%D0%B0%D0%B9%D0%BA%D0%BE%D0%BF%D0%B5'):
    response = urllib2.urlopen(url)
    html = response.read()
    return html


def parse(html):
    soup = BeautifulSoup(html)
    table1 = soup.find('table', id="forecastTable")
    table2 = table1.findAll('tr')
    get_col_m = lambda name: [b.get_text()[:2] for b in a.findAll('span', {"class": name})]
    get_col = lambda name: [b.get_text() for b in a.findAll('div', {"class": name})]
    get_odd_col = lambda name, row: [b.get_text() for b in row[1:]] if name in str(row[0]) else None
    avg = lambda l: sum([int(i) for i in l])/len(l)
    get_colf = lambda name: [avg(re.findall('(\d+)',b.findAll('div')[0].get('onmouseover'))) for b in a.findAll('div', {"class": name})]
    def ifprint(a):
        if a: print a

    def ifget(a):
        if a: return a

    w=[]
    for a in table2:
        row = a.findAll('td')
        w.append(ifget((get_col_m('monthDay'))))
        w.append(ifget(get_odd_col('Местное время', row)))
        w.append(ifget(get_odd_col('Влажность', row)))
        w.append(ifget(get_col('t_0')))
        w.append(ifget(get_col('p_0')))
        w.append(ifget(get_col('pr_0')))
        w.append(ifget(get_colf('cc_0')))
        #w.append(ifget(get_odd_col('Ветер', row)))
        #w.append((get_col('wv_0')))
    return filter(None, w)


def get_forecast(id):
    html = get_page(url='http://rp5.ru/archive.php?wmo_id=%s&lang=ru' % id)
    url = get_url(html)
    #print url
    html = get_page(url='http://rp5.ru' + url.encode('utf8'))
    return parse(html)


def push_to_db(wmid, w, cur):
    cn = lambda x: x if x else 'Null'
    j = 0
    nd = datetime.now().date()
    for i, date in enumerate(w[1]):
        #try:
        #print w
        sql = '''
        INSERT INTO agz_.forecast_weather(
            wmid, date, f_date, temp, pa, vl, ff, n)
        VALUES ({wmid}, '{date}', '{f_date}', {temp}, {pa}, {vl}, {ff}, {n});
        '''.format(**{'wmid': wmid, 'date': nd, 'f_date': datetime.combine(nd + timedelta(days=j), time(hour=int(date))),
                      'temp': w[4][i], 'pa': w[6][i], 'vl': w[7][i], 'ff': 'Null', 'n': w[2][i]})
        print sql
        cur.execute(sql)
        #except:
        #    print w, i, list(len(h) for h in w)
            #html = get_page(url='http://rp5.ru/archive.php?wmo_id=%s&lang=ru' % wmid)
            #url = get_url(html)
            #webbrowser.open('http://rp5.ru/'+url)
        if int(date) > 18:
            j += 1


def get_weather_and_push(id):
    f, conn, cur = datebase_connect('localhost')
    w = get_forecast(id)
    push_to_db(id, w, cur)
    conn.commit()


if __name__ == '__main__':
    global f, conn, cur, w
    f, conn, cur = datebase_connect('localhost')
    cn, wmid_list = load_data(table_name='weather', column_name=['DISTINCT ON(wmid) wmid'], order='wmid', add_filter='wmid is not null')
    #wmid_list = [(23788,)]
    for wmid in wmid_list:
        print wmid[0]
        get_weather_and_push(wmid[0])
