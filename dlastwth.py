# -*- coding: utf-8 -*-
import csv
import requests
import urllib2
import logging
import datetime
import MySQLdb
import gzip
import re
import psycopg2

from StringIO import StringIO


def get_last_date_bd():
    sql = '''SELECT data FROM agz_.weather
        ORDER BY weather.data  DESC
        LIMIT 1'''
    cursor.execute(sql)
    results = cursor.fetchall()
    return results[0][0]


def unk(str):
    # return str[str.find("'"):str.rfind("']")]
    return str[2:str.rfind("']")]


def expnda(base, wm):
    def plus(x, s):
        #print x, s
        if (s != ''):
            if (srx[x] != 'NULL'):
                if (srx[x] == 0):
                    srx[x] += float(s)
                else:
                    if (float(s) != 0):
                        srx[x] = (srx[x] + float(s)) / 2
            else:
                srx[x] = float(s)
        else:
            srx[x] = 'NULL'

    global srx
    srx = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    try:
        for tline in base.splitlines(1)[7:]:
            tarr = tline.split(';')
            #print tarr[1], tarr[2], tarr[3], tarr[4], tarr[5], tarr[7], tarr[22], tarr[23], tarr[26]
            plus(0, tarr[1][1:-1])
            plus(1, tarr[2][1:-1])
            plus(2, tarr[3][1:-1])
            plus(3, tarr[4][1:-1])
            plus(4, tarr[5][1:-1])
            plus(5, tarr[7][1:-1])
            plus(7, tarr[22][1:-1])
            try:
                plus(8, re.findall('(\d+.\d+)', tarr[23][1:-1])[0])
            except:
                plus(8, '')
            try:
                plus(9, tarr[26][1:-1])
            except:
                plus(9, '')
            #print 'srx', srx
    except:
        print ':('
    print 'FINAL', srx
    add_to_db(wm, base.splitlines(1)[7:][0].split(';')[0][1:-7],
              srx[0], srx[1], srx[2], srx[3], srx[4],
              srx[5], srx[6], srx[7], srx[8], srx[9])


def add_to_db(wm, date, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg):
    sql = """INSERT INTO agz_.weather(wmid, data, temp, pa, pa2, pd, vl, Ff, N, Td, RRR, Tg)
        VALUES ('%(w)s', '%(d)s', %(t)s,%(p)s,%(p2)s,%(pd)s,%(vl)s,%(Ff)s,%(N)s,%(Td)s,%(RRR)s,%(Tg)s)
        """ % {"w": wm, "d": date, "t": temp, "p": pa, "p2": pa2, "pd": pd, "vl": vl, "Ff": ff, "N": n, "Td": td,
               "RRR": rrr, "Tg": tg}
    print sql
    cursor.execute(sql)


def load_data(wmid):
    # metar=5001&a_date1=15.06.2014&a_date2=16.06.2014&f_ed3=6&f_ed4=6&f_ed5=15&f_pe=1&f_pe1=3&lng_id=2
    # http://rp5.ru/inc/f_metar.php?

    now_date = datetime.date.today()
    delta = datetime.timedelta(days=1)
    old_date = now_date - delta

    m2 = str(now_date.month)
    d2 = str(now_date.day)
    y2 = str(now_date.year)

    m1 = str(old_date.month)
    d1 = str(old_date.day)
    y1 = str(old_date.year)

    dt1 = get_last_date_bd()
    dt2 = d1 + '.' + m1 + '.' + y1
    #dt2 = d2 + '.' + m2 + '.' + y2
    #print dt1,dt2

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
    #print wmid
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
        zname = surl[surl.rfind('/') + 1:b]
        request = urllib2.Request(surl)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request)
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
        tdata = data.decode('utf8')
        expnda(tdata, wmid)


def download(url):
    try:
        file_name = url.split('/')[-1]
        u = urllib2.urlopen(url)
        f = open('data/' + file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print ("Downloading: %s Bytes: %s" % (file_name, file_size))
        file_size_dl = 0
        block_sz = 18192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break
            file_size_dl += len(buffer)
            f.write(buffer)
        f.close()
    except:
        logging.warning("Url error")


old = ''
# load_data('30692')
## log
logging.basicConfig(filename='testcsv.log', level=logging.DEBUG)
##

#db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="weather", charset='utf8')


try:
     db = psycopg2.connect("""dbname='postgis_21_sample'
                             user='postgres'
                             host='127.0.0.1'
                             password='root'""")
except:
    print "I am unable to connect to the database"

cursor = db.cursor()
filename = 'C:\Users\USER\unic1.csv'
count = 1

now_date = datetime.date.today()
delta = datetime.timedelta(days=1)
old_date = now_date - delta
m1 = str(old_date.month)
if (len(m1)==1): m1='0'+m1
d1 = str(old_date.day)
if (len(d1)==1): d1='0'+d1
y1 = str(old_date.year)
old_d =  y1 + '-' + m1 + '-' + d1
print get_last_date_bd()
if (get_last_date_bd() != old_d):
    try:
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if (old != row[1]):
                    #print row[0], row[1]
                    count = count + 1
                    load_data(row[1])
                    old = row[1]
    except:
        print 'Error'
    print str(count)
else:
    print 'Database is relevant...'
db.commit()
db.close()





