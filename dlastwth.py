# -*- coding: utf-8 -*-
import csv
import requests
import urllib2
import logging
import datetime
import MySQLdb
import gzip
from pprint import pprint
import re
import shutil


from StringIO import StringIO

def unk(str):
    #return str[str.find("'"):str.rfind("']")]
    return str[2:str.rfind("']")]


def expnd(base, wm):
    i = [0, 1, 2, 3, 4, 5, 7, 10, 22, 23, 26]
    for l in range(len(base)):
        for a in i:
            print unk(base[l][a])
    srx = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for x in base:
        print x
        g=range(len(srx)+1)
        del g[0]  # костыли дата
        #del g[5]  # облачка
        #del g[-2] # еще
        for j in g:
            if (unk(x[j])!=''):
                if (unk(x[j])!=' '):
                    if (j==5) or (j==6) or (j==10) or (j==11):
                        try:
                            srx[j]=srx[j]+float(re.findall('(\d+)', x[j])[0]) #костыль
                        except:
                            print ':('
                    else:
                        srx[j]=srx[j]+float(unk(x[j])) # складываем
    for j in g:
        try:
            srx[j] = round(srx[j]/len(base)) # среднее значение
        except:
            print ':('

    srx[0] = unk(base[0][0])[:-6]

    add_to_db(wm, srx[0], srx[1], srx[2], srx[3], srx[4], srx[5], srx[6], srx[7], srx[8], srx[9], srx[10])


def add_to_db(wm, date, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg):
    sql = """INSERT INTO weather(wmid, data, temp, pa, pa2, pd, vl, Ff, N, Td, RRR, Tg)
        VALUES ('%(w)s', '%(d)s', '%(t)s','%(p)s','%(p2)s','%(pd)s','%(vl)s','%(Ff)s','%(N)s','%(Td)s','%(RRR)s','%(Tg)s')
        """ % {"w": wm, "d": date, "t": temp, "p": pa, "p2": pa2, "pd": pd, "vl": vl, "Ff":ff, "N":n, "Td":td, "RRR":rrr,"Tg":tg}
    print sql
    cursor.execute(sql)


def load_data(wmid):
    # metar=5001&a_date1=15.06.2014&a_date2=16.06.2014&f_ed3=6&f_ed4=6&f_ed5=15&f_pe=1&f_pe1=3&lng_id=2
    #http://rp5.ru/inc/f_metar.php?

    now_date = datetime.date.today()
    delta = datetime.timedelta(days=1)
    old_date= now_date - delta

    m2 = str(now_date.month)
    d2 = str(now_date.day)
    y2 = str(now_date.year)

    m1 = str(old_date.month)
    d1 = str(old_date.day)
    y1 = str(old_date.year)

    dt1 = d1 + '.' + m1 + '.' + y1
    #dt2 = d2 + '.' + m2 + '.' + y2
    #print dt1,dt2

    data = {
        'a_date1': dt1,
        'a_date2': dt1,
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
        logging.warning('Url error '+url)
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
        #print s
        a = s.find('http://')
        b = s.rfind('csv.gz') + 6
        surl=s[a:b]
        zname=surl[surl.rfind('/')+1:b]
        #print zname
        #urllib.urlretrieve(surl,zname)
        #download(surl)self.ui.plainTextEdit.appendPlainText(s[a:b])
        #print surl
        request = urllib2.Request(surl)
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request)
        buf = StringIO( response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
        #self.ui.plainTextEdit.appendPlainText(_fromUtf8(data))
        cdata = csv.reader(data)
        n = 0

        a = []
        b = []
        y=0
        for i, p in enumerate(cdata):
            s = str(p)
            #print str(p)
            if (s.find(':')>0 and len(s)>5):
                if (y>0):
                    b.append(a)
                #print a
                del(a)
                a = []
                y=1
            if (y==1):
                if (s.rfind(';')<0):
                    a.append(s)
        expnd(b,wmid)

def download(url):
    try:
        file_name = url.split('/')[-1]
        u = urllib2.urlopen(url)
        f = open('data/'+file_name, 'wb')
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
            #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = '#'
            #print status,
        f.close()
    except:
            logging.warning("Url error")


old = ''
#load_data('30692')
## log
logging.basicConfig(filename='testcsv.log', level=logging.DEBUG)
##

db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="weather", charset='utf8')
cursor = db.cursor()
filename = 'C:\Users\USER\unic1.csv'
count=1
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
    print ''
print str(count)
db.commit()
db.close()





