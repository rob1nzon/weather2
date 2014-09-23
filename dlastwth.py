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
    return str[2:str.rfind("']")]


def expnd(base):
    i = [0, 1, 2, 3, 4, 5, 7, 10, 22, 23, 26]
    for a in i:
        # if (a==10):
        #     print re.findall('(\d+)', unk(base[0][a]))
        # else:
        print unk(base[0][a])
    srx = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for x in base:
        g=range(len(srx))
        del g[0]
        for j in g:
            srx[j]=srx[j]+int(unk(x[j]))
    for j in g:
        srx[j] = round(srx[j]/len(srx))

    add_to_db(srx[0], srx[1], srx[2], srx[3], srx[4], srx[5])


def add_to_db(date, temp, pa, pa2, pd, vl):
    sql = """INSERT INTO weather(data,temp,pa,pa2,pd,vl)
        VALUES ('%(d)s', '%(t)s','%(p)s','%(p2)s','%(pd)s','%(vl)s')
        """ % {"d": date, "t": temp, "p": pa, "p2": pa2, "pd": pd, "vl": vl}
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
        #TODO Доделать нормальный парсер
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
        expnd(b)
        #print b[0][0]
        #print b
        #
        # for r in cdata: #кривой парсер
        #    s=str(r)
        #    #print s
        #    if (n==10):
        #        vl=s
        #        #print data,temp,pa,pa2,pd,vl
        #        print(data[2:len(data)-2]+"  "
        #              +temp[2:len(temp)-2]+"  "
        #              +pa[2:len(pa)-2]+"  "+pa2[2:len(pa2)-2]+"  "+pd[2:len(pd)-2]+"  "+vl[2:len(vl)-2])
        #        add_to_db(data[2:len(data)-2],
        #                  temp[2:len(temp)-2],
        #                  pa[2:len(pa)-2],
        #                  pa2[2:len(pa2)-2],
        #                  pd[2:len(pd)-2],
        #                  vl[2:len(vl)-2])
        #        #print str(len(s))
        #
        #        n=0
        #    if (n==9):
        #        n=10
        #    if (n==8):
        #        pd=s
        #        n=9
        #    if (n==7):
        #        n=8
        #    if (n==6):
        #        pa2=s
        #        n=7
        #    if (n==5):
        #        n=6
        #    if (n==4):
        #        pa=s
        #        n=5
        #    if (n==3):
        #        n=4
        #    if (n==2):
        #        temp=s
        #        n=3
        #    if (n==1):
        #        n=2
        #
        #    if (s.find(':')>0 and len(s)>5):
        #        data=s
        #        n=1
        # request = urllib2.Request(s[a:b])



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





