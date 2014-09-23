# -*- coding: utf-8 -*-
import csv
import requests
import urllib2
import logging
import datetime


def load_data(wmid):
    # metar=5001&a_date1=15.06.2014&a_date2=16.06.2014&f_ed3=6&f_ed4=6&f_ed5=15&f_pe=1&f_pe1=3&lng_id=2
    #http://rp5.ru/inc/f_metar.php?

    y1 = '2005'
    m1 = '01'
    d1 = '01'

    now_date = datetime.date.today()
    cur_month = now_date.month # Месяц текущий
    cur_day = now_date.day # День текущий
    cur_year = now_date.year

    y2 = str(cur_year)
    m2 = str(cur_month)#'09'
    d2 = str(cur_day)#'2014'

    dt1 = d1 + '.' + m1 + '.' + y1
    dt2 = d2 + '.' + m2 + '.' + y2
    #print dt1,dt2

    data = {
        'a_date1': dt1,
        'a_date2': dt2,
        #'f_ed0': d2,
        #'f_ed1': m2,
        #'f_ed2': y2,
        'f_ed3': '09',#str(cur_month),
        'f_ed4': '09',#str(cur_month),
        'f_ed5': '15',#str(cur_day),
        #'f_ed6': d1,
        #'f_ed7': m1,
        #'f_ed8': y1,
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
        download(surl)
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
    print 'h'
print str(count)







