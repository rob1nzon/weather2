# -*- coding: utf-8 -*-
import requests
import time
import bs4
from psql import datebase_connect
from random import random

def check(id):
    f, conn, cur = datebase_connect('localhost')
    sql = """
    SELECT count(id)
  FROM agz_.mstations2 WHERE id=%s;""" % id
    cur.execute(sql)
    return cur.fetchone()




if __name__ == '__main__':
    f, conn, cur = datebase_connect('localhost')
    ny = int(random()*1000)
    for zif in range(ny,ny+40):
        print zif
        try:
            a = requests.get('http://rp5.ru/jsonwmo.php?q=%s&limit=500&timestamp=%s' % (zif, int(time.time())))
            clist = a.json()
            #print clist
            for c in clist:
                sid = c["id"]
                if check(sid)[0] == 0:
                    n = c["name"]
                    a = lambda s: s.rfind(u" в ") if s.rfind(u" в ") > 0 else s.rfind(u"на")
                    name = n[n.find(" ")+1:a(n)]
                    b = requests.get("http://rp5.ru/jsonsearch.php?q=%s&limit=500&timestamp=%s" % (name, int(time.time())))
                    z = b.json()
                    try:
                        cntr_name = z[0]["name"].split(", ")[1]
                        rgn_name = z[0]["name"].split(", ")[2]
                        ryn_name = z[0]["name"].split(", ")[3]
                    except:
                        ryn_name = rgn_name
                    #print z[0]["namealt"]
                    m = requests.get("http://rp5.ru/"+ z[0]["namealt"])
                    soup = bs4.BeautifulSoup(m.text)
                    a = soup.find_all('div', class_="pointNaviCont")
                    cord = str(a)[str(a).rfind('show_map')+9:str(a).rfind(')" style="cursor')]
                    ccord = cord.split(', ')
                    make_wkb = "ST_GeomFromText('POINT(%s %s)',4326)" % (ccord[0], ccord[1])
                    #print sid, make_wkb, cntr_name, ryn_name, rgn_name
                    sql = u"""INSERT INTO agz_.mstations2(
                            id, loc, country, state, region)
                            VALUES ({id:s}, {loc:s}, '{co:s}', '{st:s}', '{rg:s}');
                            """.format(**{'id': sid,
                                          'loc': make_wkb,
                                          'co': cntr_name,
                                          'st': rgn_name,
                                          'rg': ryn_name})
                    #print sql
                    print name, sid
                    try:
                        cur.execute(sql)
                        conn.commit()
                    except:
                        print "Дубль"
        except:
            pass





