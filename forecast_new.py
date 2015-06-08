# -*- coding: utf-8 -*-
import cv2
from numpy import *
import datetime
from psql import datebase_connect, load_data, sel_colm, get_region_list

def addclass(day, region, c):
    f, conn, cur = datebase_connect('localhost')
    sql = '''UPDATE agz_.week_union_norm
    SET pcl={0:s}, region=(SELECT geom FROM agz_."boundary-polygon" WHERE name LIKE '{2:s}%' LIMIT 1)
    WHERE (start_day = '{1:s}') AND (rname_ ='{2:s}'); '''.format(str(c), str(day), str(region))
    #print sql
    cur.execute(sql)
    conn.commit()



def forecast(show_prog=False, factor=['delta', 'delta1', 'delta2', 'delta3']):
    region_r = get_region_list()
    for rd in region_r:
        try:
            print rd
            tree = cv2.DTree()
            tree.load('./tree/'+str(rd).decode(encoding='utf-8').encode(encoding='cp1251')+'.tree')
            date = load_data(column_name=['start_day'], add_filter="rname_ = '%s' AND start_day > '2014-01-01'" % rd)[1]
            target = array(load_data(column_name=factor, add_filter="rname_ = '%s'  AND start_day > '2014-01-01'" % rd)[1], dtype=float32)
            for i, t in enumerate(target):
                addclass(date[i][0].strftime('%Y-%m-%d'), rd, tree.predict(t))
        except:
            print 'Fail'



if __name__ == '__main__':
    min_sample = 4
    forecast(factor=["now", "t", "tmax", "taday", "po", "u", "umin", "ff", "n", "td", "rrr", "t1", "tmax1", "taday1", "po1", "u1", "umin1", "ff1", "n1", "td1", "rrr1", "t2", "tmax2", "taday2", "po2", "u2", "umin2", "ff2", "n2", "td2", "rrr2", "t3", "tmax3", "taday3", "po3", "u3", "umin3", "ff3", "n3", "td3", "rrr3", "delta", "delta1", "delta2", "delta3"])

