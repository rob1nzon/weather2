# -*- coding: utf-8 -*-
import cv2
from numpy import *
import psycopg2

conn = psycopg2.connect("""dbname='postgis_21_sample'
                                    user='postgres'
                                    host='127.0.0.1'
                                    password='root'""")
cur = conn.cursor()

def get_data():
    cur.execute("""SELECT now, t, tmax, taday, po, u, umin, ff, n, td, rrr, t1, tmax1, taday1,
       po1, u1, umin1, ff1, n1, td1, rrr1, t2, tmax2, taday2, po2, u2,
       umin2, ff2, n2, td2, rrr2, t3, tmax3, taday3, po3, u3, umin3,
       ff3, n3, td3, rrr3, delta, delta1, delta2, delta3
  FROM agz_.week_union_norm WHERE ngar>0 LIMIT 100;""")
    return cur.fetchall()

def get_label():
    cur.execute("""SELECT ngar FROM agz_.week_union_norm WHERE ngar>0 LIMIT 100;""")
    return cur.fetchall()

if __name__ == '__main__':
    a = array(get_data(), dtype=float32)
    b = array(get_label(), dtype=float32)

    tree = cv2.DTree()
    print tree.train(a, 1, b)
    print 'H'
    c = empty((100, 2), dtype=float32)
    d = zeros((100, 1), dtype=float32)

    for i in range(100):
        print b[i], tree.predict(a[i])

