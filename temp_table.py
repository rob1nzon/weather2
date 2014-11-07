# -*- coding: utf-8 -*-
import psycopg2

# try:
#     conn = psycopg2.connect("""dbname='agz'
#                             user='agz'
#                             host='10.115.127.11'
#                             password='zagaz'""")
# except:
#     print "I am unable to connect to the database NCUKS"
try:
    conn2 = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the local database"

#cur = conn.cursor()
cur2 = conn2.cursor()

sql = """ SELECT id_gar, center_, rname_ FROM agz_.gar WHERE sname_ LIKE 'Ярос%' """
cur2.execute(sql)
results = cur2.fetchall()

f = open('test2.csv', 'w')

for row in results:
    #print row
    sql2= """ SELECT id, ST_ASText(loc), region, st_distance_sphere(loc, '%(p)s'::geometry) FROM agz_.mstations
          ORDER BY loc <-> '%(p)s'::geometry
          LIMIT 1; """ % {'p':row[1]}
    cur2.execute(sql2)
    results2 = cur2.fetchall()
    #print results2[0]
    sql3 = """ SELECT pplace_, rname_, point_,st_distance_sphere(point_, '%(p)s'::geometry)
      FROM agz_.pp ORDER BY point_ <-> '%(p)s'::geometry
      LIMIT 1; """ % {'p':row[1]}
    cur2.execute(sql3)
    results3 = cur2.fetchall()
    #print results3[0]
    s=''
    for a in row: s+=str(a)+';'
    for a in results2[0]: s+=str(a)+';'
    for a in results3[0]: s+=str(a)+';'
    f.write(s+'\n')
    #print '1'
    #print s
    #a.writerows([row, results2[0], results3[0]])