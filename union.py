# -*- coding: utf-8 -*-
import psycopg2
import time

try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
sql = """ SELECT id_gar, to_char(tmin_,'YYYY-MM-DD'), area_, outline_, center_, fname_, sname_,
       rname_, forest_, day_
       FROM agz_.gar WHERE sname_ LIKE 'Ярос%'"""
cur.execute(sql)
results1 = cur.fetchall()
print results1[0][1]
for term in results1:
    sql3 = """SELECT wmid, temp, pa, pa2, pd, vl, ff, n, td, rrr, tg,
      (SELECT ST_Distance_Sphere(loc,'{p:s}'::geometry) FROM agz_.mstations
      ORDER BY loc <-> '{p:s}'::geometry LIMIT 1)
      FROM agz_.weather WHERE (wmid = (SELECT id FROM agz_.mstations
      ORDER BY loc <-> '{p:s}'::geometry
      LIMIT 1)) AND (data::text LIKE '{d:s}'); """.format(**{'p': term[4], 'd': str(term[1])[:10]}) # получаем данные по погоде
    #print sql3
    cur.execute(sql3)
    results3 = cur.fetchall()
    if (len(results3)<1):
        print 'NO WEATHER DATA'
        print sql3  # ToDo: если нету то докачать
    else:
        ssql = "SELECT COUNT(tmin_), id_gar FROM agz_.union WHERE to_char(tmin_,'YYYY-MM-DD') LIKE '{d:s}' GROUP BY id_gar;".format(**{'d': str(term[1])[0:10]})
        cur.execute(ssql)
        tsq = cur.fetchall()
        if (tsq[0][0]==0): # если таких записей нет то добавляем новую
            icql = """INSERT INTO agz_."union"(
                id_gar, tmin_, area_, fname_, sname_,
                rname_, forest_, day_, wmid, temp, pa, pa2, pd,
                vl, ff, n, td, rrr, tg, dist, harea_, termharea_, countterm)
            SELECT """
            try:
                i = 0
                for b in term:
                    i += 1
                    if (i != 5) and (i != 6): # выпиливаю координаты
                        icql = icql+"'"+str(b)+"',"
                for b in results3[0]: icql=icql+"'"+str(b)+"',"
                icql = icql+"AVG(harea_),AVG(area_),COUNT(area_) FROM agz_.term WHERE (fn_=%(f)s) AND (data::text LIKE)'{d:s}') ;" % {'f':term[0], 'd': str(term[1])[:10]}
                cur.execute(icql.replace("'None'", 'NULL'))
                #print icql.replace("'None'", 'NULL')
            except:
                print 'Error'
                print icql
            print '*',
            conn.commit()
        else: # если есть то обновляем данные
            if (tsq[0][1] != term[0]): #Todo: разобраться с запросом
                icql = """UPDATE agz_."union" SET
                    area_=area+%s, temp=(temp+%s)/2, pa=(pa+%)s)/2,
                    pa2=(pa2+%)s)/2, pd=(pd+%)s)/2,  vl=(vl+%s)/2,
                    ff=(ff+%s)/2, n=(n+%s)/2, td=(td+%)s)/2,
                    rrr=(rrr+%s)/2, tg=(tg+%s)/2,
                    dist=(dist+%s)/2, harea_=harea_+%s,
                    termharea_=termharea_+%s, countterm=countterm+%s
                    WHERE id_gar=%s;""" % tuple[term[2]+[b for b in results3[0]]+['1', '1', '1', '1']]
                print icql


        #print term[0],term[1],term[2],term[3],term[4],term[5],term[6],term[7],term[8],term[9],term[10],term[11]

