# -*- coding: utf-8 -*-
import psycopg2
import datetime
import StringIO

try:
    conn = psycopg2.connect("""dbname='agz'
                            user='agz'
                            host='10.115.127.11'
                            password='zagaz'""")
except:
    print "I am unable to connect to the database NCUKS"

try:
    conn2 = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the local database"

cur = conn.cursor()
cur2 = conn2.cursor()
print "Get last date in local base"
sql = """ SELECT date_::text FROM agz_.gar ORDER BY date_ DESC LIMIT 1; """
cur2.execute(sql)
results = cur2.fetchall()
old_d = results[0][0]
print old_d

print "Get last date in NCUKS base"
sql = """ SELECT date_::text FROM agz_.f ORDER BY date_ DESC LIMIT 1; """
cur.execute(sql)
results = cur.fetchall()
now_d = results[0][0]
print now_d

# now_date = datetime.date.today()-datetime.timedelta(days=1)
# m1 = str(now_date.month)
# if (len(m1)==1): m1='0'+m1
# d1 = str(now_date.day)
# if (len(d1)==1): d1='0'+d1
# y1 = str(now_date.year)
# now_d =  y1 + '-' + m1 + '-' + d1
if old_d != now_d:
    print "Update data: %(a)s - %(b)s" % {'a': old_d, 'b': now_d}
    sql = """(SELECT fn_, tmin_, tmax_, area_, outline_, center_, fname_, sname_, rname_, forest_, date_, day_
    FROM agz_.f WHERE date_ >= '%(o)s')""" % {'o': old_d}
    print sql
    cur.execute(sql)
    results1 = cur.fetchall()
    data = StringIO.StringIO()

    f=open('temp3.data', 'w')
    cur.copy_to(f, sql, sep="#")
    f.close()

    f=open('temp3.data', 'r')
    #cur.copy_to(f, sql, sep="#")
    for a in f:
        row = a.split('#')
        sql = """ INSERT INTO agz_.gar(
            id_gar, tmin_, tmax_, area_, outline_, center_, fname_, sname_,
            rname_, forest_, date_, day_)
        VALUES ( """
        for b in row:
            sql=sql+"'"+b+"',"
        sql = sql[:-1]+');'
        #print sql
        try:
            cur2.execute(sql.replace("'\N'", 'NULL'))
        except:
            print 'Error'
            print sql
        print '*',
        conn2.commit()
else:
    print "Allright"

#f.close()
#cur2.copy_expert("COPY agz_.term2 FROM STDIN (FORMAT 'csv', DELIMITER '#', NULL '/N', ENCODING 'UTF8')", data)
#conn2.commit



