import Pycluster as pc
import psycopg2
import numpy
try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
cur.execute('SELECT id_gar, tmin_, tmax_, fname_, sname_, rname_, nterm, hareasum_\
 FROM agz_.temp_union;');
data = cur.fetchall()

cl=[]
for a in data:
    cl.append([a[-2], a[-1]])

labels, error, nfound = pc.kcluster(cl, 10)
centroids, _ = pc.clustercentroids(cl, clusterid=labels)
print labels, centroids
for i,a in enumerate(labels):
    cur.execute('''UPDATE agz_.temp_union
    SET cl=%s
    WHERE id_gar=%s;''' % (a, data[i][0]))
conn.commit()
