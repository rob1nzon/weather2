import Pycluster as pc
import psycopg2
import re
from sklearn.cluster import KMeans
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

cl = []
#for a in data:
#    cl.append([a[-2], a[-1]])


print "#####################"

f=open('data.csv')

for s in f:
    print s.split(';')[0],re.findall('(\d+.\d+)',s.split(';')[1].replace(',', '.'))[0]
    cl.append ([float(s.split(';')[0]), float(re.findall('(\d+.\d+)',s.split(';')[1])[0].replace(',', '.'))])

kmeans = KMeans(init='k-means++', n_clusters=10, n_init=10)
kmeans.fit(cl)
print kmeans.labels_
print 'claster'
f = open ('two.csv','w')
for i,j in enumerate(kmeans.labels_):
    print i,j
    f.write(str(i+1)+';'+str(j)+'\n')

#labels, error, nfound = pc.kcluster(cl, 10)
#centroids, _ = pc.clustercentroids(cl, clusterid=labels)
# print labels, centroids
# for i,a in enumerate(labels):
#     cur.execute('''UPDATE agz_.temp_union
#     SET cl=%s
#     WHERE id_gar=%s;''' % (a, data[i][0]))
# conn.commit()
