import re
from sklearn.cluster import KMeans
import collections
from psql import datebase_connect
cur = datebase_connect()
cur.execute('SELECT day, fname_, sname_, rname_, nterm, hareasum_, ngar, areasum_\
            FROM agz_.day_union;')
data = cur.fetchall()

cl = []
for a in data:
    cl.append([a[-2], a[-1]])
kmeans = KMeans(init='k-means++', n_clusters=10, n_init=10)
kmeans.fit(cl)

center = list(kmeans.cluster_centers_)

klass = list([a[1], i] for i, a in enumerate(center))
print klass
print '####'
bb = sorted(klass)
lab = [0,0,0,0,0,0,0,0,0,0]
print bb
print '####'
for i,a in enumerate(bb):
    lab[a[1]]=i
print lab

#print collections.OrderedDict(kmeans.cluster_centers_)
#klass = {'%s' % str(a): [float('{:.2}'.format(b[0])), float('{:.2}'.format(b[1]))] for a, b in enumerate(kmeans.cluster_centers_)}
##print klass
#b = list(klass.items())
#print b
#b.sort(key=lambda item: item[1][0])
#print b
#sort_klass = {'%s' % i[0]: i[1] for i in b}
#print sort_klass


for i, a in enumerate(kmeans.labels_):
     cur.execute('''UPDATE agz_.day_union
     SET cl=%s
     WHERE (day = '%s') AND (rname_ ='%s');''' % (lab[a], data[i][0], data[i][3]))
conn.commit()
