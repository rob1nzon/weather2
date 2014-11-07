# -*- coding: utf-8 -*-
import math
import psycopg2
conn = psycopg2.connect("""dbname='postgis_21_sample' user='postgres' host='127.0.0.1' password='root'""")

rad = 6372795

llat1 = 56.7084
llong1 = 38.8966

llat2 = 38.8669
llong2 = 56.6895

#в радианах
lat1 = llat1*math.pi/180.
lat2 = llat2*math.pi/180.
long1 = llong1*math.pi/180.
long2 = llong2*math.pi/180.

cl1 = math.cos(lat1)
cl2 = math.cos(lat2)
sl1 = math.sin(lat1)
sl2 = math.sin(lat2)
delta = long2 - long1
cdelta = math.cos(delta)
sdelta = math.sin(delta)

#вычисления длины большого круга
y = math.sqrt(math.pow(cl2*sdelta,2)+math.pow(cl1*sl2-sl1*cl2*cdelta,2))
x = sl1*sl2+cl1*cl2*cdelta
ad = math.atan2(y,x)
dist = ad*rad

#вычисление начального азимута
x = (cl1*sl2) - (sl1*cl2*cdelta)
y = sdelta*cl2
z = math.degrees(math.atan(-y/x))

if (x < 0):
    z = z+180.

z2 = (z+180.) % 360. - 180.
z2 = - math.radians(z2)
anglerad2 = z2 - ((2*math.pi)*math.floor((z2/(2*math.pi))) )
angledeg = (anglerad2*180.)/math.pi

print 'Distance >> %.0f' % dist, ' [meters]'
print 'Initial bearing >> ', angledeg, '[degrees]'


print "По формуле"
d = math.acos(math.sin(lat1)*math.sin(lat2)+math.cos(lat1)*math.cos(lat2)*math.cos(long2-long1))
print d*111.1

print "ST_Distance"
cur = conn.cursor()
sql = '''SELECT ST_Distance('POINT(%(o1)s %(a1)s)'::geometry,'POINT(%(o2)s %(a2)s)'::geometry);''' % {'a1': llat1, 'o1': llong1, 'a2': llat2, 'o2': llong2}
cur.execute(sql)
print cur.fetchall()[0][0]

print "ST_Distance_Sphere"
sql = '''SELECT ST_Distance_Sphere('POINT(%(a1)s %(o1)s)'::geometry,'POINT(%(a2)s %(o2)s)'::geometry);''' % {'a1': llat1, 'o1': llong1, 'a2': llat2, 'o2': llong2}
cur.execute(sql)
print cur.fetchall()[0][0]
