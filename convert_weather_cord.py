import psycopg2
try:
    conn = psycopg2.connect("""dbname='postgis_21_sample'
                            user='postgres'
                            host='127.0.0.1'
                            password='root'""")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
sql = '''SELECT * FROM agz_.meteostations;'''
cur.execute(sql)
results = cur.fetchall()
for ro in results:
    sql2= ''' INSERT INTO agz_.mstations(
            id, loc, country, state, region)
    VALUES (%(id)i, ST_GeomFromText('POINT(%(lo1)s %(lo2)s)') , '%(con)s', '%(st)s', '%(r)s') ''' % {'id': ro[0], 'lo1': ro[2], 'lo2': ro[1], 'con': ro[3], 'st': ro[4], 'r': ro[5]}
    cur.execute(sql2)
conn.commit()
conn.close()