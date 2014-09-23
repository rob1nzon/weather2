# -*- coding: utf-8 -*-
import MySQLdb

db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="weather", charset='utf8')
cursor = db.cursor()

# ds = '"14.09.2014 19:00";"10.0";"711.0";"745.6";"1.4";"75";"Ветер, дующий с западо-юго-запада";"3";"";"";"";" ";"";"";"";"";"";"";"";"";"";"";"5.7";"";"";"";"";"";"";'
# s=str.split(ds,';')
# print s[0],s[1],s[2]

str='\xd0\x9e\xd0\xb1\xd0\xbb\xd0\xb0\xd0\xba\xd0\xbe\xd0\xb2 \xd0\xbd\xd0\xb5\xd1\x82.'

sql = """INSERT INTO weather(data,temp)
        VALUES ('1253', '345', '333' ,'333')
        """
cursor.execute(sql)
db.commit()
db.close()