# -*- coding: utf-8 -*-
print 'Введите абсолютное давление в сосуде или трубопроводе мПа'
P = 1000000*float(raw_input())
print 'Расстояние от места разгерметизации'
R = float(raw_input())
print 'Внешний диаметр трубопровода'
VD = float(raw_input())
print 'Толщина стенки'
WD = float(raw_input())
d = VD - WD*2/1000
print 'Расстояние от места разрыва до конца трубопровода'
l = float(raw_input())
Q = 0.122*d*(P/101325)**0.5
if l <= Q:
    k = 0.5*((1/Q)+1)
else:
    k = 1
i = (420.24*d*(k*(P/101325)**0.5)*(1-0.963*(P/101325)**(-0.1)))/1000
x = 0.96*d*(P/101325)**0.2*(k*2)**0.5

p1 = (1.1625*(P/101325)**0.4)*101.325
p2 = (9.3*(P/101325)**0.4)*101.325
print 'dP', x
print 'i', i
print p1, p2



\copy (SELECT * FROM agz_.s WHERE date_ >= '2009-01-01' AND date_ < '2009-02-01') TO STDOUT WITH DELIMITER ',' CSV HEADER







