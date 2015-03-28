#! /usr/bin/python
# -*- coding: utf-8 -*-
from sklearn import tree
import csv

def read_file(fn, test=True):
    f = open(fn)
    data = csv.reader(f, delimiter=';')
    headers = data.next()
    dt, tg = [], []
    tof = lambda dt: [float(a) for a in dt]
    for a in data:
        if test:
            tg.append(float(a[0]))
            dt.append(tof(a[1:]))
        else:
            dt.append(tof(a))
            tg = None
    if tg is not None:
        return tg, dt, headers
    else:
        return dt


def write_file(data, data3, data2=[]):
    f = open('calc.csv', 'wb')
    c = csv.writer(f, delimiter=';')
    for i, a in enumerate(data):
        c.writerow([str(a).replace('.', ',')] + data2 + data3[i])
    f.close()


if __name__ == '__main__':
    target_edu, data_edu, headers = read_file(fn='edu.csv')

    r = raw_input('Режим работы \n 1 - тест \n 2 - прогноз \n')
    if r == '2':
        test_dt = read_file(fn='pred.csv', test=False)
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(data_edu, target_edu)
        write_file(clf.predict(test_dt), test_dt)
    elif r == '1':
        t_test, data_test, headers = read_file(fn='test.csv')
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(data_test, t_test)
        write_file(clf.predict(data_test), data_test, data2=t_test)
