# -*- coding: utf-8 -*-
import re
from sklearn.cluster import KMeans
from sklearn.svm import OneClassSVM
from psql import datebase_connect


class MyKMeans(KMeans):
    def sortfit(self, cl):
        """
        Кластеризация с сортировкой по увеличению значения
        :param cl:
        :return:
        """
        self.fit(cl)
        center = list(self.cluster_centers_)
        klass = list([a[0], i] for i, a in enumerate(center))
        bb = sorted(klass)
        lab = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        for i, a in enumerate(bb):
            lab[a[1]] = i
        for i, a in enumerate(self.labels_):
            self.labels_[i] = lab[a]


def connect_db():
    global f, conn, cur
    f, conn, cur = datebase_connect('localhost')


def get_data_bd_day(area=False):
    """
    Получение данных из БД
    :return: data
    """
    if area:
        cur.execute('SELECT day, fname_, sname_, rname_, nterm, hareasum_, areasum_ FROM agz_.day_union ')
    else:
        cur.execute('SELECT day, fname_, sname_, rname_, nterm, hareasum_, ngar FROM agz_.day_union ')
    #cur.execute('UPDATE agz_.week_union SET cl=10 WHERE areasum_>=21002390')
    #conn.commit()
    return cur.fetchall()


def get_data_bd():
    """
    Получение данных из БД
    :return: data
    """
    cur.execute('SELECT start_day, fname_, sname_, rname_, areasum_\
                FROM agz_.week_union_norm where areasum_ > 0')
    return cur.fetchall()


def insert_data_bd_day(data, kmeans):
    """
    Загрузка классов в БД
    :param data:
    :param lab:
    :param kmeans:
    :return:
    """
    for i, a in enumerate(kmeans.labels_):
        cur.execute('''UPDATE agz_.day_union
         SET cl=%s
         WHERE (day = '%s') AND (rname_ ='%s');''' % (a, data[i][0], data[i][3]))
    conn.commit()

def insert_data_t(data, kmeans):
    for i, a in enumerate(kmeans.labels_):
        print '%s %s %s' % (data[i][0], data[i][3], a)


def insert_data_bd(data, kmeans):
    """
    Загрузка классов в БД
    :param data:
    :param lab:
    :param kmeans:
    :return:
    """
    for i, a in enumerate(kmeans.labels_):
        cur.execute('''UPDATE agz_.week_union_norm
         SET cl=%s+1
         WHERE (start_day = '%s') AND (rname_ ='%s') AND (sname_ = '%s');''' % (a, data[i][0], data[i][3], data[i][2]))
    conn.commit()

def set_null_fire(field='areasum_'):
    cur.execute('''
    UPDATE agz_.week_union_norm
    SET cl=0
    WHERE {field} = 0'''.format(**{'field': field}))
    conn.commit()
    cur.execute('''
    UPDATE agz_.week_union_norm
    SET cl=10
    WHERE areasum_ > 300000''')
    conn.commit()


if __name__ == '__main__':
    connect_db()
    data = get_data_bd()
    cl = []
    for a in data:
        cl.append([a[-1]])

    #clf = OneClassSVM(kernel="linear")
    #clf.fit(cl)
    #import matplotlib.pyplot as plt
    #y = clf.fit_status_
    #print clf.C
    #x = clf.decision_function(cl).ravel()
    #print max(x)
    #n_cl, n_cl2 = [], []
    #for i, a in enumerate(x):
    #    if a > 0:
    #        n_cl.append(cl[i][0])
    #        n_cl2.append(0)
    #    else:
    #        n_cl2.append(cl[i][0])
    #        n_cl.append(0)
    #print n_cl, n_cl2
    #plt.plot(n_cl, 'r')
    #plt.plot(n_cl2, 'g')
    #plt.show()

    kmeans = MyKMeans(init='k-means++', n_clusters=10)
    kmeans.sortfit(cl)
    print kmeans.cluster_centers_, kmeans.inertia_
    insert_data_bd(data, kmeans)
    set_null_fire()



