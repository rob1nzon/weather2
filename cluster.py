# -*- coding: utf-8 -*-
import re
from sklearn.cluster import KMeans
import collections
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


def get_data_bd_day():
    """
    Получение данных из БД
    :return: data
    """
    cur.execute('SELECT day, fname_, sname_, rname_, nterm, hareasum_, areasum_\
                FROM agz_.day_union WHERE  (areasum_<21002390)')
    return cur.fetchall()
    cur.execute('UPDATE agz_.week_union   SET cl=10 WHERE areasum_>=21002390')

def get_data_bd():
    """
    Получение данных из БД
    :return: data
    """
    cur.execute('SELECT start_day, fname_, sname_, rname_, areasum_\
                FROM agz_.week_union WHERE  (areasum_<21002390)')
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

def insert_data_bd(data, kmeans):
    """
    Загрузка классов в БД
    :param data:
    :param lab:
    :param kmeans:
    :return:
    """
    for i, a in enumerate(kmeans.labels_):
        cur.execute('''UPDATE agz_.week_union
         SET cl=%s
         WHERE (start_day = '%s') AND (rname_ ='%s');''' % (a, data[i][0], data[i][3]))
    conn.commit()




if __name__ == '__main__':
    connect_db()
    data = get_data_bd()
    cl = []
    for a in data:
        cl.append([a[-1], 0])
    kmeans = MyKMeans(init='k-means++', n_clusters=10, n_init=10)
    kmeans.sortfit(cl)

    insert_data_bd(data, kmeans)

    #print collections.OrderedDict(kmeans.cluster_centers_)
    #klass = {'%s' % str(a): [float('{:.2}'.format(b[0])), float('{:.2}'.format(b[1]))] for a, b in enumerate(kmeans.cluster_centers_)}
    ##print klass
    #b = list(klass.items())
    #print b
    #b.sort(key=lambda item: item[1][0])
    #print b
    #sort_klass = {'%s' % i[0]: i[1] for i in b}
    #print sort_klass



