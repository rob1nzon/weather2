# -*- coding: utf-8 -*-
import re
from sklearn.cluster import KMeans
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
                FROM agz_.day_union ')
    #cur.execute('UPDATE agz_.week_union SET cl=10 WHERE areasum_>=21002390')
    #conn.commit()
    return cur.fetchall()


def get_data_bd():
    """
    Получение данных из БД
    :return: data
    """
    cur.execute('SELECT start_day, fname_, sname_, rname_, areasum_\
                FROM agz_.week_union_norm WHERE areasum_ > 0')
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
         SET cl=%s+1, pcl=999
         WHERE (start_day = '%s') AND (rname_ ='%s') AND (sname_ = '%s');''' % (a, data[i][0], data[i][3], data[i][2]))
    conn.commit()

def set_null_fire():
    cur.execute('''
    UPDATE agz_.week_union_norm
    SET cl=0
    WHERE areasum_ = 0''')
    conn.commit()


if __name__ == '__main__':
    connect_db()
    data = get_data_bd()
    cl = []
    for a in data:
        cl.append([a[-1], 0])
    kmeans = MyKMeans(init='k-means++', n_clusters=10)
    kmeans.sortfit(cl)
    print kmeans.cluster_centers_, kmeans.inertia_

    insert_data_bd(data, kmeans)
    set_null_fire()



