# -*- coding: utf-8 -*-
import cPickle
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from sklearn.externals.six import StringIO
from sklearn.utils import shuffle
from sklearn import tree
from pandas import DataFrame
from psql import datebase_connect
from itertools import combinations
import numpy as np
import pydot
import psycopg2


def get_colum_name(table_name):
    """
    Получить списко столбцов таблицы
    :param table_name:
    :return:
    """
    sql = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='agz_' AND table_name='{tablename}'
    '''.format(**dict(tablename=table_name))
    cur.execute(sql)

    return [a[0] for a in cur.fetchall()]


def fullcomb(iter):
    size = len(iter)
    comb = []
    for a in range(1, size):
        comb+=[(0, 1, 2)+b for b in combinations(iter, a)]
    return comb


def name_col(col, name_cl):
    s = ''
    for a in col:
        s += name_cl[a] + ' '
    return s


def sel_colm(mass, n):
    l = []
    for a in mass:
        temp = []
        for i in n:
            temp.append(a[i])
        l.append(temp)
    return l


def addclass_day(day, region, c):
    sql = '''UPDATE agz_.day_union
     SET pcl={0:s}, region=(SELECT geom FROM agz_."boundary-polygon" WHERE name LIKE '{2:s}%' LIMIT 1)
     WHERE (day = '{1:s}') AND (rname_ ='{2:s}'); '''.format(str(c), str(day), str(region))
    print sql
    cur.execute(sql)


def addclass(day, region, c):
    sql = '''UPDATE agz_.week_union
    SET pcl1={0:s}, region=(SELECT geom FROM agz_."boundary-polygon" WHERE name LIKE '{2:s}%' LIMIT 1)
    WHERE (start_day = '{1:s}') AND (rname_ ='{2:s}'); '''.format(str(c), str(day), str(region))
    #print sql
    cur.execute(sql)


class TreeAndData():
    Tree = tree.DecisionTreeClassifier()
    region = str
    training_set_sample = []
    training_set_value = []


    def create_set(self, day, training_set_s, training_set_v):
        edu_cl, edu_tg, test_cl, test_tg, test_day = [], [], [], [], []
        for i, a in enumerate(training_set_s):
            if day[i][0] < datetime.strptime('2014-01-01', '%Y-%m-%d').date():
                edu_cl.append(a)
                edu_tg.append(training_set_v[i])
            else:
                test_cl.append(a)
                test_tg.append(training_set_v[i])
                test_day.append(day[i][0])
        return edu_cl, edu_tg, test_cl, test_day, test_tg

    def plot_tree(self, clf, reg):
        dot_data = StringIO()
        tree.export_graphviz(clf, out_file=dot_data, feature_names=self.name_c)  #
        graph = pydot.graph_from_dot_data(dot_data.getvalue())
        graph.write_pdf("Week_Tree_" + str(reg).decode(encoding='utf-8') + ".pdf")

    def __init__(self, training_set_s, training_set_v, reg, day, name_c, plot=False, write_class=True):
        edu_cl, edu_tg, test_cl, test_day, test_tg = self.create_set(day, training_set_s, training_set_v)
        clf = self.Tree.fit(edu_cl, edu_tg)
        self.name_c = name_c
        if plot:
            self.plot_tree(clf, reg)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __enter__(self):
        pass


def load_data_loc():
    pkl = open('data.pkl', 'rb')
    return pickle.load(pkl)


def load_data():
    cur.execute('''
    SELECT start_day, rname_, cl,
       pa, ff, n, td, rrr, pa1, ff1, n1, td1,
       rrr1, pa2, ff2, n2, td2, rrr2, pa3, ff3,
       n3, td3, rrr3, delta, delta1, delta2, delta3
    FROM agz_.week_union WHERE temp3 is not null ORDER BY rname_
    ''')
    data = cur.fetchall()
    name_cl = ['start_day', 'rname_', 'cl',
       'pa', 'ff', 'n', 'td', 'rrr', 'pa1', 'ff1', 'n1', 'td1',
       'rrr1', 'pa2', 'ff2', 'n2', 'td2', 'rrr2', 'pa3', 'ff3',
       'n3', 'td3', 'rrr3', 'delta', 'delta1', 'delta2', 'delta3']
    return name_cl, data


def reformat_data(data, num_cl, val_cl):
    """
    Подготовка данных для дерева
    :return:
    """
    data2 = []
    for a in data:
        if a[num_cl] == val_cl:
            data2.append(a)
    return data2


if __name__ == '__main__':
    global conn, cur
    f, conn, cur = datebase_connect('localhost')
    name_cl, cl = load_data()
    region_r = ['Большесельский', 'Борисоглебский', 'Брейтовский',
                'Гаврилов-Ямский', 'Любимский', 'Мышкинский',
                'Некоузский', 'Некрасовский', 'Первомайский', 'Переславский',
                'Пошехонский', 'Ростовский', 'Рыбинский', 'Тутаевский',
                'Ярославский', 'Угличский']
    dcl = {a: i for i, a in enumerate(name_cl)}
    for rd in region_r:
        one_region = reformat_data(cl, 1, rd)
        train_s = sel_colm(one_region, [dcl[a] for a in ['delta', 'delta1', 'delta2', 'delta3']])
        train_v = sel_colm(one_region, [dcl[a] for a in ['cl']])
        train_day = sel_colm(one_region, [dcl[a] for a in ['start_day']])
        TempTree = TreeAndData(train_s, train_v, rd, train_day, name_cl[3:])
        output = cPickle.dumps(TempTree)
        out = []
        pickle.dump(TempTree, out)
        sql =  '''INSERT INTO agz_.tree(region, tree) VALUES ('{region}',
        {data})'''.format(**dict(region=rd, data=psycopg2.Binary(out)))
        cur.execute(sql)
        conn.commit()






