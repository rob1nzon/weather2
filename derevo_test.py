# -*- coding: utf-8 -*-
import pickle
import copy
import matplotlib.pyplot as plt
from sklearn.externals.six import StringIO
from sklearn import tree
from pandas import DataFrame
from psql import datebase_connect
import pydot



def addtocsv(day, region, c, r):
    #cur.execute('''SELECT count(*) FROM agz_.day_union WHERE (day = '%s') AND (rname_ ='%s');''' % (day, region))
    #print cur.fetchone()
    goold.append[day, region, c, r]


def combinations(target, data):
    for i in range(len(data)):
        new_target = copy.copy(target)
        new_data = copy.copy(data)
        new_target.append(data[i])
        new_data = data[i+1:]
        combinations(new_target,
                     new_data)

class TreeAndData():
    Tree = tree.DecisionTreeClassifier()
    region = str
    training_set_sample = []
    training_set_value = []
    score = int

    def __init__(self, training_set_s, training_set_v, reg, day, deep):
        self.training_set_sample = training_set_s
        self.training_set_value = training_set_v
        self.region = reg

        ct = int(len(training_set_s) // coeff)
        tt = len(training_set_v) - int(len(training_set_v) // coeff)
        edu_cl = training_set_s[:ct]
        edu_tg = training_set_v[:ct]
        test_cl = training_set_s[ct:]
        test_tg = training_set_v[ct:]
        test_day = day[ct:]
        #print test_day
        if deep != 0:
            self.Tree.set_params(max_depth=deep)

        clf = self.Tree.fit(edu_cl, edu_tg)

        dot_data = StringIO()

        a = self.Tree.predict(test_cl)
        c = float(0)


        for i, b in enumerate(test_tg):
            c += (abs(b-a[i]))
            goold.append([test_day[i], reg, a[i], b])
            #print str(day[b])+' '+str(a[i])

            #addclass(test_day[i], reg, a[i])
        print ' ', float(c) / 9
        #print float(c) , (len(test_tg)*9)

        self.score = float(c) / (len(test_tg)*9) #self.Tree.score(test_cl, test_tg)
        #print self.region, 'О %s Т %s' % (ct, tt), self.score

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __enter__(self):
        pass

def load_data():
    pkl = open('data.pkl', 'rb')
    return pickle.load(pkl)

def cut_colum(mass, n):
    """

    >>> cut_colum([[1,2,3],[1,2,3],[1,2,3]],[0,2])
    [[1, 3], [1, 3], [1, 3]]

    Вывести определенные колонки из массива
    :param mass: массив
    :param n: список колонок
    :return:
    """
    L = []
    for a in mass:
        temp = []
        for i in n:
            temp.append(a[i])
        L.append(temp)
    return L

def Generating_Trees(cl,n,m,deep):
    #print n
    list_tree = []
    set_sample = []
    set_value = []
    d = []
    a = 0

    for b in cl:
        if a == 0:
            a = b[1]
        if a != b[1]:
            TempTree = TreeAndData(set_sample, set_value, a , d , deep)
            list_tree.append(TempTree)
            a = b[1]
        d.append(b[0])
        set_sample.append(b[n:m])
        set_value.append(b[2])
    return list_tree


global coeff, goold
goold = []
coeff = 1.25

list_params = []
name_cl = ['day', 'rname_', 'cl', 'month', 'DOW', 'temp', 'pa', 'vl', 'ff', 'n', 'rrr']
combinations(list_params, [0, 1, 2, 3, 4, 5, 6])
cl = load_data()
list_tree = Generating_Trees(cl, 3, 10, 0)

m = 0
for a in list_tree:
    m+=a.score
    #print a.score
score = m/len(list_tree)
print 'Средняя', score

colmn=[[None],[None],[None],[None]]

for c in goold:
    colmn[0].append(c[0])
    colmn[1].append(c[1])
    colmn[2].append(c[2])
    colmn[3].append(c[3])


df = DataFrame({'day': colmn[0], 'region': colmn[1], 'prognoz': colmn[2], 'real': colmn[3]})
df.to_csv('Tree_test.csv')

#list_tree = Generating_Trees(cl, 3, 9, 0)





