# -*- coding: utf-8 -*-

from sklearn.datasets import load_iris
from sklearn.externals.six import StringIO
from sklearn import tree
import pydot
from psql import datebase_connect
import pickle
f, conn, cur = datebase_connect('localhost')
#date_part('month', day), EXTRACT(DOW FROM day)+1
cur.execute('''SELECT day, rname_, cl, date_part('month', day), EXTRACT(DOW FROM day), temp,pa,vl,ff,n,rrr
  FROM agz_.day_union WHERE (temp is not null) AND (pa is not null)
  AND (vl is not null) AND (ff is not null) AND  (n is not null) AND (rrr is not null) AND (cl is not null) ORDER BY rname_ ;''')
data = cur.fetchall()

cl = data

#for a in data:
#    cl.append([a[-3], a[-2], a[-1]])
#    target.append(a[0])

output = open('data.pkl', 'wb')
pickle.dump(cl, output)
output.close()

#iris = load_iris()
#clf = tree.DecisionTreeClassifier()
#clf = clf.fit(cl, target)
#print cl
#print target
#dot_data = StringIO()
#tree.export_graphviz(clf, out_file=dot_data, feature_names=['temp', 'pa', 'vl'])
#graph = pydot.graph_from_dot_data(dot_data.getvalue())
#graph.write_pdf("iris.pdf")