#! /usr/bin/python
# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
from forecast import sel_colm
from psql import load_data, sel_colm


def plot_graph():
    column_name, data = load_data(column_name=['delta', 'cl', 'pcl'], add_filter='pcl is not null')
    num_col = {a: i for i, a in enumerate(column_name)}
    sc = lambda b: sel_colm(data, [num_col[b]])
    d1 = sc('delta')
    d2 = sc('cl')
    d3 = sc('pcl')
    d2 = [[float(a[0]) * 0.05] for a in d2]
    d3 = [[float(a[0]) * 0.05] for a in d3]
    #plt.plot(d1, 'b')
    plt.plot(d2, 'g')
    plt.plot(d3, 'r')
    plt.grid()
    plt.show()


def plot_graph2():
    column_name, data = load_data(column_name=['cl', 'pcl'], add_filter='now is not null ', order='areasum_')
    num_col = {a: i for i, a in enumerate(column_name)}
    sc = lambda b: sel_colm(data, [num_col[b]])
    d1 = sc('cl')
    d2 = sc('areasum_')
    plt.plot(d2, d1, 'bo')
    plt.grid()
    plt.show()


if __name__ == '__main__':
    plot_graph()

