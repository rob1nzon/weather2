#! /usr/bin/python
# -*- coding: utf-8 -*-
from psql import datebase_connect, load_data, sel_colm
from week_union2 import get_region_list

def create_set():
    pass

def forecast2(factor=['delta', 'delta1', 'delta2', 'delta3', 'now']):
    reg_list = get_region_list()
    for reg in reg_list:
        name_cl, cl = load_data(column_name=['start_day', 'rname_', 'cl'] + factor, add_filter="rname_ = '%s'" % reg)

        dcl = {a: i for i, a in enumerate(name_cl)}
        grunt = lambda col: sel_colm(cl, [dcl[a] for a in col])


if __name__ == '__main__':
    forecast2()