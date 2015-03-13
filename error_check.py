#! /usr/bin/python
# -*- coding: utf-8 -*-
"""
Модуль проверки ошибки прогноза
"""
from forecast import load_data, sel_colm


def check_error(detail=True, table_name='week_union_norm', unic_field=['rname_'], check_field=['cl', 'pcl']):
    col_n, dt = load_data(column_name=unic_field + check_field, add_filter='pcl is not Null', table_name=table_name, order=unic_field[0])
    unic = dt[0][0]
    err_reg=[]
    unic = dt[0][0]
    err, err2, c = 0, 0, 0
    for line in dt:
        if line[0] != unic:
            l = [unic, float(err) / float(c), (float(err2)/9) / float(c), c]
            err_reg.append(l)
            unic = line[0]
            err, err2, c = 0, 0, 0
        if line[1] != line[2]:
            err += 1
        err2 += abs(line[1] - line[2])
        c += 1
    sc = lambda a: sum([b[0] for b in sel_colm(err_reg, [a])])
    lc = lambda a: len([b[0] for b in sel_colm(err_reg, [a])])
    if detail:
        return (sc(1) / lc(1), (sc(2)) / lc(2)), err_reg
    else:
        return (sc(1) / lc(1), (sc(2)) / lc(2))
        #print 'All %.5s %.5s' % (sc(1) / lc(1), sc(2) / lc(2))






if __name__ == '__main__':
    er, dt = check_error(detail=True, unic_field=['rname_'])
    print er

