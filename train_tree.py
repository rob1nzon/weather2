#! /usr/bin/python
# -*- coding: utf-8 -*-
from psql import load_data, get_region_list,sel_colm
import cv2
from numpy import *
__author__ = 'meow^__^'
factor = ["now", "t", "tmax", "taday", "po", "u", "umin", "ff", "n", "td", "rrr", "t1", "tmax1", "taday1", "po1", "u1", "umin1", "ff1", "n1", "td1", "rrr1", "t2", "tmax2", "taday2", "po2", "u2", "umin2", "ff2", "n2", "td2", "rrr2", "t3", "tmax3", "taday3", "po3", "u3", "umin3", "ff3", "n3", "td3", "rrr3", "delta", "delta1", "delta2", "delta3"]



if __name__ == '__main__':
    region_list = get_region_list()
    for one_region in region_list:
        print one_region
        try:
            train_arr = array(load_data(column_name=factor, add_filter="rname_ = '%s' AND start_day < '2014-01-01'" % one_region)[1],dtype=float32)
            lab_train = array(load_data(column_name=["ngar"], add_filter="rname_ = '%s' AND start_day < '2014-01-01'" % one_region)[1], dtype=float32)
            tree = cv2.DTree()
            tree.train(train_arr, 1, lab_train)
            tree.save('./tree/'+str(one_region).decode(encoding='utf-8').encode(encoding='cp1251')+'.tree')
        except:
            print 'Fail'

