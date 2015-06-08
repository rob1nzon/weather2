#! /usr/bin/python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET

csv_file = 'c:\\Users\\User\\123.xml'


if __name__ == '__main__':
    tree = ET.parse(csv_file)
    root = tree.getroot()
    for c in root.findall('input'):
        print c.text
    for c in root.findall('output'):
        print c.text
    #for child in root:
    #    print child.text ,child.tag, child.attrib