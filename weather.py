# -*- coding: utf-8 -*-


# Form implementation generated from reading ui file 'weather.ui'
#
# Created: Fri May 16 14:44:10 2014
#      by: PyQt4 UI code generator 4.10.4
#
# WARNING! All changes made in this file will be lost!
#from StringIO import StringIO
import requests
import urllib2
import gzip
import shutil

from urllib2 import urlopen
from bs4 import BeautifulSoup
from PyQt4 import QtCore, QtGui
from StringIO import StringIO
import csv

global dump

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s
try:
    _toUtf8 = QtCore.QString.toUtf8
except AttributeError:
    def _toUtf8(s):
        return s
id=''
cityurl={}
class Weather(QtGui.QMainWindow):
    
    def __init__(self,parent=None):
        super(Weather, self).__init__(parent)
        self.prevCmd = ''
        self.counter = 0
        self.muted = False
        self.conns = []
        self.setWindowTitle(u"Weather RP5")
        self.ui = Ui_Weather()
        self.ui.setupUi(self)
        gry = self.geometry()
        gry.moveCenter(QtCore.QCoreApplication.instance().desktop().availableGeometry().center())
        self.setGeometry(gry)
        QtCore.QObject.connect(self.ui.load_city,QtCore.SIGNAL(u"clicked()"), self.load_city)
        QtCore.QObject.connect(self.ui.load_data,QtCore.SIGNAL(u"clicked()"), self.load_data)
        QtCore.QObject.connect(self.ui.comboBox,QtCore.SIGNAL(u"activated(QString)"), self.load_m)
        QtCore.QObject.connect(self.ui.comboBox_2,QtCore.SIGNAL(u"activated(QString)"), self.load_id)
        self.show()

    def load_city(self):
        url = "http://rp5.ru/%D0%9F%D0%BE%D0%B3%D0%BE%D0%B4%D0%B0_%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B8"
        html_doc = urlopen(url).read()
        soup = BeautifulSoup(html_doc)
        global cityurl
        for link in soup.find_all('div', 'RuLinks'):
            s=str(link)
            #print s
            a=s.rfind("<a")+9
            b=a+s[s.rfind("<a")+9:a+200].find('">')
   
            s2=s[a:a+200]
            a1=s2.find("title")+2
            b1=s2.find('"><s')
            url =s[a:b]
            name = s2[a1:b1]
            #print name
            #print url
            #cityurl[_fromUtf8(name)]=_fromUtf8(url)
            # a=s.index("RuLinks")+18
            # b=a+s[s.index("RuLinks")+18:200].index(">")-1
            # s2=s[s.index("RuLinks")+18:200]
            # a1=s2.index(">")+1
            # b2=s2.index("<")
            # print s2[a1:b2]
            # url+=s[a:b]
            # name+=s2[a1:b2]
            self.ui.comboBox.addItem(_fromUtf8(url))
            #loar city in region
            
            # 


            # #print n[1]
        #for link in soup.find_all('a'):
        #    print link.get('href')
    def load_m(self,text):
        s=_toUtf8(text)
        #print s
        url = "http://rp5.ru"+str(s)
        #print url
        html_doc2 = urlopen(url).read()
        soup = BeautifulSoup(html_doc2)
        self.ui.comboBox_2.clear()
            #url=[]
            #name=[]
        for link in soup.find_all('a'):
            s=str(link)
            if (s.find('href20')>0):
                #print link
                s1=s[s.index("href")+15:len(s)-1]
                b=s1.index('">')
                #url+=s1[0:b]
                #name+=s1[b+2:s1.index('<')-1]
                url=s1[0:b]
                #print 'reg:'+url
                self.ui.comboBox_2.addItem(_fromUtf8(url))
    def load_id(self,text):
        global id
        s=_toUtf8(text)
        #print s
        url3 = "http://rp5.ru/"+str(s)
        html_doc = urlopen(url3).read()
        soup = BeautifulSoup(html_doc)
        s=str(soup.find_all(id="archive_link"))
        #print s[10:s.find('id')-2]
        url3 = s[10:s.find('id')-2]
        html_doc = urlopen(url3).read()
        soup = BeautifulSoup(html_doc)
        for sth in soup.find_all('div'):
            s=str(sth)
            if (s.find('fconfirm')>0):
                a=s
        # print a
        id=a[a.find(",")+1:a.find(")")]
        #print id
        # self.ui.plainTextEdit.appendPlainText(text)
        self.ui.plainTextEdit.appendPlainText(text+_fromUtf8(" "+id))

    def load_data(self):
        #metar=5001&a_date1=15.06.2014&a_date2=16.06.2014&f_ed3=6&f_ed4=6&f_ed5=15&f_pe=1&f_pe1=3&lng_id=2
        #http://rp5.ru/inc/f_metar.php?
        temp_var = self.ui.dateEdit.date()
        var_name = temp_var.toPyDate()
        y1=str(var_name)[0:4]
        m1=str(var_name)[5:7]
        d1=str(var_name)[8:10]
        temp_var = self.ui.dateEdit_2.date()
        var_name = temp_var.toPyDate()
        y2=str(var_name)[0:4]
        m2=str(var_name)[5:7]
        d2=str(var_name)[8:10]

        dt1=d1+'.'+m1+'.'+y1
        dt2=d2+'.'+m1+'.'+y2
        #print dt1,dt2
        # data = {
        #     'metar=':str(id),
        #     'a_date1':dt1,
        #     'a_date2':dt2,
        #     'f_ed3':'6',
        #     'f_ed4':'6',
        #     'f_ed5':'15',
        #     'f_pe':'1',
        #     'f_pe1':'3',
        #     'lng_id':'2'
        # }
        data = {
                    'a_date1':dt1,
                    'a_date2':dt2,
                    'f_ed0':d2,
                    'f_ed1':m2,
                    'f_ed2':y2,
                    'f_ed3':'05',
                    'f_ed4':'05',
                    'f_ed5':'21',
                    'f_ed6':d1,
                    'f_ed7':m1,
                    'f_ed8':y1,
                    'f_pe':'1',
                    'f_pe1':'2',
                    'lng_id':'2',
                    'wmo_id':str(id)
                    }
        # url='http://rp5.ru/inc/f_archive.php'
        url='http://rp5.ru/inc/f_archive.php'
        r = requests.post(url,data)
        s=r.text
        #print s
        a=s.find('http://')
        b=s.rfind('csv.gz')+6

        #global dump
        #gfurl = s[a:b]
       # file = requests.get(gfurl, stream=True)
       # dump = file.raw
        location = os.path.abspath("file.gz")
       # with open("file.gz", 'wb') as location:
       #     shutil.copyfileobj(dump, location)
       # del dump


        self.ui.plainTextEdit.appendPlainText(s[a:b])

        request = urllib2.Request(s[a:b])
        request.add_header('Accept-encoding', 'gzip')
        response = urllib2.urlopen(request)
        buf = StringIO( response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
        #self.ui.plainTextEdit.appendPlainText(_fromUtf8(data))
        cdata= csv.reader(data)

        n=0
        for r in cdata:
            s=str(r)
            if (n==2):
                temp=s
                self.ui.plainTextEdit.appendPlainText(_fromUtf8(data+"  "+temp))
                n=0
            if (n==1):
                n=2
            if (s.find(':')>0 and len(s)>2):
                data=s
                n=1


class Ui_Weather(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName(_fromUtf8("Weather RP5 loader"))
        MainWindow.resize(363, 334)
        self.centralwidget = QtGui.QWidget(MainWindow)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.comboBox = QtGui.QComboBox(self.centralwidget)
        self.comboBox.setGeometry(QtCore.QRect(10, 10, 201, 31))
        self.comboBox.setObjectName(_fromUtf8("comboBox"))
        self.comboBox.addItem(_fromUtf8("Регион"))
        self.comboBox_2 = QtGui.QComboBox(self.centralwidget)
        self.comboBox_2.setGeometry(QtCore.QRect(10, 50, 201, 31))
        self.comboBox_2.setObjectName(_fromUtf8("comboBox_2"))
        self.comboBox_2.addItem(_fromUtf8("Город"))
        self.load_city = QtGui.QPushButton(self.centralwidget)
        self.load_city.setGeometry(QtCore.QRect(220, 10, 131, 31))
        self.load_city.setObjectName(_fromUtf8("load_city"))
        self.load_city.setText(_fromUtf8("Загрузить список"))
        self.dateEdit = QtGui.QDateEdit(self.centralwidget)
        self.dateEdit.setGeometry(QtCore.QRect(10, 90, 101, 31))
        self.dateEdit.setObjectName(_fromUtf8("dateEdit"))
        self.dateEdit_2 = QtGui.QDateEdit(self.centralwidget)
        self.dateEdit_2.setGeometry(QtCore.QRect(120, 90, 91, 31))
        self.dateEdit_2.setObjectName(_fromUtf8("dateEdit_2"))
        self.plainTextEdit = QtGui.QPlainTextEdit(self.centralwidget)
        self.plainTextEdit.setGeometry(QtCore.QRect(10, 120, 341, 181))
        self.plainTextEdit.setObjectName(_fromUtf8("plainTextEdit"))
        self.load_data = QtGui.QPushButton(self.centralwidget)
        self.load_data.setGeometry(QtCore.QRect(220, 90, 131, 31))
        self.load_data.setObjectName(_fromUtf8("load_data"))
        self.load_data.setText(_fromUtf8("Загрузить данные"))
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtGui.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 363, 20))
        self.menubar.setObjectName(_fromUtf8("menubar"))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtGui.QStatusBar(MainWindow)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        MainWindow.setStatusBar(self.statusbar)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)



if __name__ == "__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('cp1251')
    #sys.setdefaultencoding('utf8')
    app = QtGui.QApplication(["Weather RP5 loader"])
    MainWindow = QtGui.QMainWindow()
    wem = Weather()
    res = app.exec_()
    
    sys.exit(res)