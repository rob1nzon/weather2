#!-*-coding:utf-8-*-
import sys
 # import PyQt4 QtCore and QtGui modules
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4 import uic
from update_gar import update_gar
from psql import datebase_connect
import time
 
( Ui_MainWindow, QMainWindow ) = uic.loadUiType( 'main.ui' )



class MainWindow ( QMainWindow ):
    """MainWindow inherits QMainWindow"""
 
    def __init__ ( self, parent = None ):
        QMainWindow.__init__( self, parent )
        self.ui = Ui_MainWindow()
        self.ui.setupUi( self )
        self.conn, self.cur = datebase_connect()
 
    def __del__ ( self ):
        self.ui = None

    def det_date_1(self):
        date1 = self.ui.dateEdit.date()
        date2 = self.ui.dateEdit_2.date()
        sdate2 = date2.toString("yyyy-MM-dd")
        sdate1 = date1.toString("yyyy-MM-dd")
        return (str(sdate1), str(sdate2))

    def load_data(self):

        #sql = update_gar()
        #print sql
        date = self.det_date_1()
        self.cur.execute("SELECT id_gar, tmin_, tmax_, area_, center_, fname_, sname_, rname_ FROM agz_.gar WHERE (date_ >= '%s') AND (date_ <= '%s') LIMIT 100" % date)
        data = self.cur.fetchall()
        #data = {'col1':['1','2','3','4'], 'col2':['4','5','6'], 'col3':['7','8','9']}
        horHeaders = ['id_gar', 'tmin_', 'tmax_', 'area_', 'center_', 'fname_', 'sname_',
       'rname_']
        logtext=[u'Подключение к базе данных', u'Получение последней даты 2013-01-31', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Загрузка данных...', u'*******************************', u'*******************************']
        for a in logtext:
            self.ui.textEdit.append(a)
        for a in logtext:
            self.ui.textEdit.append(a)
        for n, key in enumerate(data):
            for m, item in enumerate(key):
                newitem = QTableWidgetItem(str(item).decode(encoding='utf-8'))
                self.ui.tableWidget.setItem(n, m,  newitem)
            self.ui.tableWidget.setHorizontalHeaderLabels(horHeaders)

    def load_weather(self):
        #sql = update_gar()
        #print sql
        self.cur.execute("""SELECT *
        FROM agz_.weather WHERE (data >= '2013-01-31') AND (data <= '2013-03-01') LIMIT 100""")
        data = self.cur.fetchall()
        self.ui.tableWidget.clear()
        #data = {'col1':['1','2','3','4'], 'col2':['4','5','6'], 'col3':['7','8','9']}
        horHeaders = ['wmid', 'data', 'temp', 'pa', 'pa2', 'pd', 'vl', 'ff', 'n', 'td', 'rrr', 'tg', 'loc']
        logtext=[u'Подключение к базе данных', u'Получение последней даты 2013-01-31', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Загрузка данных...', u'*******************************', u'*******************************']
        for a in logtext:
            self.ui.textEdit.append(a)
        for n, key in enumerate(data):
            for m, item in enumerate(key):
                newitem = QTableWidgetItem(str(item).decode(encoding='utf-8'))
                self.ui.tableWidget.setItem(n, m,  newitem)
            self.ui.tableWidget.setHorizontalHeaderLabels(horHeaders)
    def gis_open1(self):
        QProcess.startDetached(""" "C:\Program Files\QGIS Chugiak\\bin\\nircmd.exe" exec hide C:\\PROGRA~1\\QGISCH~1\\bin\\qgis.bat C:\\weather.qgs""")

    def mid(self):
        self.ui.label.visible = False
        logtext=[u'Подключение к базе данных', u'Получение последней даты 2013-01-31', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Загрузка данных...', u'*******************************', u'*******************************']
        for a in logtext:
            self.ui.textEdit_2.append(a)
        self.cur.execute("""SELECT day, fname_, sname_, rname_, ngar, areasum_, cl,
       temp, pa, vl, ff
        FROM agz_.day_union LIMIT 100""")
        data = self.cur.fetchall()
        #data = {'col1':['1','2','3','4'], 'col2':['4','5','6'], 'col3':['7','8','9']}
        horHeaders = ['day', 'fname_', 'sname_', 'rname_', 'ngar', 'areasum_', 'cl',
       'temp', 'pa', 'vl', 'ff']
        self.ui.tableWidget_2.columCount = len(horHeaders)
        for n, key in enumerate(data):
            for m, item in enumerate(key):
                newitem = QTableWidgetItem(str(item).decode(encoding='utf-8'))
                #print item
                self.ui.tableWidget_2.setItem(n, m,  newitem)
                if m == 6:
                    self.ui.tableWidget_2.item(n,m).setBackground(QColor(146,223,255))
            self.ui.tableWidget_2.setHorizontalHeaderLabels(horHeaders)
        self.ui.tableWidget_2.setColumnHidden(6, True)


    def cluster(self):
        logtext=[u'Подключение к базе данных', u'Получение последней даты 2013-01-31', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Загрузка данных...', u'*******************************', u'*******************************']
        for a in logtext:
            self.ui.textEdit_2.append(a)
        self.ui.label.show()
        self.ui.label.setPixmap(QPixmap("c:\\graph.JPG"))
        self.ui.tableWidget_2.setColumnHidden(6, False)
    def make_tree(self):
        region_r = [u'Большесельский', u'Борисоглебский', u'Брейтовский',
            u'Гаврилов-Ямский', u'Даниловский', u'Любимский', u'Мышкинский',
            u'Некоузский', u'Некрасовский', u'Первомайский', u'Переславский',
            u'Пошехонский', u'Ростовский', u'Рыбинский', u'Тутаевский',
            u'Ярославский', u'Угличский']
        self.ui.comboBox.addItems(region_r)
        logtext=[u'Подключение к базе данных', u'Получение последней даты 2013-01-31', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Построение деревьев...', u'*******************************', u'*******************************']+region_r
        for a in logtext:
            self.ui.textEdit_3.append(a)


    def show_tree(self):
        r = self.ui.comboBox.currentText().toUtf8()
        img = str("c:\\tree\\Tree_"+str(r)+"-1.png").decode(encoding='utf-8')
        #print img
        #self.ui.label_2.setText(img)
        self.ui.label_2.setPixmap(QPixmap(img))

    def prog(self):
        logtext=[u'Подключение к базе данных', u'Получение данных', u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                 u'Построение прогноза...', u'*******************************', u'*******************************']
        for a in logtext:
            self.ui.textEdit_4.append(a)
        self.cur.execute("""SELECT day, fname_, sname_, rname_, nterm, hareasum_, ngar, areasum_,
        cl, pcl, abs(round((cl-pcl)/10.::numeric,2))
        FROM agz_.day_union WHERE pcl is not null""")
        data = self.cur.fetchall()
        #data = {'col1':['1','2','3','4'], 'col2':['4','5','6'], 'col3':['7','8','9']}
        horHeaders = ['day', 'fname_', 'sname_', 'rname_', 'nterm', 'hareasum_', 'ngar', 'areasum_',
       'cl', 'pcl', 'd']
        self.ui.tableWidget_3.columCount = len(horHeaders)
        zero = 0
        midlle = 0
        n = 0
        for n, key in enumerate(data):
            for m, item in enumerate(key):
                newitem = QTableWidgetItem(str(item).decode(encoding='utf-8'))
                #print item
                if m != 10:
                    self.ui.tableWidget_3.setItem(n, m,  newitem)
                else:
                    self.ui.tableWidget_3.setItem(n, m, newitem)
                try:
                    if m == 8:
                        self.ui.tableWidget_3.item(n,m).setBackground(QColor(204, 229, 255))
                    if m == 9:
                        self.ui.tableWidget_3.item(n,m).setBackground(QColor(255, 255, 204))
                    if m == 10:
                        c = abs(float(str(item).decode(encoding='utf-8')))
                        if c != 0:
                            self.ui.tableWidget_3.item(n, m).setBackground(QColor(255, 102*c,102*c))
                            midlle += c
                            n += 1
                        else:
                            zero += 1
                            self.ui.tableWidget_3.item(n, m).setBackground(QColor(102, 255, 102))
                except: pass
            self.ui.tableWidget_3.setHorizontalHeaderLabels(horHeaders)
        #print str(zero),midlle
        self.ui.label_12.setText(u"Количество точных прогнозов: {1:.2%} Средняя ошибка: {2:.2%}".format(zero, float(midlle / n)))






 
#-----------------------------------------------------#
if __name__ == '__main__':

    # create application
    app = QApplication( sys.argv )
    app.setApplicationName( 'MainWindow' )
    # create widget
    w = MainWindow()
    w.setWindowTitle(u'Информационная система прогнозирования класса лесопожарной опасности муниципального района' )
    w.show()
 
    # connection
    QObject.connect( app, SIGNAL( 'lastWindowClosed()' ), app, SLOT( 'quit()' ) )
 
    # execute application
    sys.exit( app.exec_() )