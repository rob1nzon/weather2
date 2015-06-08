#!-*-coding:utf-8-*-
import sys
 # import PyQt4 QtCore and QtGui modules
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4 import uic
from psql import datebase_connect, load_data, get_colum_name, sel_colm
#from psql import sel_colm, load_data, get_colum_name
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
 
( Ui_MainWindow, QMainWindow ) = uic.loadUiType( 'main.ui' )



class MainWindow ( QMainWindow ):
    """MainWindow inherits QMainWindow"""
 
    def __init__ ( self, parent = None ):
        QMainWindow.__init__( self, parent )
        self.ui = Ui_MainWindow()
        self.ui.setupUi( self )
        self.c, self.conn, self.cur = datebase_connect('localhost')
 
    def __del__ ( self ):
        self.ui = None

    def det_date_1(self):
        date1 = self.ui.dateEdit.date()
        date2 = self.ui.dateEdit_2.date()
        sdate2 = date2.toString("yyyy-MM-dd")
        sdate1 = date1.toString("yyyy-MM-dd")
        return (str(sdate1), str(sdate2))

    def fake_log(self, edit_t, text=u'Загрузка данных'):
        logtext = [u'Подключение к базе данных', u'Получение последней даты 2013-01-31',
                   u"SELECT * FROM agz_.gar WHERE (date_ >= '2013-01-31') AND (date_ <= '2013-03-01')",
                   u'%s...' % text, u'*******************************', u'*******************************']
        for a in logtext:
            edit_t.append(a)

    def transl(self, name_list):
        d = {'id_gar': u'ID гари', 'tmin_': u'Первое время наблюдения',
             'tmax_':  u'Текущее время наблюдения', 'area_/1000': u'Площадь пожара',
             'fname_': u'Федеральный округ', 'sname_': u'Область', 'rname_': u'Район',
             'wmid': u'ID метеостанции', 'data': u'Дата', 't': u'Температура',
             't': u'Температура', 'p': u'Давление', 'u': u'Влажность',
             'start_day': u'Начало периода', 'end_day': u'Конец периода',
             'now': u'Номер недели','areasum_/1000': u'Суммарная площадь', 'cl': u'Факт', 'cl1': u'Класс',
             'ngar': u'Количество гарей', 'pcl': u'Прогноз', 'abs(cl-pcl)': u'Отклонение'
             }
        name = {'temp': u'Температура', 'pa': u'Давление', 'vl': u'Влажность', 'ff': u'Скорость ветра', 'n': u'Облачность', 'rrr': u'Уровень осадков', 'td': u'Температура точки росы'}
        wek = [u'', u' неделю назад', u' две недели назад', u' три недели назад']
        d = dict(d, **{a + b: name[a] + wek[i] for a in name for i, b in enumerate(['', '1', '2', '3'])})
        new_list = []
        for a in name_list:
            try:
                new_list.append(d[a])
            except:
                new_list.append(a)
        return new_list

    def load_data(self):
        date = self.det_date_1()
        col_n = ['id_gar', 'tmin_', 'tmax_', 'area_/1000', 'fname_', 'sname_', 'rname_']
        horHeaders, data = load_data(order_b=False, table_name='gar', column_name=col_n, add_filter='''(date_ >= '2013-01-01') AND (date_ <= '2015-01-01') AND sname_ LIKE 'Ярослав%' ''', order='LIMIT 100')
        self.fake_log(self.ui.textEdit)
        self.add_to_table(data, self.transl(horHeaders), self.ui.tableWidget)

    def show_graph(self):
        column_name, data = load_data(column_name=['cl', 'areasum_'], add_filter='now is not null ', order='areasum_')
        num_col = {a: i for i, a in enumerate(column_name)}
        cmap = cm.jet
        sc = lambda b: sel_colm(data, [num_col[b]])
        d1 = sc('cl')
        d2 = sc('areasum_')
        c = np.linspace(0, 10, len(d1) * 10)
        plt.plot(d2, d1, 'bo')
        plt.grid()
        plt.show()


    def add_to_table(self, data, horHeaders, tab_name):
        tab_name.columCount = len(horHeaders)
        for n, key in enumerate(data):
            for m, item in enumerate(key):
                newitem = QTableWidgetItem(str(item).decode(encoding='utf-8'))
                tab_name.setItem(n, m, newitem)
            tab_name.setHorizontalHeaderLabels(horHeaders)
        tab_name.resizeColumnsToContents()

    def load_weather(self):
        col_n = ['wmid', 'data', 't', 'p', 'u', 'ff', 'n', 'rrr']
        horHeaders, data = load_data(table_name='new_weather', column_name=col_n, add_filter='''(data >= '2013-01-31') AND (data <= '2013-03-01')''', order='data LIMIT 300')
        self.ui.tableWidget.clear()

        self.fake_log(self.ui.textEdit)

        tab_name = self.ui.tableWidget
        self.add_to_table(data, self.transl(horHeaders), tab_name)

    def gis_open1(self):
        QProcess.startDetached(""" "C:\Program Files\QGIS Chugiak\\bin\\nircmd.exe" exec hide C:\\PROGRA~1\\QGISCH~1\\bin\\qgis.bat C:\\weather.qgs""")

    def mid(self):
        #self.ui.label.visible = False
        self.fake_log(self.ui.textEdit_2)
        col_n = ['start_day', 'end_day', 'now', 'fname_', 'sname_', 'rname_', 'cl', 'ngar', 'areasum_/1000']
        w = ['t', 'tmax', 'taday', 'po', 'u', 'umin', 'ff', 'n', 'td', 'rrr', 't1', 'tmax1', 'taday1', 'po1', 'u1', 'umin1', 'ff1', 'n1', 'td1', 'rrr1', 't2', 'tmax2', 'taday2', 'po2', 'u2', 'umin2', 'ff2', 'n2', 'td2', 'rrr2', 't3', 'tmax3', 'taday3', 'po3', 'u3', 'umin3', 'ff3', 'n3', 'td3', 'rrr3']
        d = ['delta', 'delta1', 'delta2', 'delta3']
        w1 = ['to_char(' + str(j) + ", '0.99')" for j in w]
        horHeaders, data = load_data(table_name='week_union_norm', column_name=col_n+w1+d, add_filter="sname_ LIKE 'Ярослав%'", order='start_day LIMIT 300')
        col_n[6] = 'cl1'
        horHeaders = self.transl(col_n+w+d)
        self.ui.tableWidget_2.columCount = len(horHeaders)
        self.add_to_table(data, horHeaders, self.ui.tableWidget_2)
        self.ui.tableWidget_2.setColumnHidden(6, True)

    def cluster(self):
        tab_name = self.ui.tableWidget_2
        tab_name.setColumnHidden(6, False)
        try:
            for n in range(300):
                tab_name.item(n, 6).setBackground(QColor(167,255,255))
        except:
            pass

    def make_tree(self):
        region_r = [u"Брейтовский",
u"Первомайский",
u"Некрасовский",
u"Ярославский",
u"Любимский",
u"Рыбинский",
u"Пошехонский",
u"Большесельский",
u"Угличский",
u"Ростовский",
u"Некоузский",
u"Тутаевский",
u"Гаврилов-Ямский",
u"Переславль-Залесский",
u"Переславский",
u"Борисоглебский",
u"Даниловский"]#[u'Абанский', u'Ачинский', u'Балахтинский', u'Березовский', u'Бирилюсский', u'Боготольский', u'Богучанский', u'Большемуртинский', u'Большеулуйский', u'Дзержинский', u'Емельяновский', u'Енисейский', u'Ермаковский', u'Идринский', u'Иланский', u'Ирбейский', u'Казачинский', u'Канский', u'Каратузский', u'Кежемский', u'Минусинский', u'Мотыгинский', u'Назаровский', u'Нижнеингашский', u'Новоселовский', u'Партизанский', u'Пировский', u'Рыбинский', u'Саянский', u'Северо-Енисейский', u'Сухобузимский', u'Таймырский', u'Тасеевский', u'Туруханский', u'Тюхтетский', u'Ужурский', u'Уярский', u'Шарыповский', u'Шушенский', u'Эвенкийский']
        self.ui.comboBox.addItems(region_r)
        self.fake_log(self.ui.textEdit_3, text=u'Построение деревьев...')


    def show_tree(self):
        r = self.ui.comboBox.currentText().toUtf8()
        img = str("c:\\tree\\Week_Tree_"+str(r)+"-1.png").decode(encoding='utf-8')
        #print img
        #self.ui.label_2.setText(img)
        self.ui.label_2.setPixmap(QPixmap(img))

    def prog(self):
        self.fake_log(self.ui.textEdit_4, text=u'Построение прогноза...')
        col_n = ['start_day', 'end_day', 'now', 'fname_', 'sname_', 'rname_', 'ngar', 'pcl', 'abs(ngar-pcl)']
        horHeaders, data = load_data(table_name='week_union_norm', column_name=col_n, add_filter='pcl is not null', order='ngar DESC LIMIT 300')
        self.add_to_table(data, self.transl(horHeaders), self.ui.tableWidget_3)


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