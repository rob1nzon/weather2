import requests
import urllib2
import gzip
import shutil

from urllib2 import urlopen
from bs4 import BeautifulSoup

def load_city(self):
        url = "http://rp5.ru/%D0%9F%D0%BE%D0%B3%D0%BE%D0%B4%D0%B0_%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B8"
        html_doc = urlopen(url).read()
                     name = s2[a1:b1]
                print name
                load_m('',url)
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
            #print url

            #loar city in region

            #


            # #print n[1]
        #for link in soup.find_all('a'):
        #    print link.get('href')
   soup = BeautifulSoup(html_doc)
        global cityurl
        for link in soup.find_all('a'):
            s=str(link)
            #print s
            #print str(s).rfind("ToWeather")
            if (str(s).rfind("ToWeather") > 0):
                a=s.rfind("<a")+9
                b=a+s[s.rfind("<a")+9:a+200].find("style")-2
                s2=s[a:a+200]
                a1=s2.find("title")+7
                b1=s2.find('"><s')
                url =s[a:b]

def load_m(self,text):
     s=str(text)
     #print s
     url = "http://rp5.ru"+str(s)
     html_doc2 = urlopen(url).read()
     soup = BeautifulSoup(html_doc2)
     for link in soup.find_all('a'):
         s2=str(link)
         #print s2
         #print str(s2).rfind("ToWeather")
         if (str(s2).rfind("ToWeather") > 0):
                         #print s2
            a=str(s2).rfind("title=")+7
            b=a+str(s2)[str(s2).rfind("title=")+7:a+200].find("span")-3
            s3=s2[a:a+200]
            a1=str(s2).find("href=")+6
            b1=str(s2).find("title")-2
            url2 =s2[a1:b1]
            name2 = s2[a:b]
            print name2,url2
            load_m('',url2)
         for link in soup.find_all('a'):
             s=str(link)
             if (s.find('href20')>0):
                 s1=s[s.index("href")+15:len(s)-1]
                 b=s1.index('">')
                 #url+=s1[0:b]
                 name=s1[b+2:s1.index('<')]
                 url=s1[0:b]
                 #print 'reg:'+url
                 print '-',name

#load_city('')
load_m('',str('/Weather_in_Adygea'))