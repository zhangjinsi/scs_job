#!/usr/bin/env python
# coding=utf-8
__author__ = 'zhangjinsi'

import random
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf8')

def userinfo():
    area_list = [u'重庆',u'广州',u'北京',u'深圳',u'上海',u'厦门']

    fd = open('C:\\pywork\\scs_job\\userinfo.txt','w+')
    for i in range(1000):
        userid = str(100+i)
        age = str(random.randrange(10,40))
        area = str(random.choice(area_list)).encode('utf8')
        user_money = str(i)
        str_temp = userid+','+age+','+area+','+user_money+'\n'
        fd.write(str_temp)
    fd.close()

def gameinfo():
    area_list = [u'斗地主',u'斗牛',u'农药',u'扑克']

    fd = open('C:\\pywork\\scs_job\\gameinfo.txt','w+')
    for i in range(4):
        gameid = str(i)
        gamename = str(area_list[i])

        str_temp = gameid+','+gamename+'\n'
        fd.write(str_temp)
    fd.close()

def gametime():
    area_list = [0,1,2,3]
    time_list = [10, 15, 20, 30, 50, 80, 90]


    for j in range(10):
        date = (datetime.datetime.now()+datetime.timedelta(days=j)).strftime('%Y-%m-%d')
        fd = open('C:\\pywork\\scs_job\\gametime_{}.txt'.format(date),'w+')
        for i in range(1000):
            fdate = str(date)
            userid = str(100+i)
            gameid = str(random.choice(area_list))
            gametime = str(random.choice(time_list))

            str_temp = fdate+','+userid+','+gameid+','+gametime+'\n'
            fd.write(str_temp)
        fd.close()

def userfee():
    area_list = [0,1,2,3]
    time_list = [10, 15, 30, 23, 33, 55, 90]

    for j in range(10):
        date = (datetime.datetime.now()+datetime.timedelta(days=j)).strftime('%Y-%m-%d')
        fd = open('C:\\pywork\\scs_job\\userfee_{}.txt'.format(date),'w+')
        for i in range(1000):
            fdate = str(date)
            userid = str(100+i)
            gameid = str(random.choice(area_list))
            gametime = str(random.choice(time_list))

            str_temp = fdate+','+userid+','+gameid+','+gametime+'\n'
            fd.write(str_temp)
        fd.close()


if __name__ == '__main__':
    gametime()
    userfee()