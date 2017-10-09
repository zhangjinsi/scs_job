#!/usr/bin/env python
# coding=utf-8
__author__ = 'zhangjinsi'

import sys
sys.path.append('/home/hadoop/scs_jobs/scs_job')
from dbbase.HiveBase import HiveBase

class UserInfo(HiveBase):
    #stat
    def create_table(self):
        hql = """
        create table if not EXISTS stat.userinfo (fuserid int, fage int, farea string, fmoney int)
        row format delimited fields terminated by ','
        location '/user/hadoop/stat/userinfo'
        """
        return self.execute(hql)

    def do_jobs(self):
        hql = """
        load data inpath '/user/hadoop/stat/userinfo' into table stat.userinfo
        """
        return self.execute(hql)




if __name__ == '__main__':
    obj = UserInfo()
    obj()