#!/usr/bin/env python
# coding=utf-8
__author__ = 'zhangjinsi'

import sys
sys.path.append('/home/hadoop/scs_jobs/scs_job')
from sqoop.sqoopexe import ShellExec

class ExportGameActive(ShellExec):
    def create_table(self):
        sql = '''create table if not EXISTS gameactive
              (fdate varchar(64), fgamename varchar(64), fcount int)
              DEFAULT CHARACTER set utf8
              '''

        return self.mysql_db.execute_commit(sql)
    def do_jobs(self):
        sqoop_export = """
        /usr/local/sqoop/sqoop-1.4.4-cdh5.0.0/bin/sqoop export  \
        --connect jdbc:mysql://192.168.1.69:3306/analysis?characterEncoding=utf8  \
        -m 1  \
        --username root  \
        --password root  \
        --export-dir /user/hadoop/analysis/gameactive/dt={fdate}  \
        --table gameactive  \
        --fields-terminated-by ','
        """.format(fdate=self.today)
        return self.execute(cmd=sqoop_export)

if __name__ == '__main__':
    obj = ExportGameActive()
    obj()