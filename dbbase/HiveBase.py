#!/usr/bin/env python
# coding=utf-8
__author__ = 'zhangjinsi'

import sys
sys.path.append('/home/hadoop/scs_jobs/scs_job')
from pyhive import hive
from logger import logger
import config
import datetime

class HiveBase(object):
    def __init__(self, date=None):
        self.port = config.HIVE_PORT
        self.host = config.HIVE_HOST
        if date:
            self.today = date
        else:
            self.today = datetime.datetime.now().date().strftime('%Y-%m-%d')

    def execute(self, hql=None):
        try:
            logger.info('hql-- {}'.format(hql))
            conn = hive.connect(host=self.host, port=self.port)
            cursor = conn.cursor()
            cursor.execute(hql)
            cursor.commit()
            cursor.close()
            conn.close()
            return True

        except Exception as e:
            logger.error('hive执行异常')
            cursor.close()
            conn.close()
            return False
    def create_table(self):
        raise '子类必须实现创建方法'

    def do_jobs(self):
        raise '子类必须实现统计方法'

    def __call__(self, *args, **kwargs):
        logger.info('日期--{}'.format(self.today))
        logger.info('开始建表--')
        res = self.create_table()
        if res:
            logger.info('建表成功')
        else:
            logger.warning('建表失败')

        logger.info('开始统计，耐心--')
        res = self.do_jobs()
        if res:
            logger.info('统计成功')
        else:
            logger.warning('统计失败')