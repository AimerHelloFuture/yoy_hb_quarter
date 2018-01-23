#! /usr/bin/env python
# -*- coding: utf-8 -*-



"""
计算各项财务指标的同比值（累计和单季）
author：     ycc
create time: 2017-09-06
一次性计算多个指标值, 累计和单季同比计算合并
last update time：2017-05-25
"""


import time
import logging

from config.config import configs
from service.source.basestocksheet import BaseStockSheet
from utils.mysqlutils import MysqlUtils
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class BaseStockYOY(BaseStockSheet):
    """
    财务指标的同比计算（累计同比和单季同比）
    """

    # 运行模式 base:全量计算, increase:增量计算
    mode = configs['mode']

    # 基础配置
    table = ''
    source_table = ''  # 源表，引用类失败

    # 配置columns
    column_id = ''

    column_com_uni_code_index = 0
    column_end_date_index = 0
    column_consolidation_index = 0

    # 非计算字段
    fields_columns_tuple = ()

    # 系统字段
    fields_columns_tuple2 = ('createtime', 'updatetime')

    column_com_uni_code = 'com_uni_code'
    column_end_date = 'end_date'
    column_consolidation = 'consolidation'

    @classmethod
    def work(cls):
        """
        work 入口
        :return:
        """

        logging.info(cls.__name__ + ' perform start')
        all_com_codes = cls.get_all_com_codes(cls.table)
        if all_com_codes:
            for index, com_code in enumerate(all_com_codes):
                if cls.mode == 'increase':  # 增量更新
                    find_new_com = cls.find_new_com(com_code, cls.source_table)
                    # 如果是新增公司，第一次就跑全量
                    if find_new_com:
                        cls.work_com_yoy(com_code, consolidation=1501002)
                    else:
                        cls.work_com_yoy_increase(com_code, consolidation=1501002)
                elif cls.mode == 'full':  # 全量运行
                    cls.work_com_yoy(com_code, consolidation=1501002)
                time.sleep(0.1)

        logging.info(cls.__name__ + ' perform end')

    @classmethod
    def work_update(cls):
        """
        work 入口
        :return:
        """

        logging.info(cls.__name__ + ' perform start')
        all_com_codes = cls.get_update_com_codes(cls.table)
        if all_com_codes:
            for index, com_code in enumerate(all_com_codes):
                if cls.mode == 'increase':  # 增量更新
                    find_new_com = cls.find_new_com(com_code, cls.source_table)
                    # 如果是新增公司，第一次就跑全量
                    if find_new_com:
                        cls.work_com_yoy(com_code, consolidation=1501002)
                    else:
                        cls.work_com_yoy_increase(com_code, consolidation=1501002)
                elif cls.mode == 'full':  # 全量运行
                    cls.work_com_yoy(com_code, consolidation=1501002)
                time.sleep(0.1)

        logging.info(cls.__name__ + ' perform end')

    @classmethod
    def work_com_yoy(cls, com_code, consolidation):
        """
        Perform hb of one com code
        :param com_code:
        :param consolidation:
        :return:
        """

        logging.info('perform com code of yoy '+str(com_code))
        conn = MysqlUtils.connect()

        query = 'select * from ' + cls.table + ' where com_uni_code=%s and consolidation=%s and status != %s order by end_date desc'
        args = (com_code, consolidation, '9')

        cursor = MysqlUtils.common_query(conn, query, args)

        rows = cursor.rowcount
        results = cursor.fetchall()

        if rows:
            records = []
            for index, row in enumerate(results):
                record = MysqlUtils.sql_row_to_dict(cursor, row)
                records.append(record)

            for index, record in enumerate(records):
                initial_account_date = cls.get_yesteryear_account_date(str(record[u'end_date']))
                year_record = None
                for i in range(0, len(records)):
                    if str(records[i][u'end_date']) == initial_account_date:
                        year_record = records[i]
                        break
                cls.update_indicator_yoy(record, year_record)

        cursor.close()
        conn.close()

        logging.info('finish com code '+str(com_code))

    @classmethod
    def work_com_yoy_increase(cls, com_code, consolidation):
        """
        增量更新
        更新策略：只对最近的4个account_date update
        :param com_code:
        :param consolidation:
        :return:
        """

        logging.info('perform com code of yoy increase '+com_code)
        # 获取最近更新报告期
        end_date_increase_scope = cls.get_time_scope_new(com_code, consolidation, day_formate=False)[:4]

        index = 0
        if end_date_increase_scope:
            for index, end_date_increase in enumerate(end_date_increase_scope):
                cls.update_indicator_yoy(com_code, end_date_increase, consolidation)
            index += 1
        logging.info('finish com code {0} increase {1} records'.format(com_code, index))

    @classmethod
    def update_indicator_mysql(cls, record, year_record, indicators_dict=dict(), upsert=False):
        """
        更新数据到MYSQL中去(全部待计算的指标)
        :param record:
        :param year_record:
        :param indicators_dict: 全部待计算的指标
        :param upsert:
        :return:
        """

        for indicator, value in indicators_dict.items():
            if indicator not in cls.fields_columns_tuple2:
                indicators_dict[indicator] = cls.get_yoy_rate_new(record, year_record, indicator)

        find_record = cls.find_record_new(record[u'com_uni_code'], record[u'end_date'], record[u'consolidation'], cls.source_table)

        temp_list = list()
        fields_columns_list = list(cls.fields_columns_tuple)
        for index, filed in enumerate(fields_columns_list):
            # # 保证插入的推送标志为False  {'push_flag': 0}
            # if filed == 'push_flag':
            #     record[filed] = 0
            if filed != 'src_id':
                temp_list.append(record[filed])
                indicators_dict[filed] = record[filed]
            else:
                temp_list.append(None)
                indicators_dict[filed] = None

        for i in range(0, len(cls.fields_columns_tuple2)):
            indicators_dict.pop(cls.fields_columns_tuple2[i])

        # 更新数据
        conn = MysqlUtils.connect()
        if not upsert and find_record:
            # # 保证插入的推送标志为False  {'push_flag': 0}
            # indicators_dict['push_flag'] = 0
            MysqlUtils.columns_update(conn=conn, table=cls.source_table,columns=tuple(indicators_dict.keys()), values=tuple(indicators_dict.values()),select_column=cls.column_id, select_value=find_record[cls.column_id])
        else:
            for indicator, value in indicators_dict.items():
                if str(indicator) not in fields_columns_list:
                    fields_columns_list.append(str(indicator))
                    temp_list.append(value)

            MysqlUtils.columns_insert(conn=conn, table=cls.source_table, columns=tuple(fields_columns_list), values=tuple(temp_list))
        conn.close()

    @classmethod
    def update_indicator_yoy(cls, record, year_record, upsert=False):
        """
        更新指标的同比值(同比)
        :param record:
        :param year_record:
        :param upsert:
        :return:
        """

        indicators_dict = dict()
        for indicator, indicator_value in record.items():
            if indicator == cls.column_id or indicator in cls.fields_columns_tuple:
                pass
            else:
                indicators_dict[indicator] = None

        cls.update_indicator_mysql(record, year_record, indicators_dict, upsert)
