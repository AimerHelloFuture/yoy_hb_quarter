#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time

from config.config import configs
from service.source.basestocksheet import BaseStockSheet
from service.source.companybalancesheet import CompanyBalanceSheet
from utils.mysqlutils import MysqlUtils


class BaseCompanyIndicator(BaseStockSheet):
    """
    财务指标基础类
    """
    table = ''

    # 运行模式 base:全量计算, increase:增量计算
    mode = configs['mode']

    @classmethod
    def perform(cls):
        """
        work 入口

        :return:
        """
        logging.info(cls.__name__+' perform start mode:'+cls.mode)
        all_com_codes = cls.get_all_com_codes()

        if all_com_codes and len(all_com_codes) > 0:
            for com_code in all_com_codes:
                # threading.Thread(target=cls.perform_company, args=(stock_code, ), name=stock_code).start()
                cls.perform_company(com_code)
                time.sleep(0.1)

        logging.info(cls.__name__+' perform end mode:'+cls.mode)

    @classmethod
    def perform_company(cls, com_code):
        """
        Perform one stock code
        :param com_code:
        :return:
        """
        raise NotImplemented

    @classmethod
    def get_all_com_codes(cls, table=''):
        """
        获取所有代码
        :return:
        """
        return CompanyBalanceSheet.get_all_com_codes()

    @classmethod
    def get_all_common_columns(cls, table=None, except_columns=[]):
        """
        Get COMMON_COLUMNS
        :return:
        """
        try:
            if not table:
                table = cls.table
            if not table or len(table) == 0:
                raise AttributeError

            conn = MysqlUtils.connect()
            query = 'show columns from '+table
            cursor = MysqlUtils.common_query(conn, query)
            rows = cursor.fetchall()

            if not rows or len(rows) == 0:
                return None
            else:
                result = []
                for row in rows:
                    if row[0] not in except_columns:
                        result.append(str(row[0]))
                return tuple(result)

        except Exception as e:
            logging.error(e)
        finally:
            conn.close()