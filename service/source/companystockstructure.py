#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from service.source.basestocksheet import BaseStockSheet
from utils.mysqlutils import MysqlUtils


class CompanyStockStructure(BaseStockSheet):
    """
    股本结构
    """
    table = 'sec_struc_alter'
    column_account_date = 'chan_date'

    @classmethod
    def get_record_new(cls, com_code='', chan_date=''):
        """
        Get single one record
        :param com_code:
        :param chan_date:
        :return: dict
        """
        try:
            conn = MysqlUtils.connect()
            query = "select * from "+cls.table+" where com_uni_code = %s and "+cls.column_account_date+"<=%s order by "+cls.column_account_date+" desc limit 1"
            args = (com_code, chan_date)

            cursor = MysqlUtils.common_query(conn, query, args)
            row = cursor.fetchone()

            if not row:
                return None
            else:
                return MysqlUtils.sql_row_to_dict(cursor, row)
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

