#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from service.source.basestocksheet import BaseStockSheet
from utils.mysqlutils import MysqlUtils
from service.source.basecompanyquarter import BaseCompanyQuarter
from service.source.basestockhb import BaseStockHB
from service.source.basestockyoy import BaseStockYOY


class CompanyProfitSheet(BaseStockSheet):
    """
    财务报表(利润表)
    """
    table = 'com_profit'
    all_com_codes = None

    @classmethod
    def get_recent_record(cls, com_code='', end_date='', consolidation=1501002):
        """
        Get single one record(recent)

        :param com_code:
        :param end_date:
        :param consolidation:
        :return: dict
        """
        try:

            conn = MysqlUtils.connect()
            query = 'select * from '+cls.table+' where com_uni_code = %s and consolidation = %s and '+cls.column_end_date+'<=%s order by '+cls.column_end_date+' desc'
            args = (com_code, consolidation, end_date)

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

    @classmethod
    def get_all_com_codes(cls, table=''):
        """
        获取全部代码(缓存)
        :return:
        """
        if not cls.all_com_codes:
            cls.all_com_codes = BaseStockSheet.get_all_com_codes(cls.table)
        return cls.all_com_codes


class CompanyProfitSheetQuarter(BaseCompanyQuarter):
    """
    财务报表(利润表)(单季度)
    """

    # 基础配置
    table = 'com_profit'
    source_table = 'com_profit_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'report_type', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyProfitSheetQuarterHB(BaseStockHB):
    """
    财务报表(利润表)(单季度环比)
    """

    # 基础配置
    table = 'com_profit_quarter'
    source_table = 'com_profit_sheet_hb_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'report_type', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyProfitSheetQuarterYOY(BaseStockYOY):
    """
    财务报表(利润表)(单季度同比)
    """

    # 基础配置
    table = 'com_profit_quarter'
    source_table = 'com_profit_sheet_yoy_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'report_type', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyProfitSheetYOY(BaseStockYOY):
    """
    财务报表(利润表)(累计同比)
    """

    # 基础配置
    table = 'com_profit'
    source_table = 'com_profit_sheet_yoy'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'report_type', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


if __name__ == '__main__':

    logging.basicConfig(filename="service_profit.log", format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', level=logging.DEBUG, filemode='w')
    logger = logging.getLogger("Analyse Service")

    CompanyProfitSheetQuarter.work()
    # CompanyProfitSheetQuarterYOY.work()
    # CompanyProfitSheetYOY.work()
