#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from service.source.basestocksheet import BaseStockSheet
from service.source.basestockyoy import BaseStockYOY


class CompanyBalanceSheet(BaseStockSheet):
    """
    财务报表(资产表,负债表)
    """
    table = 'company_balance_sheet'
    all_stock_codes = None

    @classmethod
    def get_all_com_codes(cls, table=''):
        """
        获取全部股票代码(缓存)
        :return:
        """
        if not cls.all_stock_codes:
            cls.all_stock_codes = BaseStockSheet.get_all_stock_codes(cls.table)
        return cls.all_stock_codes


class CompanyBalanceSheetYOY(BaseStockYOY):
    """
    财务报表(资产表,负债表)(累计同比)
    """

    # 基础配置
    table = 'company_balance_sheet'
    source_table = 'company_balance_sheet_yoy'  # 源表，引用类失败

    # 配置columns
    column_id = 'balance_sheet_id'
    column_stock_code_index = 1  # 字段名在数据库中索引
    column_account_date_index = 4  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('stock_code', 'account_date', 'publish_date', 'end_date', 'report_period', 'push_flag', 'push_search', 'push_product')


class ComBalanceYOY(BaseStockYOY):
    """
    财务报表(资产负债表)(累计同比)
    """

    # 基础配置
    table = 'com_balance'
    source_table = 'com_balance_yoy'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'report_type', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


if __name__ == '__main__':

    logging.basicConfig(filename="service_balance.log", format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', level=logging.DEBUG, filemode='w')
    logger = logging.getLogger("Analyse Service")

    ComBalanceYOY.work_update()
    # ComBalanceYOY.update_indicator_yoy_new(1, '2003-03-31 00:00:00', 1501002)
    # ComBalanceYOY.perform_com_yoy(1, consolidation=1501002)
