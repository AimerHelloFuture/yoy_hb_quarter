#! /usr/bin/env python
# -*- coding: utf-8 -*-
from service.source.basecompanyquarter import BaseCompanyQuarter
from service.source.basestocksheet import BaseStockSheet
from service.source.basestockhb import BaseStockHB
from service.source.basestockyoy import BaseStockYOY
import logging


class CompanyCashSheet(BaseStockSheet):
    """
    财务报表(现金表)
    """
    table = 'company_cash_sheet'


class CompanyCashSheetQuarter(BaseCompanyQuarter):
    """
    财务报表(现金表)(单季度)
    """

    # 基础配置
    table = 'com_cashflow'
    source_table = 'com_cashflow_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'reporttype', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyCashSheetQuarterHB(BaseStockHB):
    """
    财务报表(现金表)(单季度环比)
    """

    # 基础配置
    table = 'com_cashflow_quarter'
    source_table = 'com_cashflow_hb_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'reporttype', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyCashSheetQuarterYOY(BaseStockYOY):
    """
    财务报表(现金表)(单季度同比)
    """

    # 基础配置
    table = 'com_cashflow_quarter'
    source_table = 'com_cash_sheet_yoy_quarter'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'reporttype', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


class CompanyCashSheetYOY(BaseStockYOY):
    """
    财务报表(现金表)(累计同比)
    """

    # 基础配置
    table = 'com_cashflow'
    source_table = 'com_cash_sheet_yoy'  # 源表，引用类失败

    # 配置columns
    column_id = 'id'
    column_com_uni_code_index = 1  # 字段名在数据库中索引
    column_end_date_index = 2  # 字段名及其在数据库中索引
    column_consolidation_index = 5  # 字段名及其在数据库中索引

    # 非计算字段
    fields_columns_tuple = ('com_uni_code', 'end_date', 'reporttype', 'principles', 'consolidation', 'currency_code', 'announcement_date', 'report_format', 'whether_published', 'special_case_description', 'come_source', 'status', 'remark', 'creator', 'editor', 'push_search', 'push_product', 'qhqm_seq', 'src_id')


if __name__ == '__main__':

    logging.basicConfig(filename="service_cashflow.log", format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', level=logging.DEBUG, filemode='w')
    logger = logging.getLogger("Analyse Service")

    CompanyCashSheetQuarterHB.work()
    # CompanyCashSheetQuarterYOY.work()
    # CompanyCashSheetYOY.work()


