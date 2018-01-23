#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import sys

from utils.mysqlutils import MysqlUtils
from utils.commontools import *
import datetime


class BaseStockSheet(object):
    """
    StockSheet 基类
    """

    """
    子类配置项
    """
    table = ''
    column_account_date = 'account_date'
    column_end_date = 'end_date'
    column_consolidation = 'consolidation'

    @staticmethod
    def exit():
        sys.exit(0)

    @classmethod
    def get_record(cls,stock_code='',account_date='', tolerance=0):
        """
        Get single one record
        :param stock_code:
        :param account_date:
        :param tolerance : 容忍度 0表示零容忍,1表示最多往前取一个季度
        :return: dict
        """
        current_record = cls.__get_record(stock_code, account_date)
        
        #容忍度:最多取n个季度
        while not current_record and tolerance>0:
            account_date = cls.get_last_account_date(account_date)
            current_record = cls.get_record(stock_code, account_date)
            tolerance -= 1

        return current_record

    @classmethod
    def get_record_new(cls, com_uni_code='', end_date='', consolidation=0, tolerance=0):
        """
        Get single one record
        :param stock_code:
        :param account_date:
        :param tolerance : 容忍度 0表示零容忍,1表示最多往前取一个季度
        :return: dict
        """
        current_record = cls.__get_record_new(com_uni_code, end_date, consolidation)

        # 容忍度:最多取n个季度
        while not current_record and tolerance > 0:
            account_date = cls.get_last_account_date(end_date)
            current_record = cls.get_record_new(com_uni_code, account_date, consolidation)
            tolerance -= 1

        return current_record

    @classmethod
    def find_record(cls, stock_code='', account_date='', source_table=''):
        """
        Find single one record from source_table
        :param stock_code:
        :param account_date:
        :param source_table:
        :return:
        """

        try:
            conn = MysqlUtils.connect()
            query = 'select * from ' + source_table +' where stock_code=%s and account_date=%s'
            args = (stock_code, account_date)

            cursor = MysqlUtils.common_query(conn, query, args)
            row = cursor.fetchone()

            results = None
            if row:
                results = MysqlUtils.sql_row_to_dict(cursor, row)

            conn.close()
            return results
        except Exception as ex:
            logging.error(ex)

    @classmethod
    def find_record_new(cls, com_uni_code='', end_date='', consolidation=0, source_table=''):
        """
        Find single one record from source_table
        :param stock_code:
        :param account_date:
        :param source_table:
        :return:
        """

        try:
            try:
                conn = MysqlUtils.connect()
            except Exception as e:
                logging.error('ccc' + str(e))
            query = 'select * from ' + source_table + ' where com_uni_code=%s and end_date=%s and consolidation=%s and status != %s'
            args = (com_uni_code, end_date, consolidation, '9')

            cursor = MysqlUtils.common_query(conn, query, args)
            row = cursor.fetchone()

            results = None
            if row:
                results = MysqlUtils.sql_row_to_dict(cursor, row)

            conn.close()
            return results
        except Exception as ex:
            logging.error(ex)

    @classmethod
    def find_new_stock(cls, stock_code='', source_table=''):
        """
        find new stock_code from source_table
        :param stock_code:
        :param source_table:
        :return: True or False
        """

        try:
            conn = MysqlUtils.connect()
            query = 'select 1 from ' + source_table +' where stock_code=%s limit 1;'
            args = (stock_code,)

            cursor = MysqlUtils.common_query(conn, query, args)
            row = cursor.fetchone()

            results = True
            if row:
                results = False
            conn.close()

            return results
        except Exception as ex:
            logging.error(ex)

    @classmethod
    def find_new_com(cls, com_code='', source_table=''):
        """
        find new com_code from source_table
        :param com_code:
        :param source_table:
        :return: True or False
        """

        try:
            try:
                conn = MysqlUtils.connect()
            except Exception as e:
                logging.error('bbb' + str(e))
            query = 'select 1 from ' + source_table + ' where com_uni_code=%s limit 1;'
            args = (com_code,)

            cursor = MysqlUtils.common_query(conn, query, args)
            row = cursor.fetchone()

            results = True
            if row:
                results = False
            conn.close()

            return results
        except Exception as ex:
            logging.error(ex)

    @classmethod
    def __get_record(cls, stock_code='', account_date=''):
        """
        Get single one record
        :param cls: 
        :param stock_code: 
        :param account_date: 
        :return: 
        """
        try:
            stock_code = cls.formate_stock_code(stock_code)

            conn = MysqlUtils.connect()
            query = 'select * from '+cls.table+' where stock_code=%s and '+cls.column_account_date+'=%s'
            args = (stock_code, account_date)

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
    def __get_record_new(cls, com_uni_code='', end_date='', consolidation=0):
        """
        Get single one record
        :param cls:
        :param com_uni_code:
        :param end_date:
        :param consolidation:
        :return:
        """
        try:
            try:
                conn = MysqlUtils.connect()
            except Exception as e:
                logging.error('sss' + str(e))
            query = 'select * from ' + cls.table + ' where com_uni_code=%s and ' + cls.column_end_date + '=%s and ' + cls.column_consolidation + '=%s'
            args = (com_uni_code, end_date, consolidation)

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
    def get_recent_record(cls, com_code='', end_date='', consolidation=1501002):
        """
        Get single one record <=account_date
        :param com_code:
        :param end_date:
        :param consolidation:
        :return: dict
        """
        try:
            conn = MysqlUtils.connect()
            query = 'select * from '+cls.table+' where com_uni_code=%s and consolidation=%s and '+cls.column_end_date+'<=%s order by '+cls.column_end_date+' desc'
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
    def get_records(cls, stock_code='', start_date='', end_date=''):
        """
        Get records from start_date to end_date. eg: 2014-01-01 00:00:00(包含) -- 2017-03-31 00:00:00(包含)

        :param stock_code:
        :param start_date:
        :param end_date:
        :return:
        """
        return cls.get_column_records(stock_code=stock_code, column='*', start_date=start_date, end_date=end_date)

    @classmethod
    def get_column_records(cls, column='', stock_code='', start_date='', end_date=''):
        """
        Get column records from start_date to end_date. eg: 2014-12-31 00:00:00(不包含) -- 2017-03-31 00:00:00(包含)

        :param column: str, list or tuple
        :param stock_code:
        :param start_date:
        :param end_date:
        :return:
        """
        try:
            stock_code = cls.formate_stock_code(stock_code)

            if isinstance(column, list):
                column = tuple(column)
            if isinstance(column, tuple):
                column = MysqlUtils.tuple_to_plaint_sql(column)

            if not isinstance(column, basestring):
                return None

            conn = MysqlUtils.connect()
            query = 'select '+column+' from '+cls.table+' where stock_code=%s and '+cls.column_account_date+'>%s and '+cls.column_account_date+' <=%s order by '+cls.column_account_date
            args = (stock_code, start_date, end_date)

            cursor = MysqlUtils.common_query(conn, query, args)
            rows = cursor.fetchall()

            if not rows or len(rows)==0:
                return None
            else:
                results = []
                for row in rows:
                    if column == '*' or len(column.split(','))>1:
                        results.append(MysqlUtils.sql_row_to_dict(cursor, row))
                    else:
                        results.append(row[0])
                return results

        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    @classmethod
    def get_time_scope(cls, stock_code, day_formate=True):
        """
        获取时间跨度 des排序
        :param stock_code:
        :param day_formate: True(%Y-%m-%d"), False=(%Y-%m-%d %H:%M:%S")
        :return:
        """
        try:
            conn = MysqlUtils.connect()
            query = 'select '+cls.column_account_date+' from '+cls.table+' where stock_code = %s order by '+cls.column_account_date+' desc'
            args = (stock_code,)

            cursor = MysqlUtils.common_query(conn,query,args)
            rows = cursor.fetchall()

            if not rows or len(rows)==0:
                return None

            result=[]
            for row in rows:
                if day_formate:
                    result.append(row[0].strftime("%Y-%m-%d"))
                else:
                    result.append(convert_to_datestr(row[0]))

            return result
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    @classmethod
    def get_time_scope_new(cls, com_code, consolidation, day_formate=True):
        """
        获取时间跨度 des排序
        :param com_code:
        :param consolidation:
        :param day_formate: True(%Y-%m-%d"), False=(%Y-%m-%d %H:%M:%S")
        :return:
        """
        try:
            conn = MysqlUtils.connect()
            query = 'select ' + cls.column_end_date + ' from ' + cls.table + ' where com_uni_code = %s and consolidation = %s order by ' + cls.column_end_date + ' desc'
            args = (com_code, consolidation)

            cursor = MysqlUtils.common_query(conn, query, args)
            rows = cursor.fetchall()

            if not rows or len(rows) == 0:
                return None

            result = []
            for row in rows:
                if day_formate:
                    result.append(row[0].strftime("%Y-%m-%d"))
                else:
                    result.append(convert_to_datestr(row[0]))

            return result
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    @classmethod
    def get_all_stock_codes(cls, table=''):
        """
        获取全部股票代码
        :return:
        """
        try:
            if not table:
                table = cls.table
            if not table or len(table) == 0:
                raise AttributeError

            conn = MysqlUtils.connect()
            query = 'select distinct stock_code from '+table

            cursor = MysqlUtils.common_query(conn=conn,query=query)
            rows = cursor.fetchall()

            if not rows or len(rows) == 0:
                return None

            result = []
            for row in rows:
                if not row[0] or len(row[0].strip())==0:
                    continue
                stock_code = cls.formate_stock_code(row[0].strip())
                result.append(stock_code)

            return result
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    @classmethod
    def get_all_com_codes(cls, table=''):
        """
        获取全部公司代码
        :return:
        """
        try:
            if not table:
                table = cls.table
            if not table or len(table) == 0:
                raise AttributeError

            conn = MysqlUtils.connect()
            query = 'select distinct com_uni_code from ' + table + ' where consolidation = 1501002'

            cursor = MysqlUtils.common_query(conn=conn, query=query)
            rows = cursor.fetchall()

            if not rows or len(rows) == 0:
                return None

            result = []
            for row in rows:
                if not row[0] or len(row) == 0:
                    continue
                com_code = row[0]
                result.append(com_code)

            return result
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    @classmethod
    def get_update_day(cls, day):
        """
        @:param day:
        :return:
        """
        today = datetime.date.today()
        minus_day = datetime.timedelta(days=day)
        want_day = today - minus_day
        want_day = str(want_day) + ' 00:00:00'
        return want_day

    @classmethod
    def get_update_com_codes(cls, table=''):
        """
        获取更新公司代码
        :return:
        """
        try:
            if not table:
                table = cls.table
            if not table or len(table) == 0:
                raise AttributeError

            conn = MysqlUtils.connect()
            query = 'select distinct com_uni_code from ' + table + ' where updatetime > %s'

            updatetime = cls.get_update_day(day=1)

            args = (updatetime,)

            cursor = MysqlUtils.common_query(conn=conn, query=query, args=args)
            rows = cursor.fetchall()

            if not rows or len(rows) == 0:
                return None

            result = []
            for row in rows:
                if not row[0] or len(row) == 0:
                    continue
                com_code = row[0]
                result.append(com_code)

            return result
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()

    """
    TTM, MRQ, LYR, HB, YOY
    """

    @classmethod
    def get_quarter_value(cls, record, year_record, property_name=''):
        """
        :param record:
        :param year_record:
        :param property_name:
        :return:
        """
        try:
            is_first_season = cls.is_first_season(str(record[u'end_date']))
            final_value = get_property_value(record, property_name)
            initial_value = get_property_value(year_record, property_name)
            if not is_first_season:
                return safe_subtract(final_value, initial_value)
            else:
                return final_value
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_safe_quarter_value(cls, record, year_record, property_name=''):
        """
        :param record:
        :param year_record:
        :param property_name:
        :return:
        """
        try:
            is_first_season = cls.is_first_season(str(record[u'end_date']))
            final_value = get_property_value(record, property_name)
            initial_value = get_property_value(year_record, property_name)
            if not is_first_season:
                return safe_zero_subtract(final_value, initial_value)
            else:
                return final_value
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_ttm_value(cls, stock_code='', account_date='', property_name=''):
        """
        获取TTM(最近12个月)

        :param table:
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            (current_record, initial_record, last_record) = cls.get_ttm_records(stock_code, account_date)

            return cls.get_ttm_value_from_records(records=(current_record, initial_record, last_record), property_name=property_name)
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_ttm_records(cls, stock_code='', account_date=''):
        """
        Get TTM Records (当前Record, 期初Record, 上年同期Record)

        :param stock_code: 
        :param account_date: 
        :return: (current_record, initial_record, last_record)
        """
        try:
            (current_record, initial_record, last_record) = (None, None, None)
        
            def output():
                return current_record, initial_record, last_record            

            if not account_date or len(account_date)<10:
                return output()

            # 规整account_date为季度值
            account_date = cls.get_recent_account_date(account_date)

            # 最近季度Record(容忍度:最多取4个季度)
            current_record = cls.get_record(stock_code, account_date, tolerance=4)
            if not current_record:
                return output()

            if current_record and cls.get_report_period(account_date) == 4:
                return output() 

            year = account_date[:4]
            sub_year = account_date[4:]

            # 期初Record
            initial_record = cls.get_record(stock_code, str(int(year)-1)+'-12-31 00:00:00')
            # 上年同期Record
            last_record = cls.get_record(stock_code, str(int(year)-1)+sub_year)

            return output()
        
        except Exception as e:
            logging.error(e)        
        
    @classmethod
    def get_ttm_value_from_records(cls, records=None, property_name=''):
        """
        Get ttm value from ttm records

        :param records:
        :return:
        """
        try:
            if not records or not property_name or len(property_name) == 0:
                return None

            (current_record, initial_record, last_record) = records

            if not current_record:
                return None

            if current_record and not initial_record and not last_record:
                return get_property_value(current_record, property_name)

            return safe_subtract(safe_sum(get_property_value(current_record, property_name), get_property_value(initial_record, property_name)),
                                 get_property_value(last_record, property_name))
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_mrq_value(cls, stock_code='', account_date='', property_name=''):
        """
        获取MRQ(最近一季)
        :param table:
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            initial_record = cls.get_mrq_record(stock_code, account_date)

            return get_property_value(initial_record, property_name)
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_mrq_record(cls, stock_code, account_date):
        """
        :param stock_code:
        :param account_date:
        :return:
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            #规整account_date为季度值
            account_date = cls.get_recent_account_date(account_date)

            initial_record = cls.get_record(stock_code, account_date, tolerance=4)

            return initial_record
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_yoy_rate(cls, stock_code='', account_date='', property_name=''):
        """
        获取同比增长率 例如:(期末营业总收入-期初营业总收入)/期初营业总收入

        期初: 上年同期
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            final_record = cls.get_record(stock_code, account_date)

            year = account_date[:4]
            initial_account_date = str(int(year)-1)+account_date[4:]
            initial_record = cls.get_record(stock_code, initial_account_date)

            final_value = get_property_value(final_record, property_name)
            initial_value = get_property_value(initial_record, property_name)

            return safe_division(safe_subtract(final_value, initial_value), safe_abs(initial_value))

        except Exception as e:
            logging.error(e)

    @classmethod
    def get_yoy_rate_new(cls, record, year_record, property_name=''):
        """
        :param record:
        :param year_record:
        :param property_name:
        :return:
        """
        try:

            final_value = get_property_value(record, property_name)
            initial_value = get_property_value(year_record, property_name)

            return safe_division(safe_subtract(final_value, initial_value), safe_abs(initial_value))

        except Exception as e:
            logging.error(e)

    @classmethod
    def get_lastyear_record(cls, record):
        """
        :param record:
        :return:
        """
        end_date = str(record[u'end_date'])
        initial_account_date = cls.get_yesteryear_account_date(end_date)
        initial_record = cls.get_record_new(record[u'com_uni_code'], initial_account_date, record[u'consolidation'])
        return initial_record

    @classmethod
    def get_hb_rate(cls, stock_code='', account_date='', property_name=''):
        """
        获取HB Rate
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            final_record = cls.get_record(stock_code, account_date)
            initial_record = cls.get_hb_record(stock_code, account_date)

            final_value = get_property_value(final_record, property_name)
            initial_value = get_property_value(initial_record, property_name)

            return safe_division(safe_subtract(final_value, initial_value), safe_abs(initial_value))
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_hb_rate_new(cls, record, last_record, property_name=''):
        """
        获取HB Rate
        :param record:
        :param last_record:
        :param property_name:
        :return:
        """
        try:

            final_value = get_property_value(record, property_name)
            initial_value = get_property_value(last_record, property_name)

            return safe_division(safe_subtract(final_value, initial_value), safe_abs(initial_value))
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_hb_value(cls, stock_code='', account_date='', property_name=''):
        """
        获取HBValue(上一季)
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            initial_record = cls.get_hb_record(stock_code, account_date)

            return get_property_value(initial_record, property_name)
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_hb_record(cls, stock_code, account_date):
        """
        获取HB Record
        :param stock_code:
        :param account_date:
        :return:
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            account_date = cls.get_last_account_date(account_date)
            initial_record = cls.get_record(stock_code, account_date)

            return initial_record
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_lyr_value(cls, stock_code='', account_date='', property_name=''):
        """
        获取LYR(最近年度)
        :param table:
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            initial_record = cls.get_lyr_record(stock_code, account_date)
            return get_property_value(initial_record, property_name)
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_lyr_record(cls, stock_code='', account_date=''):
        """
        :param stock_code:
        :param account_date:
        :return:
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            #规整account_date为季度值
            account_date = cls.get_recent_account_date(account_date)

            if account_date[4:] == '-12-31 00:00:00':
                initial_record = cls.get_record(stock_code, account_date)
                return initial_record

            year = account_date[:4]
            #LastYearRecord
            initial_record = cls.get_record(stock_code, str(int(year)-1)+'-12-31 00:00:00')

            #容忍度:最多取掉第二年
            if not initial_record:
                initial_record = cls.get_record(stock_code, str(int(year)-2)+'-12-31 00:00:00')

            return initial_record
        except Exception as e:
            logging.error(e)

    @classmethod
    def get_initial_value(cls, stock_code='', account_date='', property_name=''):
        """
        获取期初数据(上一年年度)
        :param table:
        :param stock_code:
        :param account_date:
        :param property_name:
        :return:
        """
        try:
            initial_record = cls.get_initial_record(stock_code, account_date)
            return get_property_value(initial_record, property_name)
        except Exception as e:
            logging.error(e)      
            
    @classmethod
    def get_initial_record(cls, stock_code, account_date):
        """
        获取期初Record
        :param stock_code: 
        :param account_date: 
        :return: 
        """
        try:
            if not account_date or len(account_date)<10:
                return None

            year = account_date[:4]
            #LastYearRecord
            initial_record = cls.get_record(stock_code, str(int(year)-1)+'-12-31 00:00:00')

            return initial_record
        except Exception as e:
            logging.error(e)


    ######################
    #
    #       相关工具集
    #
    ######################
    @staticmethod
    @typeassert(stock_code=basestring)
    def formate_stock_code(stock_code):
        """
        格式化StockCode
        :param stock_code:
        :return:
        """
        return stock_code

        # if not stock_code or len(stock_code.strip())<5:
        #     return None
        # stock_code = stock_code.strip()
        #
        # #remove .suffix
        # if len(stock_code.split('.'))>=2:
        #     stock_code = ''.join(stock_code.split('.')[:-1])
        #
        # #remove prefix
        # prefix = stock_code[0:2]
        # if prefix.upper() in ['SH', 'SZ', 'OC', 'HK']:
        #     stock_code = stock_code[2:]
        #
        # return stock_code

    @staticmethod
    @typeassert(com_code=basestring)
    def formate_com_code(com_code):
        """
        格式化ComCode
        :param com_code:
        :return:
        """
        return com_code

        # if not com_code or len(com_code.strip())<5:
        #     return None
        # com_code = com_code.strip()
        #
        # #remove .suffix
        # if len(com_code.split('.'))>=2:
        #     com_code = ''.join(com_code.split('.')[:-1])
        #
        # #remove prefix
        # prefix = com_code[0:2]
        # if prefix.upper() in ['SH', 'SZ', 'OC', 'HK']:
        #     com_code = com_code[2:]
        #
        # return com_code

    @staticmethod
    def get_report_period(account_date=''):
        """
        获取报告期 03-31,06-30,09-30,12-31分别对应1,2,3,4季度

        :param account_date:
        :return:
        """
        if not account_date or len(account_date)<10:
            return None

        month_day = account_date[5:10]
        if month_day == '03-31':
            return 1
        elif month_day == '06-30':
            return 2
        elif month_day == '09-30':
            return 3
        elif month_day == '12-31':
            return 4
        else:
            return 0

    @staticmethod
    def get_recent_account_date(account_date):
        """
        获取最近的季度时间
        :param account_date:
        :return:
        """
        if not account_date or len(account_date)<10:
            return None

        year = account_date[:4]
        month_day = account_date[5:10]

        if month_day in ['03-31','06-30','09-30','12-31']:
            return year+'-'+month_day+' 00:00:00'
        else:
            return BaseStockSheet.get_last_account_date(account_date)

    @staticmethod
    def get_recent_report_period(account_date):
        """
        获取最近的季度
        :param account_date:
        :return:
        """
        if not account_date or len(account_date)<10:
            return None

        month = account_date[5:7]
        month_day = account_date[5:10]
        int_month = int(month)

        if month_day in ['03-31','06-30','09-30','12-31']:
            if int_month<4:
                return 1
            elif int_month>=4 and int_month<7:
                return 2
            elif int_month>=7 and int_month<10:
                return 3
            else:
                return 4
        else:
            return BaseStockSheet.get_last_report_period(account_date)

    @staticmethod
    def is_first_season(account_date):
        """
        是否是第一季度
        :param account_date:
        :return:
        """
        if not account_date or len(account_date)<10:
            return False

        month_str = account_date[5:]

        return month_str == '03-31 00:00:00'

    @staticmethod
    def get_yesteryear_account_date(account_date=''):
        """
        获取上一年的同一时间
        :param account_date:
        :return:
        """

        if not account_date or len(account_date) < 10:
            return None

        year = account_date[:4]
        return str(int(year)-1)+account_date[4:]

    @staticmethod
    def get_last_account_date(account_date):
        """
        获取上一个季度的时间
        :param account_date:
        :return:
        """
        if not account_date or len(account_date) < 10:
            return None

        year = account_date[:4]
        month = account_date[5:7]
        int_month = int(month)

        if int_month < 4:
            return str(int(year)-1)+'-12-31 00:00:00'
        elif 4 <= int_month < 7:
            return year+'-03-31 00:00:00'
        elif 7 <= int_month < 10:
            return year+'-06-30 00:00:00'
        else:
            return year+'-09-30 00:00:00'

    @staticmethod
    def get_last_report_period(account_date):
        """
        获取上一个季度
        :param account_date:
        :return:
        """
        if not account_date or len(account_date)<10:
            return None

        month = int(account_date[5:7])

        if month < 4:
            return 4
        elif 4 <= month < 7:
            return 1
        elif 7 <= month < 10:
            return 2
        else:
            return 3

    @staticmethod
    def get_start_date(account_date='', scope=''):
        """
        Get start date before account_date in scope
        :param account_date:
        :param scope:'season'= 3 month 'year'= 12 month
        :return:
        """
        if scope not in ['season', 'year']:
             raise Exception('param error: scope')

        year = account_date[:4]
        month = account_date[5:7]

        start_date = account_date
        if scope == 'year':
            start_date = str(int(year)-1)+account_date[4:]
        elif scope == 'season':
            start_date = BaseStockSheet.get_last_account_date(account_date)

        return start_date
