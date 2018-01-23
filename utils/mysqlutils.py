#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import mysql
from mysql.connector import Error, MySQLConnection

from config.config import configs
from utils.commontools import safe_dict_value
from utils.middlewares import *


class MysqlUtils(object):
    """
    Mysql 工具类

    """

    @staticmethod
    def connect():
        try:
            conf = configs['mysql']
            conn = mysql.connector.connect(host=conf['host'],
                                           database=conf['database'],
                                           port=conf['port'],
                                           user=conf['user'],
                                           password=conf['password'])
            conn.set_converter_class(NumpyMySQLConverter)

            if conn.is_connected():
                #logging.warning('Connected to MySQL database')
                pass
            return conn
        except Error as e:
            logging.error(e)
        finally:
            #logging.warning('Connect to MySQL database colse')
            pass

    """
    工具类方法

    """
    @staticmethod
    @typeassert(conn=MySQLConnection,query=str,args=tuple)
    def common_query(conn=MySQLConnection(), query='', args=()):
        """
        Common Query

        :param conn:
        :param query:
        :param args:
        :return: cursor
        """
        if not conn or not query:
            raise TypeError

        try:
            cursor = conn.cursor(buffered=True)
            if args:
                cursor.execute(query, args)
            else:
                cursor.execute(query)
            return cursor
        except Exception as error:
            logging.warning('query error')
            logging.warning(error)
        finally:
            ##cursor.close()()
            pass

    @staticmethod
    @typeassert(conn=MySQLConnection,table=str)
    def select_all(conn=None,table=''):
        """
        Select all table data to array of dict_item

        :param conn:
        :param table:
        :return:
        """
        if not conn or not table:
            raise TypeError

        cursor = MysqlUtils.common_query(conn=conn,query="select * from "+table)
        rows = cursor.fetchall()
        dict_rows = []
        for row in rows:
            dict_item = MysqlUtils.sql_row_to_dict(cursor=cursor,row=row)
            dict_rows.append(dict_item)
        return dict_rows

    @staticmethod
    @typeassert(conn=MySQLConnection,table=str,column=str)
    def column_select(conn=None,table='',column='',value=None):
        if not conn or not table or not column or not value:
            raise RuntimeError

        try:
            query = 'SELECT * FROM '+table+' WHERE '+column+'=%s'
            args = (value,)
            cursor = MysqlUtils.common_query(conn,query,args)
            return cursor
        except Error as error:
            logging.warning('find all error')
            logging.warning(error)
        finally:
            # #cursor.close()()
            pass

    @staticmethod
    @typeassert(conn=MySQLConnection, table=str, column=str)
    def column_select_new(conn=None,table='',column='',value=None):
        if not conn or not table or not column or not value:
            raise RuntimeError

        try:
            query = 'SELECT * FROM '+table+' WHERE '+column+'=%s' + ' order by end_date desc'
            args = (value,)
            cursor = MysqlUtils.common_query(conn,query,args)
            return cursor
        except Error as error:
            logging.warning('find all error')
            logging.warning(error)
        finally:
            # #cursor.close()()
            pass

    @staticmethod
    @typeassert(table=str,column=str)
    def column_insert(self,table,column,value):
        if not value:
            raise TypeError

        conn = self.conn
        cursor = self.column_select(conn=conn,table=table,column=column,value=value)
        if cursor.fetchone():
            logging.warning('already exists on insert id', cursor.lastrowid)
        else:
            self.columns_insert(conn=conn,table=table,columns=(column,),values=(value,))

    @staticmethod
    @typeassert(conn=MySQLConnection,table=str,columns=tuple,values=tuple)
    def columns_insert(conn=MySQLConnection(),table='',columns=(),values=()):
        if not conn or not table or not columns or not values:
            raise TypeError

        try:
            columns_sql = MysqlUtils.tuple_to_sql(columns)
            value_sql = MysqlUtils.multiple_str(item='%s',mutiple=len(columns))

            query = 'INSERT INTO '+table+columns_sql+' VALUES('+value_sql+')'
            args = values
            cursor = MysqlUtils.common_query(conn,query,args)
            if cursor.lastrowid:
                logging.warning('last insert id %s', cursor.lastrowid)
            else:
                logging.warning('last insert id not found')

            conn.commit()
            return cursor
        except Exception as error:
            logging.warning('columns insert error')
            logging.warning(error)
        finally:
            #cursor.close()
            pass

    @staticmethod
    @typeassert(conn=MySQLConnection,table=str,columns=tuple,values=tuple)
    def columns_update(conn=MySQLConnection(),table='',columns=(),values=(),select_column='',select_value=None):
        if not conn or not table or not columns or not values:
            raise TypeError

        try:
            columns_sql = MysqlUtils.tuple_to_update_sql(columns)
            query = 'UPDATE '+table+' SET '+columns_sql+' WHERE '+select_column+'=%s'
            args = tuple(list(values)+[select_value])

            cursor = MysqlUtils.common_query(conn,query,args)
            if cursor.lastrowid:
                logging.warning('last insert id %s', cursor.lastrowid)
            else:
                logging.warning('last insert id not found')

            conn.commit()
            return cursor
        except Exception as error:
            logging.warning('columns insert error')
            logging.warning(error)
        finally:
            #cursor.close()
            pass

    """
    sql拼装
    """
    @staticmethod
    @typeassert(obj=tuple)
    def tuple_to_sql(obj=()):
        """
         Tuple to sql like ('a','b','c')=>"(a,b,c)"

        :param obj:
        :return:
        """
        return str(obj).replace('\'','')

    @staticmethod
    @typeassert(obj=tuple)
    def tuple_to_plaint_sql(obj=()):
        """
         Tuple to sql like ('a','b','c')=>"a,b,c"

        :param obj:
        :return:
        """
        return str(obj).replace('\'','').replace('(','').replace(')','')

    @staticmethod
    def multiple_str(item='', mutiple=0):
        """
        Create str like "%,%@,%@"

        :param item:
        :param mutiple:
        :return: item,item,item
        """
        m = []
        m.extend([item]*mutiple)
        return ','.join(m)

    @staticmethod
    def tuple_to_update_sql(obj=()):
        """
        Tuple to update sql like ('a','b','c')=>"a=%,b=%,c=%"
        :return:
        """
        new_sql=[]
        for column in obj:
            if isinstance(column, unicode):
                column = str(column)
            new_sql.append(column+'=%s')

        new_sql = tuple(new_sql)
        result_sql = str(new_sql).replace('\'','').replace('(','').replace(')','')

        if result_sql.endswith(','):
            result_sql = result_sql[:-1]

        return result_sql


    @staticmethod
    def sql_row_to_dict(cursor=None,row=None):
        """
        Sql result row to dict

        :param cursor:
        :param row:
        :return:
        """
        try:
            aResult = {}
            columns = [desc[0] for desc in cursor.description]
            for k, v in zip(columns, row):
                aResult[ k ] = v
            return aResult
        except Error as e:
            logging.error(e)

    @staticmethod
    @typeassert(items=list,column=str)
    def filter_column_items(items=[],column='',value=''):
        """
        过滤出items中 item[column]=value的所有项

        :param items:
        :param column:
        :param value:
        :return:
        """
        if not items or len(items) == 0:
            return []

        if not column or len(column) == 0:
            return []

        result = [];
        for item in items:
            matching_value = safe_dict_value(item=item,key=column,default='')
            if matching_value == value:
                result.append(item)
                continue
        return result

    @staticmethod
    @typeassert(items=list,source_key=str,target_key=str)
    def list_match_value_with_items(value=None,items=[],source_key='',target_key='',like=False):
        """
        match_value_with_items 兼容value为'value,value...'的场景

        """
        if not items or not source_key or not target_key:
            raise TypeError

        if not value or len(value)==0:
            return ''

        if len(value.split(',')) <= 1:
            return MysqlUtils.match_value_with_items(value=value,items=items,source_key=source_key,target_key=target_key,like=like)

        result = ''
        for single_value in value.split(','):
            result = MysqlUtils.match_value_with_items(value=single_value.strip(),items=items,source_key=source_key,target_key=target_key,like=like)
            if len(result) >0 :
                break
        return result

    @staticmethod
    @typeassert(items=list,source_key=str,target_key=str)
    def match_value_with_items(value=None,items=[],source_key='',target_key='',like=False):
        """
        Math value with item[source_key] return item[target_key] or ''

        :param value:
        :param items:
        :param source_key:
        :param target_key:
        :param like true for like match or equal match
        :return:
        """
        if not items or not source_key or not target_key:
            raise TypeError

        if not value or len(value)==0:
            return ''

        _has_match_value = False
        for item in items:
            matching_value = safe_dict_value(item=item,key=source_key,default='')
            if len(matching_value) == 0:
                continue

            if like:
                _has_match_value = matching_value.find(value)>=0
            else:
                _has_match_value = (matching_value == value)

            if _has_match_value:
                break

        if _has_match_value:
            return safe_dict_value(item=item,key=target_key,default='')
        return ''


class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)
