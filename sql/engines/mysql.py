# -*- coding: UTF-8 -*-
import logging
import traceback
import MySQLdb
import re

import schemaobject
import sqlparse
from MySQLdb.constants import FIELD_TYPE
from schemaobject.connection import build_database_url

from sql.engines.goinception import GoInceptionEngine
from sql.utils.sql_utils import get_syntax_type, remove_comments
from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet
from .inception import InceptionEngine
from sql.utils.data_masking import data_masking
from common.config import SysConfig

logger = logging.getLogger('default')


class MysqlEngine(EngineBase):

    def __init__(self, instance=None):
        super().__init__(instance=instance)
        self.config = SysConfig()
        self.inc_engine = InceptionEngine() if self.config.get('inception') else GoInceptionEngine()

    def get_connection(self, db_name=None):
        # https://stackoverflow.com/questions/19256155/python-mysqldb-returning-x01-for-bit-values
        conversions = MySQLdb.converters.conversions
        conversions[FIELD_TYPE.BIT] = lambda data: data == b'\x01'
        if self.conn:
            self.thread_id = self.conn.thread_id()
            return self.conn
        if db_name:
            self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        db=db_name, charset=self.instance.charset or 'utf8mb4',
                                        conv=conversions,
                                        connect_timeout=10)
        else:
            self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        charset=self.instance.charset or 'utf8mb4',
                                        conv=conversions,
                                        connect_timeout=10)
        self.thread_id = self.conn.thread_id()
        return self.conn

    @property
    def name(self):
        return 'MySQL'

    @property
    def info(self):
        return 'MySQL engine'

    @property
    def auto_backup(self):
        """??????????????????"""
        return True

    @property
    def seconds_behind_master(self):
        slave_status = self.query(sql='show slave status', close_conn=False, cursorclass=MySQLdb.cursors.DictCursor)
        return slave_status.rows[0].get('Seconds_Behind_Master') if slave_status.rows else None

    @property
    def server_version(self):
        def numeric_part(s):
            """Returns the leading numeric part of a string.
            """
            re_numeric_part = re.compile(r"^(\d+)")
            m = re_numeric_part.match(s)
            if m:
                return int(m.group(1))
            return None

        self.get_connection()
        version = self.conn.get_server_info()
        return tuple([numeric_part(n) for n in version.split('.')[:3]])

    @property
    def schema_object(self):
        """????????????????????????"""
        url = build_database_url(host=self.host,
                                 username=self.user,
                                 password=self.password,
                                 port=self.port)
        return schemaobject.SchemaObject(url, charset=self.instance.charset or 'utf8mb4')

    def kill_connection(self, thread_id):
        """?????????????????????"""
        self.query(sql=f'kill {thread_id}')

    def get_all_databases(self):
        """?????????????????????, ????????????ResultSet"""
        sql = "show databases"
        result = self.query(sql=sql)
        db_list = [row[0] for row in result.rows
                   if row[0] not in ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """??????table ??????, ????????????ResultSet"""
        sql = "show tables"
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows if row[0] not in ['test']]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """??????????????????, ????????????ResultSet"""
        sql = f"""SELECT 
            COLUMN_NAME,
            COLUMN_TYPE,
            CHARACTER_SET_NAME,
            IS_NULLABLE,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{tb_name}'
        ORDER BY ORDINAL_POSITION;"""
        result = self.query(db_name=db_name, sql=sql)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet ????????????"""
        sql = f"show create table `{tb_name}`;"
        result = self.query(db_name=db_name, sql=sql)
        return result

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True, **kwargs):
        """?????? ResultSet """
        result_set = ResultSet(full_sql=sql)
        max_execution_time = kwargs.get('max_execution_time', 0)
        cursorclass = kwargs.get('cursorclass') or MySQLdb.cursors.Cursor
        try:
            conn = self.get_connection(db_name=db_name)
            conn.autocommit(True)
            cursor = conn.cursor(cursorclass)
            try:
                cursor.execute(f"set session max_execution_time={max_execution_time};")
            except MySQLdb.OperationalError:
                pass
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = rows
            result_set.affected_rows = effect_row
        except Exception as e:
            logger.warning(f"MySQL??????????????????????????????{sql}???????????????{traceback.format_exc()}")
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_check(self, db_name=None, sql=''):
        # ?????????????????????????????????????????????
        result = {'msg': '', 'bad_query': False, 'filtered_sql': sql, 'has_star': False}
        # ???????????????????????????????????????????????????????????????sql
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result['filtered_sql'] = sql.strip()
        except IndexError:
            result['bad_query'] = True
            result['msg'] = '???????????????SQL??????'
        if re.match(r"^select|^show|^explain", sql, re.I) is None:
            result['bad_query'] = True
            result['msg'] = '??????????????????????????????!'
        if '*' in sql:
            result['has_star'] = True
            result['msg'] = 'SQL??????????????? * '
        # select???????????????Explain????????????????????????
        if re.match(r"^select", sql, re.I):
            explain_result = self.query(db_name=db_name, sql=f"explain {sql}")
            if explain_result.error:
                result['bad_query'] = True
                result['msg'] = explain_result.error
        return result

    def filter_sql(self, sql='', limit_num=0):
        # ?????????sql??????limit??????,limit n ??? limit n,n ??? limit n offset n???????????????limit n
        sql = sql.rstrip(';').strip()
        if re.match(r"^select", sql, re.I):
            # LIMIT N
            limit_n = re.compile(r'limit([\s]*\d+[\s]*)$', re.I)
            # LIMIT N, N ???LIMIT N OFFSET N
            limit_offset = re.compile(r'limit([\s]*\d+[\s]*)(,|offset)([\s]*\d+[\s]*)$', re.I)
            if limit_n.search(sql):
                sql_limit = limit_n.search(sql).group(1)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_n.sub(f'limit {limit_num};', sql)
            elif limit_offset.search(sql):
                sql_limit = limit_offset.search(sql).group(3)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_offset.sub(f'limit {limit_num};', sql)
            else:
                sql = f'{sql} limit {limit_num};'
        else:
            sql = f'{sql};'
        return sql

    def query_masking(self, db_name=None, sql='', resultset=None):
        """?????? sql??????, db???, ?????????,
        ?????????????????????????????????"""
        # ??????select????????????
        if re.match(r"^select", sql, re.I):
            mask_result = data_masking(self.instance, db_name, sql, resultset)
        else:
            mask_result = resultset
        return mask_result

    def execute_check(self, db_name=None, sql=''):
        """???????????????????????????, ??????Review set"""
        # ??????Inception???????????????????????????
        try:
            inc_check_result = self.inc_engine.execute_check(instance=self.instance, db_name=db_name, sql=sql)
        except Exception as e:
            logger.debug(f"{self.inc_engine.name}?????????????????????????????????{traceback.format_exc()}")
            raise RuntimeError(f"{self.inc_engine.name}???????????????????????????????????????????????????{self.inc_engine.name}????????????????????????\n{e}")

        # ??????Inception????????????
        if inc_check_result.error:
            logger.debug(f"{self.inc_engine.name}?????????????????????????????????{inc_check_result.error}")
            raise RuntimeError(f"{self.inc_engine.name}????????????????????????????????????\n{inc_check_result.error}")

        # ??????/??????????????????
        check_critical_result = ReviewSet(full_sql=sql)
        line = 1
        critical_ddl_regex = self.config.get('critical_ddl_regex', '')
        p = re.compile(critical_ddl_regex)
        check_critical_result.syntax_type = 2  # TODO ???????????? 0????????? 1???DDL???2???DML

        for row in inc_check_result.rows:
            statement = row.sql
            # ????????????
            statement = remove_comments(statement, db_type='mysql')
            # ????????????
            if re.match(r"^select", statement.lower()):
                check_critical_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='?????????????????????',
                                      errormessage='?????????DML???DDL??????????????????????????????SQL???????????????',
                                      sql=statement)
            # ????????????
            elif critical_ddl_regex and p.match(statement.strip().lower()):
                check_critical_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='????????????SQL',
                                      errormessage='??????????????????' + critical_ddl_regex + '??????????????????',
                                      sql=statement)
            # ????????????
            else:
                result = ReviewResult(id=line, errlevel=0,
                                      stagestatus='Audit completed',
                                      errormessage='None',
                                      sql=statement,
                                      affected_rows=0,
                                      execute_time=0, )

            # ????????????DDL?????????????????????????????????
            if check_critical_result.syntax_type == 2:
                if get_syntax_type(statement, parser=False, db_type='mysql') == 'DDL':
                    check_critical_result.syntax_type = 1
            check_critical_result.rows += [result]

            # ???????????????????????????????????????
            if check_critical_result.is_critical:
                check_critical_result.error_count += 1
                return check_critical_result
            line += 1
        return inc_check_result

    def execute_workflow(self, workflow):
        """????????????????????????Review set"""
        # ????????????????????????
        read_only = self.query(sql='SELECT @@global.read_only;').rows[0][0]
        if read_only in (1, 'ON'):
            result = ReviewSet(
                full_sql=workflow.sqlworkflowcontent.sql_content,
                rows=[ReviewResult(id=1, errlevel=2,
                                   stagestatus='Execute Failed',
                                   errormessage='??????read_only=1???????????????????????????!',
                                   sql=workflow.sqlworkflowcontent.sql_content)])
            result.error = '??????read_only=1???????????????????????????!',
            return result
        # TODO ????????????
        # if workflow.is_manual == 1:
        #     return self.execute(db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content)
        # inception??????
        return self.inc_engine.execute(workflow)

    def execute(self, db_name=None, sql='', close_conn=True):
        """??????????????????"""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                cursor.execute(statement)
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"MySQL??????????????????????????????{sql}???????????????{traceback.format_exc()}")
            result.error = str(e)
        if close_conn:
            self.close()
        return result

    def get_rollback(self, workflow):
        """??????inception????????????????????????"""
        inception_engine = InceptionEngine()
        return inception_engine.get_rollback(workflow)

    def get_variables(self, variables=None):
        """??????????????????"""
        if variables:
            variables = "','".join(variables) if isinstance(variables, list) else "','".join(list(variables))
            db = 'performance_schema' if self.server_version > (5, 7) else 'information_schema'
            sql = f"""select * from {db}.global_variables where variable_name in ('{variables}');"""
        else:
            sql = "show global variables;"
        return self.query(sql=sql)

    def set_variable(self, variable_name, variable_value):
        """?????????????????????"""
        sql = f"""set global {variable_name}={variable_value};"""
        return self.query(sql=sql)

    def osc_control(self, **kwargs):
        """??????osc???????????????????????????????????????????????????
            get???kill???pause???resume
        """
        return self.inc_engine.osc_control(**kwargs)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
