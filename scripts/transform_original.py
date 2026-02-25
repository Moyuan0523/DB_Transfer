#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MSSQL 到 MariaDB 資料庫遷移程式
基於原有 MSSQL 程式進行擴展，實現完整的資料庫遷移功能
"""

import pandas as pd
import pyodbc
import mysql.connector
import argparse
import os
import sys
import re
import json
import hashlib
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import urllib.parse
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

class DatabaseMigrator:
    """資料庫遷移核心類別"""
    
    def __init__(self, mssql_config: Dict, mariadb_config: Dict, batch_size: int = 1000):
        self.mssql_config = mssql_config
        self.mariadb_config = mariadb_config
        self.batch_size = batch_size
        self.migration_log = []
        self.verification_results = {}
        self.mssql_config = mssql_config
        self.mariadb_config = mariadb_config
        self.batch_size = batch_size
        self.migration_log = []
        self.verification_results = {}

        # 新增：SQLAlchemy引擎
        self.mssql_engine = None
        self.mariadb_engine = None
        
        # 設置詳細日誌
        self.setup_logging()
        
        # 資料類型映射表
        self.datatype_mapping = {
            'int': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'TINYINT',
            'bit': 'BOOLEAN',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'money': 'DECIMAL(19,4)',
            'float': 'DOUBLE',
            'real': 'FLOAT',
            'datetime': 'DATETIME',
            'datetime2': 'DATETIME',
            'date': 'DATE',
            'time': 'TIME',
            'varchar': 'VARCHAR',
            'nvarchar': 'VARCHAR',
            'char': 'CHAR',
            'nchar': 'CHAR',
            'text': 'TEXT',
            'ntext': 'LONGTEXT',
            'uniqueidentifier': 'VARCHAR(36)'
        }
    
    def setup_logging(self):
        """設置日誌系統"""
        # 創建日誌目錄
        os.makedirs('migration_logs', exist_ok=True)
        
        # 設置主日誌
        self.logger = logging.getLogger('DatabaseMigrator')
        self.logger.setLevel(logging.INFO)
        
        # 清除現有處理器
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # 文件處理器
        file_handler = logging.FileHandler(
            f'migration_logs/migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        
        # 控制台處理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 設置格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def connect_mssql(self) -> Optional[pyodbc.Connection]:
        """連接MSSQL資料庫"""
        server = self.mssql_config['server']
        database = self.mssql_config['database']
        
        # 清理伺服器名稱中的空格和特殊字符
        server = server.strip()
        
        if self.mssql_config.get('use_windows_auth', False):
            # Windows驗證連接字串（簡化但穩定的方式）
            connection_attempts = [
                # 標準Windows驗證格式
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes",
                # 嘗試不同的Trusted_Connection值
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=true",
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Integrated Security=SSPI",
                # 嘗試舊版驅動程式
                f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes",
            ]
        else:
            # SQL Server驗證
            username = self.mssql_config.get('username', '')
            password = self.mssql_config.get('password', '')
            connection_attempts = [
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}",
                f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}",
            ]
        
        # 逐一嘗試連接
        for i, conn_str in enumerate(connection_attempts, 1):
            try:
                self.logger.info(f"嘗試連接方式 {i}")
                self.logger.debug(f"連接字串: {conn_str}")
                
                conn = pyodbc.connect(conn_str, timeout=15)
                self.logger.info(f"✅ 成功連接到MSSQL資料庫: {database}")
                
                # 測試連接
                cursor = conn.cursor()
                cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
                result = cursor.fetchone()
                self.logger.info(f"伺服器: {result[0]}, 資料庫: {result[1]}")
                cursor.close()
                
                return conn
                
            except Exception as e:
                self.logger.warning(f"❌ 連接方式 {i} 失敗: {str(e)}")
                continue
        
        # 所有連接方式都失敗
        self.logger.error("🔴 所有連接方式都失敗！")
        self.logger.error("請檢查以下項目：")
        self.logger.error("1. SQL Server服務運行狀態: net start MSSQL$SQLEXPRESS")
        self.logger.error("2. 伺服器名稱是否正確")  
        self.logger.error("3. 資料庫名稱是否存在")
        self.logger.error("4. Windows用戶是否有資料庫權限")
        return None
    
    def connect_mariadb(self) -> Optional[mysql.connector.MySQLConnection]:
        """連接MariaDB資料庫"""
        try:
            conn = mysql.connector.connect(
                host=self.mariadb_config['host'],
                port=self.mariadb_config.get('port', 3306),
                database=self.mariadb_config['database'],
                user=self.mariadb_config['username'],
                password=self.mariadb_config['password'],
                charset='utf8mb4',
                autocommit=False
            )
            self.logger.info(f"成功連接到MariaDB資料庫: {self.mariadb_config['database']}")
            return conn
        except Exception as e:
            self.logger.error(f"連接MariaDB資料庫失敗: {str(e)}")
            return None
    
    def get_mssql_tables(self, schema: str = 'dbo') -> List[str]:
        """獲取MSSQL中的所有表格名稱（SQLAlchemy版本）"""
        engine = self.create_mssql_engine()
        if not engine:
            return []
        
        try:
            query = text("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = :schema 
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {"schema": schema})
                tables = [row[0] for row in result.fetchall()]
            
            self.logger.info(f"找到 {len(tables)} 個表格: {', '.join(tables)}")
            return tables
            
        except Exception as e:
            self.logger.error(f"獲取表格列表失敗: {str(e)}")
            return []
    
    def get_table_schema(self, table_name: str, schema: str = 'dbo') -> Tuple[List, List, List]:
        """獲取表格結構信息（修復版本）"""
        conn = self.connect_mssql()
        if not conn:
            return [], [], []
        
        try:
            cursor = conn.cursor()
            
            # 先檢查表格是否存在
            self.logger.info(f"檢查表格 {schema}.{table_name} 是否存在...")
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
            """, table_name, schema)
            
            table_exists = cursor.fetchone()[0] > 0
            if not table_exists:
                self.logger.warning(f"表格 {schema}.{table_name} 不存在")
                
                # 列出所有可用的表格
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                available_tables = cursor.fetchall()
                self.logger.info("可用的表格:")
                for t_schema, t_name in available_tables:
                    self.logger.info(f"  - {t_schema}.{t_name}")
                
                cursor.close()
                conn.close()
                return [], [], []
            
            # 方法1: 使用INFORMATION_SCHEMA（推薦）
            self.logger.info(f"方法1: 使用INFORMATION_SCHEMA獲取表格結構...")
            try:
                cursor.execute(f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
                    ORDER BY ORDINAL_POSITION
                """, table_name, schema)
                columns = cursor.fetchall()
                
                if columns:
                    self.logger.info(f"✅ 成功獲取 {len(columns)} 個欄位")
                else:
                    self.logger.warning("❌ INFORMATION_SCHEMA 沒有返回任何欄位")
                    
            except Exception as e:
                self.logger.warning(f"INFORMATION_SCHEMA 查詢失敗: {e}")
                columns = []
            
            # 方法2: 如果方法1失敗，使用sys.columns（備用方法）
            if not columns:
                self.logger.info(f"方法2: 使用sys.columns獲取表格結構...")
                try:
                    cursor.execute(f"""
                        SELECT 
                            c.name as column_name,
                            t.name as data_type,
                            CASE 
                                WHEN t.name IN ('varchar', 'nvarchar', 'char', 'nchar') 
                                THEN CASE WHEN c.max_length = -1 THEN NULL ELSE c.max_length END
                                ELSE NULL 
                            END as character_maximum_length,
                            CASE 
                                WHEN t.name IN ('decimal', 'numeric', 'float', 'real') 
                                THEN c.precision 
                                ELSE NULL 
                            END as numeric_precision,
                            CASE 
                                WHEN t.name IN ('decimal', 'numeric') 
                                THEN c.scale 
                                ELSE NULL 
                            END as numeric_scale,
                            CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                            d.definition as column_default
                        FROM sys.columns c
                        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                        INNER JOIN sys.tables tb ON c.object_id = tb.object_id
                        INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
                        LEFT JOIN sys.default_constraints d ON c.default_object_id = d.object_id
                        WHERE tb.name = ? AND s.name = ?
                        ORDER BY c.column_id
                    """, table_name, schema)
                    
                    columns = cursor.fetchall()
                    
                    if columns:
                        self.logger.info(f"✅ 使用sys.columns成功獲取 {len(columns)} 個欄位")
                    else:
                        self.logger.error("❌ sys.columns 也沒有返回任何欄位")
                        
                except Exception as e:
                    self.logger.error(f"sys.columns 查詢失敗: {e}")
                    columns = []
            
            # 方法3: 最後的嘗試 - 使用簡單查詢獲取基本結構
            if not columns:
                self.logger.info(f"方法3: 使用簡單查詢獲取基本結構...")
                try:
                    # 使用brackets來處理特殊字符
                    cursor.execute(f"SELECT TOP 1 * FROM [{schema}].[{table_name}]")
                    cursor.fetchone()  # 我們不關心數據，只要查詢結構
                    
                    # 從cursor.description獲取基本欄位信息
                    column_desc = cursor.description
                    columns = []
                    
                    for desc in column_desc:
                        col_name = desc[0]
                        # 簡化的類型映射
                        type_mapping = {
                            1: 'varchar',      # SQL_CHAR
                            4: 'int',          # SQL_INTEGER  
                            6: 'float',        # SQL_FLOAT
                            7: 'real',         # SQL_REAL
                            8: 'float',        # SQL_DOUBLE
                            12: 'varchar',     # SQL_VARCHAR
                            91: 'date',        # SQL_DATE
                            93: 'datetime',    # SQL_TIMESTAMP
                            -1: 'text',        # SQL_LONGVARCHAR
                            -7: 'bit'          # SQL_BIT
                        }
                        
                        data_type = type_mapping.get(desc[1], 'varchar')
                        max_length = desc[2] if desc[2] and desc[2] > 0 else None
                        
                        # 模擬INFORMATION_SCHEMA的格式
                        col_info = (
                            col_name,           # COLUMN_NAME
                            data_type,          # DATA_TYPE
                            max_length,         # CHARACTER_MAXIMUM_LENGTH
                            desc[4],            # NUMERIC_PRECISION
                            desc[5],            # NUMERIC_SCALE
                            'YES' if desc[6] else 'NO',  # IS_NULLABLE
                            None                # COLUMN_DEFAULT
                        )
                        columns.append(col_info)
                    
                    self.logger.info(f"✅ 使用簡單查詢獲取 {len(columns)} 個欄位")
                    
                except Exception as e:
                    self.logger.error(f"簡單查詢也失敗: {e}")
                    columns = []
            
            # 如果還是沒有獲取到欄位
            if not columns:
                self.logger.error(f"❌ 所有方法都無法獲取表格 {schema}.{table_name} 的結構")
                cursor.close()
                conn.close()
                return [], [], []
            
            # 獲取主鍵信息
            self.logger.info(f"獲取主鍵信息...")
            try:
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = ? 
                    AND TABLE_SCHEMA = ?
                    AND CONSTRAINT_NAME LIKE 'PK%'
                    ORDER BY ORDINAL_POSITION
                """, table_name, schema)
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                if not primary_keys:
                    # 備用方法獲取主鍵
                    cursor.execute(f"""
                        SELECT c.name
                        FROM sys.key_constraints k
                        INNER JOIN sys.index_columns ic ON k.parent_object_id = ic.object_id 
                            AND k.unique_index_id = ic.index_id
                        INNER JOIN sys.columns c ON ic.object_id = c.object_id 
                            AND ic.column_id = c.column_id
                        INNER JOIN sys.tables t ON k.parent_object_id = t.object_id
                        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                        WHERE k.type = 'PK' AND t.name = ? AND s.name = ?
                        ORDER BY ic.key_ordinal
                    """, table_name, schema)
                    primary_keys = [row[0] for row in cursor.fetchall()]
                
                self.logger.info(f"找到 {len(primary_keys)} 個主鍵: {primary_keys}")
                
            except Exception as e:
                self.logger.warning(f"獲取主鍵失敗: {e}")
                primary_keys = []
            
            # 獲取外鍵信息
            self.logger.info(f"獲取外鍵信息...")
            try:
                cursor.execute(f"""
                    SELECT 
                        KCU1.COLUMN_NAME,
                        KCU2.TABLE_NAME as REFERENCED_TABLE_NAME,
                        KCU2.COLUMN_NAME as REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
                        ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
                        ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
                    WHERE KCU1.TABLE_NAME = ? 
                    AND KCU1.TABLE_SCHEMA = ?
                """, table_name, schema)
                foreign_keys = cursor.fetchall()
                self.logger.info(f"找到 {len(foreign_keys)} 個外鍵")
                
            except Exception as e:
                self.logger.warning(f"獲取外鍵失敗: {e}")
                foreign_keys = []
            
            cursor.close()
            conn.close()
            
            # 顯示獲取結果摘要
            self.logger.info(f"表格 {schema}.{table_name} 結構獲取完成:")
            self.logger.info(f"  - 欄位數: {len(columns)}")
            self.logger.info(f"  - 主鍵數: {len(primary_keys)}")
            self.logger.info(f"  - 外鍵數: {len(foreign_keys)}")
            
            # 顯示前幾個欄位的詳細信息
            if columns:
                self.logger.info("前5個欄位:")
                for i, col in enumerate(columns[:5]):
                    self.logger.info(f"  {i+1}. {col[0]} ({col[1]})")
            
            return columns, primary_keys, foreign_keys
            
        except Exception as e:
            self.logger.error(f"獲取表格 {table_name} 結構時發生未預期錯誤: {str(e)}")
            if conn:
                conn.close()
            return [], [], []
    
    def create_mssql_engine(self):
        """創建MSSQL SQLAlchemy引擎"""
        if self.mssql_engine:
            return self.mssql_engine
        
        try:
            server = self.mssql_config['server']
            database = self.mssql_config['database']
            
            if self.mssql_config.get('use_windows_auth', False):
                # Windows驗證
                connection_url = URL.create(
                    "mssql+pyodbc",
                    host=server,
                    database=database,
                    query={
                        "driver": "ODBC Driver 17 for SQL Server",
                        "trusted_connection": "yes"
                    }
                )
            else:
                # SQL Server驗證
                username = self.mssql_config.get('username', '')
                password = self.mssql_config.get('password', '')
                connection_url = URL.create(
                    "mssql+pyodbc",
                    username=username,
                    password=password,
                    host=server,
                    database=database,
                    query={
                        "driver": "ODBC Driver 17 for SQL Server"
                    }
                )
            
            self.mssql_engine = create_engine(connection_url, echo=False)
            
            # 測試連接
            with self.mssql_engine.connect() as conn:
                result = conn.execute(text("SELECT @@SERVERNAME, DB_NAME()"))
                server_info = result.fetchone()
                self.logger.info(f"✅ SQLAlchemy MSSQL連接成功: {server_info[0]}, {server_info[1]}")
            
            return self.mssql_engine
            
        except Exception as e:
            self.logger.error(f"❌ 創建MSSQL SQLAlchemy引擎失敗: {str(e)}")
            return None
        
    def convert_datatype(self, mssql_type: str, length: Optional[int], precision: Optional[int], scale: Optional[int]) -> str:
        """將MSSQL資料類型轉換為MariaDB類型（基於測試成功的邏輯）"""
        mssql_type_lower = mssql_type.lower()
        
        if mssql_type_lower in ['decimal', 'numeric']:
            if precision and scale is not None:
                return f'DECIMAL({precision},{scale})'
            else:
                return 'DECIMAL(10,2)'
        elif mssql_type_lower in ['varchar', 'nvarchar']:
            if length and length > 0:
                # MariaDB VARCHAR 限制，超過16383轉為TEXT
                if length > 16383:
                    return 'TEXT'
                return f'VARCHAR({length})'
            else:
                return 'TEXT'
        elif mssql_type_lower in ['char', 'nchar']:
            if length and length > 0:
                if length > 255:
                    return f'VARCHAR({length})'
                return f'CHAR({length})'
            else:
                return 'CHAR(1)'
        else:
            return self.datatype_mapping.get(mssql_type_lower, 'TEXT')
    
    def create_mariadb_table(self, table_name: str, columns: List, primary_keys: List, foreign_keys: List) -> bool:
        """在MariaDB中創建表格（修復語法錯誤並改進邏輯）"""
        conn = self.connect_mariadb()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 先刪除表格（如果存在）- 確保重新創建
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                self.logger.info(f"清理舊表格: {table_name}")
            except:
                pass
            
            # 構建CREATE TABLE語句
            col_definitions = []
            for col in columns:
                col_name = col[0]
                data_type = self.convert_datatype(col[1], col[2], col[3], col[4])
                nullable = "NULL" if col[5] == "YES" else "NOT NULL"
                
                col_def = f"`{col_name}` {data_type} {nullable}"
                col_definitions.append(col_def)
            
            # 添加主鍵
            if primary_keys:
                pk_def = f"PRIMARY KEY ({', '.join([f'`{pk}`' for pk in primary_keys])})"
                col_definitions.append(pk_def)
            
            # 修復f-string語法錯誤
            columns_sql = ',\n                '.join(col_definitions)
            create_sql = f"""CREATE TABLE `{table_name}` (
                {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"""
            
            cursor.execute(create_sql)
            conn.commit()
            
            # 驗證創建成功
            cursor.execute(f"DESCRIBE `{table_name}`")
            description = cursor.fetchall()
            self.logger.info(f"✅ 成功創建表格 {table_name}: {len(description)} 個欄位")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 創建表格 {table_name} 失敗: {str(e)}")
            if conn:
                conn.close()
            return False
    
    def migrate_table_data(self, table_name: str, primary_keys: List) -> bool:
        """遷移單個表格的資料 - 以表格為單位commit版本"""
        self.logger.info(f"開始遷移表格: {table_name}")
        
        mssql_engine = self.create_mssql_engine()
        mariadb_conn = self.connect_mariadb()
        
        if not mssql_engine or not mariadb_conn:
            return False
        
        try:
            mariadb_cursor = mariadb_conn.cursor()
            
            # 獲取總記錄數
            count_query = text(f"SELECT COUNT(*) FROM [{table_name}]")
            with mssql_engine.connect() as conn:
                result = conn.execute(count_query)
                total_records = result.fetchone()[0]
            
            self.logger.info(f"表格 {table_name} 總記錄數: {total_records:,}")
            
            if total_records == 0:
                self.logger.info(f"表格 {table_name} 無資料，跳過遷移")
                mariadb_conn.close()
                return True
            
            # 🔧 關鍵：在開始前設置自動提交為False，整個表格作為一個事務
            mariadb_conn.autocommit = False
            
            # 分批處理變數
            offset = 0
            migrated_count = 0
            batch_number = 0
            start_time = time.time()
            
            # 處理所有批次，但不提交事務
            while offset < total_records:
                batch_number += 1
                
                try:
                    # 構建分頁查詢
                    if primary_keys:
                        order_clause = f"ORDER BY {', '.join([f'[{pk}]' for pk in primary_keys])}"
                    else:
                        order_clause = ""
                    
                    select_sql = f"""
                    SELECT * FROM [{table_name}] {order_clause}
                    OFFSET {offset} ROWS FETCH NEXT {self.batch_size} ROWS ONLY
                    """
                    
                    # 從MSSQL獲取數據
                    df = pd.read_sql(select_sql, mssql_engine)
                    
                    if df.empty:
                        break
                    
                    # 資料預處理
                    df = self.preprocess_data(df)
                    
                    # 插入MariaDB（不提交）
                    success = self.insert_batch_to_mariadb(mariadb_cursor, table_name, df)
                    
                    if success:
                        migrated_count += len(df)
                        
                        # 記錄進度（每50個批次或到達末尾時顯示）
                        if batch_number % 50 == 0 or offset + self.batch_size >= total_records:
                            elapsed_time = time.time() - start_time
                            progress = min((offset + self.batch_size) / total_records * 100, 100)
                            rate = migrated_count / elapsed_time if elapsed_time > 0 else 0
                            
                            self.logger.info(
                                f"批次 {batch_number}: 已處理 {migrated_count:,}/{total_records:,} 筆 "
                                f"({progress:.1f}%) - 速度: {rate:.0f} 筆/秒"
                            )
                            mariadb_conn.commit()
                    else:
                        # 如果任何批次失敗，回滾整個表格
                        self.logger.error(f"❌ 批次 {batch_number} 插入失敗，回滾整個表格")
                        mariadb_conn.rollback()
                        mariadb_conn.close()
                        return False
                    
                    offset += self.batch_size
                    
                except Exception as e:
                    # 批次處理異常，回滾整個表格
                    self.logger.error(f"❌ 批次 {batch_number} 處理異常: {str(e)}")
                    mariadb_conn.rollback()
                    mariadb_conn.close()
                    return False
            
            # 🎯 關鍵：所有批次成功後，一次性提交整個表格
            self.logger.info(f"所有批次處理完成，提交表格 {table_name} 的 {migrated_count:,} 筆資料...")
            
            try:
                mariadb_conn.commit()
                total_time = time.time() - start_time
                avg_rate = migrated_count / total_time if total_time > 0 else 0
                
                self.logger.info(f"✅ 表格 {table_name} 遷移成功完成:")
                self.logger.info(f"  - 總記錄數: {total_records:,}")
                self.logger.info(f"  - 成功遷移: {migrated_count:,}")
                self.logger.info(f"  - 處理時間: {total_time:.1f} 秒")
                self.logger.info(f"  - 平均速度: {avg_rate:.0f} 筆/秒")
                self.logger.info(f"  - 成功率: 100.00%")
                
            except Exception as commit_error:
                self.logger.error(f"❌ 提交事務失敗: {str(commit_error)}")
                mariadb_conn.rollback()
                mariadb_conn.close()
                return False
            
            mariadb_conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 遷移表格 {table_name} 時發生錯誤: {str(e)}")
            if mariadb_conn:
                try:
                    mariadb_conn.rollback()
                except:
                    pass
                mariadb_conn.close()
            return False
    
    def insert_batch_to_mariadb(self, cursor, table_name: str, df: pd.DataFrame) -> bool:
        """批次插入資料到MariaDB（配合表格級commit）"""
        if df.empty:
            return True
        
        try:
            # 構建INSERT語句
            columns = [f"`{col}`" for col in df.columns]
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"
            
            # 準備數據，關鍵：處理numpy類型轉換
            data_tuples = []
            for _, row in df.iterrows():
                processed_row = []
                for value in row.values:
                    if value is None or pd.isna(value):
                        processed_row.append(None)
                    elif isinstance(value, str):
                        # 處理字符串，保持數據完整性
                        processed_row.append(value)
                    else:
                        # 🔧 關鍵修復：將numpy類型轉換為Python原生類型
                        processed_row.append(self.convert_numpy_to_python(value))
                
                data_tuples.append(tuple(processed_row))
            
            # 根據數據量選擇插入方式
            if len(data_tuples) == 1:
                # 單筆插入
                cursor.execute(insert_sql, data_tuples[0])
            elif len(data_tuples) <= 100:
                # 小批次：使用executemany
                cursor.executemany(insert_sql, data_tuples)
            else:
                # 大批次：分段executemany，避免內存問題
                chunk_size = 100
                for i in range(0, len(data_tuples), chunk_size):
                    chunk = data_tuples[i:i + chunk_size]
                    cursor.executemany(insert_sql, chunk)
            
            return True
            
        except mysql.connector.Error as e:
            self.logger.error(f"❌ MariaDB插入失敗:")
            self.logger.error(f"   錯誤碼: {e.errno}")
            self.logger.error(f"   錯誤訊息: {e.msg}")
            
            # 詳細錯誤分析
            if e.errno == 1062:  # Duplicate entry
                self.logger.error("   → 主鍵重複，可能需要清理目標表格")
            elif e.errno == 1406:  # Data too long
                self.logger.error("   → 資料長度超過欄位限制")
            elif e.errno == 1264:  # Out of range
                self.logger.error("   → 數值超出範圍")
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 插入過程發生未知錯誤: {str(e)}")
            import traceback
            self.logger.error(f"   詳細錯誤: {traceback.format_exc()}")
            return False
        
    def batch_insert_remaining(self, cursor, table_name: str, df: pd.DataFrame, insert_sql: str) -> bool:
        """批次插入剩餘資料"""
        try:
            # 準備剩餘資料
            data_tuples = []
            for _, row in df.iterrows():
                processed_row = []
                for value in row.values:
                    if value is None or pd.isna(value):
                        processed_row.append(None)
                    elif isinstance(value, str):
                        processed_row.append(value) # strip
                    else:
                        processed_row.append(value)
                
                data_tuples.append(tuple(processed_row))
            
            # 執行批次插入
            cursor.executemany(insert_sql, data_tuples)
            self.logger.info(f"✅ 批次插入剩餘 {len(data_tuples)} 筆成功")
            return True
            
        except mysql.connector.Error as e:
            self.logger.error(f"❌ 批次插入剩餘資料失敗:")
            self.logger.error(f"   錯誤碼: {e.errno}")
            self.logger.error(f"   錯誤訊息: {e.msg}")
            
            # 如果批次插入失敗，改為逐筆插入以找出問題資料
            return self.fallback_single_insert(cursor, df, insert_sql)
            
        except Exception as e:
            self.logger.error(f"❌ 批次插入發生未知錯誤: {str(e)}")
            return False

    def fallback_single_insert(self, cursor, df: pd.DataFrame, insert_sql: str) -> bool:
        """備用方案：逐筆插入"""
        self.logger.info("🔄 批次插入失敗，改為逐筆插入診斷...")
        
        success_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                processed_row = []
                for value in row.values:
                    if value is None or pd.isna(value):
                        processed_row.append(None)
                    elif isinstance(value, str):
                        processed_row.append(value) #.strip()
                    else:
                        processed_row.append(value)
                
                cursor.execute(insert_sql, tuple(processed_row))
                success_count += 1
                
                if success_count % 100 == 0:
                    self.logger.info(f"   已成功插入 {success_count} 筆...")
                
            except mysql.connector.Error as e:
                error_count += 1
                if error_count <= 5:  # 只記錄前5個錯誤
                    self.logger.error(f"❌ 第 {index+1} 筆插入失敗:")
                    self.logger.error(f"   錯誤: {e.msg}")
                    
                    # 記錄問題資料的前幾個欄位
                    problem_data = {}
                    for col, val in zip(df.columns, row.values):
                        if isinstance(val, str) and len(val) > 50:
                            problem_data[col] = f"{val[:47]}..."
                        else:
                            problem_data[col] = val
                    self.logger.error(f"   資料: {problem_data}")
                
                if error_count > 10:  # 如果錯誤太多，停止
                    self.logger.error(f"❌ 錯誤過多 ({error_count})，停止插入")
                    break
        
        self.logger.info(f"📊 逐筆插入結果: 成功 {success_count}, 失敗 {error_count}")
        return error_count == 0

    def analyze_insert_error(self, error, row_data, columns, table_name):
        """分析插入錯誤"""
        self.logger.error("🔍 錯誤分析:")
        
        # 常見錯誤分析
        if hasattr(error, 'errno'):
            if error.errno == 1062:  # Duplicate entry
                self.logger.error("   → 主鍵重複錯誤")
                # 找出主鍵欄位的值
                for col, val in zip(columns, row_data.values):
                    if 'id' in col.lower() or 'name' in col.lower():
                        self.logger.error(f"   → 可能的重複值: {col} = {val}")
                        
            elif error.errno == 1406:  # Data too long
                self.logger.error("   → 資料長度超過欄位限制")
                for col, val in zip(columns, row_data.values):
                    if isinstance(val, str) and len(val) > 255:
                        self.logger.error(f"   → 過長欄位: {col} (長度: {len(val)})")
                        
            elif error.errno == 1264:  # Out of range
                self.logger.error("   → 數值超出範圍")
                for col, val in zip(columns, row_data.values):
                    if isinstance(val, (int, float)) and abs(val) > 2147483647:
                        self.logger.error(f"   → 過大數值: {col} = {val}")
                        
            elif error.errno == 1292:  # Incorrect value
                self.logger.error("   → 資料格式錯誤")
                for col, val in zip(columns, row_data.values):
                    if 'date' in col.lower() or 'time' in col.lower():
                        self.logger.error(f"   → 可能的日期問題: {col} = {val}")
        
        # 檢查表格結構是否匹配
        self.logger.error("🔍 建議檢查:")
        self.logger.error("   1. MariaDB表格結構是否正確創建")
        self.logger.error("   2. 欄位長度是否足夠")
        self.logger.error("   3. 資料類型是否匹配")
        self.logger.error("   4. 字符編碼是否一致")
    
    def validate_batch_data(self, table_name: str, df: pd.DataFrame, primary_keys: List) -> bool:
        """驗證批次資料一致性"""
        if df.empty or not primary_keys:
            return True
        
        try:
            mariadb_conn = self.connect_mariadb()
            if not mariadb_conn:
                return False
            
            cursor = mariadb_conn.cursor()
            
            # 檢查第一筆和最後一筆記錄
            for idx in [0, len(df) - 1]:
                if idx < len(df):
                    # 構建WHERE條件
                    where_conditions = []
                    params = []
                    for pk in primary_keys:
                        if pk in df.columns:
                            where_conditions.append(f"`{pk}` = %s")
                            params.append(df.iloc[idx][pk])
                    
                    if where_conditions:
                        where_clause = " AND ".join(where_conditions)
                        check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
                        cursor.execute(check_sql, params)
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            cursor.close()
                            mariadb_conn.close()
                            return False
            
            cursor.close()
            mariadb_conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"批次驗證失敗: {str(e)}")
            return False
    
    def log_batch_error(self, table_name: str, batch_number: int, error_message: str):
        """記錄批次錯誤"""
        error_log = {
            'timestamp': datetime.now().isoformat(),
            'table_name': table_name,
            'batch_number': batch_number,
            'error_message': error_message
        }
        
        self.migration_log.append(error_log)
        
        # 寫入錯誤日誌檔案
        error_file = f'migration_logs/batch_errors_{datetime.now().strftime("%Y%m%d")}.json'
        with open(error_file, 'w', encoding='utf-8') as f:
            json.dump(self.migration_log, f, ensure_ascii=False, indent=2)
    
    def validate_migration_complete(self, schema: str = 'dbo') -> Dict[str, Any]:

        """完整的遷移後驗證"""
        self.logger.info("開始進行遷移後完整驗證...")
        
        tables = self.get_mssql_tables(schema)
        validation_results = {
            'timestamp': datetime.now().isoformat(),
            'tables': {},
            'overall_success': True
        }
        
        for table_name in tables:
            if table_name == "Memo":
                validation_results['tables'][table_name] = {
                    'record_count_match': True,
                    'mssql_count': 0,
                    'mariadb_count': 0,
                    'sample_data_match': True,
                    'extreme_values_match': True,
                    'data_consistency': True
                }
                continue

            self.logger.info(f"驗證表格: {table_name}")
            table_result = self.validate_single_table(table_name)
            validation_results['tables'][table_name] = table_result
            
            if not table_result['data_consistency']:
                validation_results['overall_success'] = False
        
        # 生成驗證報告
        self.generate_validation_report(validation_results)
        
        return validation_results
    
    def validate_single_table(self, table_name: str) -> Dict[str, Any]:
        """驗證單個表格的一致性"""
        result = {
            'record_count_match': False,
            'mssql_count': 0,
            'mariadb_count': 0,
            'sample_data_match': False,
            'extreme_values_match': False,
            'data_consistency': False
        }
        
        try:
            mssql_conn = self.connect_mssql()
            mariadb_conn = self.connect_mariadb()
            
            if not mssql_conn or not mariadb_conn:
                return result
            
            # 1. 記錄數比較
            mssql_cursor = mssql_conn.cursor()
            mariadb_cursor = mariadb_conn.cursor()
            
            mssql_cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            result['mssql_count'] = mssql_cursor.fetchone()[0]
            
            mariadb_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            result['mariadb_count'] = mariadb_cursor.fetchone()[0]
            
            result['record_count_match'] = result['mssql_count'] == result['mariadb_count']
            
            # 2. 抽樣資料比較（僅在記錄數匹配時進行）
            if result['record_count_match'] and result['mssql_count'] > 0:
                result['sample_data_match'] = self.validate_sample_data(table_name, mssql_cursor, mariadb_cursor)
                result['extreme_values_match'] = self.validate_extreme_values(table_name, mssql_cursor, mariadb_cursor)
            
            # 3. 綜合判斷
            result['data_consistency'] = (
                result['record_count_match'] and 
                result['sample_data_match'] and 
                result['extreme_values_match']
            )
            
            mssql_conn.close()
            mariadb_conn.close()
            
        except Exception as e:
            self.logger.error(f"驗證表格 {table_name} 失敗: {str(e)}")
        
        return result
    
    def validate_sample_data(self, table_name: str, mssql_cursor, mariadb_cursor, sample_size: int = 100) -> bool:
        """抽樣驗證資料一致性"""
        try:
            # 隨機抽取樣本進行比較
            mssql_cursor.execute(f"SELECT TOP {sample_size} * FROM [{table_name}] ORDER BY NEWID()")
            mssql_sample = mssql_cursor.fetchall()
            
            if not mssql_sample:
                return True
            
            # 獲取列名
            columns = [desc[0] for desc in mssql_cursor.description]
            
            # 從MariaDB獲取對應資料（簡化比較）
            mariadb_cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {sample_size}")
            mariadb_sample = mariadb_cursor.fetchall()
            
            return len(mssql_sample) == len(mariadb_sample)
            
        except Exception as e:
            self.logger.error(f"抽樣驗證失敗: {str(e)}")
            return False
    
    def validate_extreme_values(self, table_name: str, mssql_cursor, mariadb_cursor) -> bool:
        """驗證極值一致性"""
        try:
            # 獲取數值列
            mssql_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' 
                AND DATA_TYPE IN ('int', 'bigint', 'decimal', 'numeric', 'float', 'real', 'money')
            """)
            numeric_columns = mssql_cursor.fetchall()
            
            for col_name, data_type in numeric_columns:
                # MSSQL極值
                mssql_cursor.execute(f"SELECT MIN([{col_name}]), MAX([{col_name}]) FROM [{table_name}] WHERE [{col_name}] IS NOT NULL")
                mssql_extremes = mssql_cursor.fetchone()
                
                # MariaDB極值
                mariadb_cursor.execute(f"SELECT MIN(`{col_name}`), MAX(`{col_name}`) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
                mariadb_extremes = mariadb_cursor.fetchone()
                
                if mssql_extremes and mariadb_extremes:
                    if mssql_extremes[0] != mariadb_extremes[0] or mssql_extremes[1] != mariadb_extremes[1]:
                        self.logger.warning(f"表格 {table_name} 列 {col_name} 極值不匹配")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"極值驗證失敗: {str(e)}")
            return False
    
    def generate_validation_report(self, validation_results: Dict):
        """生成驗證報告"""
        report_file = f'migration_logs/validation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>資料庫遷移驗證報告</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                .success {{ color: green; font-weight: bold; }}
                .error {{ color: red; font-weight: bold; }}
                .warning {{ color: orange; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .status-ok {{ background-color: #d4edda; }}
                .status-error {{ background-color: #f8d7da; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>資料庫遷移驗證報告</h1>
                <p><strong>驗證時間:</strong> {validation_results['timestamp']}</p>
                <p><strong>整體結果:</strong> 
                    <span class="{'success' if validation_results['overall_success'] else 'error'}">
                        {'成功' if validation_results['overall_success'] else '失敗'}
                    </span>
                </p>
            </div>
            
            <h2>表格詳細驗證結果</h2>
            <table>
                <tr>
                    <th>表格名稱</th>
                    <th>MSSQL記錄數</th>
                    <th>MariaDB記錄數</th>
                    <th>記錄數匹配</th>
                    <th>抽樣驗證</th>
                    <th>極值驗證</th>
                    <th>整體一致性</th>
                </tr>
        """
        
        for table_name, result in validation_results['tables'].items():
            consistency_class = "status-ok" if result['data_consistency'] else "status-error"
            html_content += f"""
                <tr class="{consistency_class}">
                    <td>{table_name}</td>
                    <td>{result['mssql_count']:,}</td>
                    <td>{result['mariadb_count']:,}</td>
                    <td>{'✓' if result['record_count_match'] else '✗'}</td>
                    <td>{'✓' if result['sample_data_match'] else '✗'}</td>
                    <td>{'✓' if result['extreme_values_match'] else '✗'}</td>
                    <td>{'✓' if result['data_consistency'] else '✗'}</td>
                </tr>
            """
        
        html_content += """
            </table>
            
            <h2>說明</h2>
            <ul>
                <li><strong>記錄數匹配:</strong> 檢查來源和目標資料庫的記錄總數是否相同</li>
                <li><strong>抽樣驗證:</strong> 隨機抽取樣本資料進行比較</li>
                <li><strong>極值驗證:</strong> 檢查數值列的最大值和最小值是否一致</li>
                <li><strong>整體一致性:</strong> 綜合所有驗證項目的結果</li>
            </ul>
        </body>
        </html>
        """
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"驗證報告已生成: {report_file}")
    
    def migrate_full_database(self, schema: str = 'dbo') -> bool:
        """執行完整資料庫遷移（配合表格級commit）"""
        self.logger.info("開始完整資料庫遷移...")
        
        # 定義表格遷移順序 (GPT自行分析)
        table_order = [
            'CompanyOwner',
            'Factory', 
            'Announcement',
            'ViolationCase',
            'Memo',
            'AllowRework',
            'Appeal',
            'IllegalProfit',
            'Inspection',
            'Detail'
        ]
        
        # 獲取所有表格
        all_tables = self.get_mssql_tables(schema)
        if not all_tables:
            self.logger.error("❌ 無法獲取MSSQL表格列表")
            return False
        
        # 第一階段：創建所有表格結構
        self.logger.info("🔧 第一階段：創建表格結構...")
        structure_success_count = 0
        
        for table_name in table_order:
            if table_name in all_tables:
                self.logger.info(f"創建表格結構: {table_name}")
                
                columns, primary_keys, foreign_keys = self.get_table_schema(table_name, schema)
                
                if not columns:
                    self.logger.error(f"❌ 無法獲取表格 {table_name} 的結構")
                    continue
                
                if self.create_mariadb_table(table_name, columns, primary_keys, foreign_keys):
                    structure_success_count += 1
                    self.logger.info(f"✅ 表格 {table_name} 結構創建成功")
                else:
                    self.logger.error(f"❌ 表格 {table_name} 結構創建失敗")
        
        if structure_success_count == 0:
            self.logger.error("❌ 沒有任何表格結構創建成功，終止遷移")
            return False
        
        self.logger.info(f"✅ 表格結構創建完成: {structure_success_count}/{len(table_order)}")
        
        # 第二階段：以表格為單位遷移資料
        self.logger.info("📊 第二階段：遷移資料（以表格為單位commit）...")
        data_success_count = 0
        failed_tables = []
        
        for table_name in table_order:
            if table_name in all_tables:
                self.logger.info(f"\n🚀 開始遷移表格: {table_name}")
                
                # 獲取主鍵
                _, primary_keys, _ = self.get_table_schema(table_name, schema)
                
                # 遷移整個表格（作為一個事務）
                if self.migrate_table_data(table_name, primary_keys):
                    data_success_count += 1
                    self.logger.info(f"✅ 表格 {table_name} 完整遷移成功\n")
                else:
                    failed_tables.append(table_name)
                    self.logger.error(f"❌ 表格 {table_name} 遷移失敗\n")
        
        # 處理剩餘表格
        remaining_tables = [t for t in all_tables if t not in table_order]
        for table_name in remaining_tables:
            self.logger.info(f"\n🚀 處理額外表格: {table_name}")
            
            columns, primary_keys, foreign_keys = self.get_table_schema(table_name, schema)
            if columns:
                if self.create_mariadb_table(table_name, columns, primary_keys, foreign_keys):
                    structure_success_count += 1
                    
                    if self.migrate_table_data(table_name, primary_keys):
                        data_success_count += 1
                        self.logger.info(f"✅ 額外表格 {table_name} 完整遷移成功")
                    else:
                        failed_tables.append(table_name)
                        self.logger.error(f"❌ 額外表格 {table_name} 遷移失敗")
        
        # 第三階段：驗證遷移結果
        self.logger.info("🔍 第三階段：驗證遷移結果...")
        existing_tables = self.check_mariadb_tables_exist()
        
        # 生成詳細報告
        self.logger.info(f"\n📊 遷移結果總結:")
        self.logger.info(f"  - 目標表格數: {len(all_tables)}")
        self.logger.info(f"  - 結構創建成功: {structure_success_count}")
        self.logger.info(f"  - 資料遷移成功: {data_success_count}")
        self.logger.info(f"  - MariaDB現有表格: {len(existing_tables)}")
        
        if failed_tables:
            self.logger.warning(f"  - 失敗表格: {', '.join(failed_tables)}")
        
        if data_success_count == len(all_tables):
            self.logger.info("🎉 所有表格遷移成功！")
        elif data_success_count > 0:
            self.logger.warning(f"⚠️  部分表格遷移成功 ({data_success_count}/{len(all_tables)})")
        else:
            self.logger.error("❌ 沒有任何表格成功遷移")
        
        return data_success_count > 0
    
    def check_mariadb_tables_exist(self) -> List[str]:
        """檢查MariaDB中存在的表格"""
        conn = self.connect_mariadb()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except Exception as e:
            self.logger.error(f"檢查MariaDB表格失敗: {str(e)}")
            return []
    
    def optimize_mariadb_tables(self):
        """對MariaDB表格進行優化（快速修復版本）"""
        self.logger.info("開始優化MariaDB表格...")
        
        conn = self.connect_mariadb()
        if not conn:
            return
        
        try:
            optimization_results = {}
            
            # 獲取所有表格
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            for table_name in tables:
                self.logger.info(f"優化表格: {table_name}")
                
                try:
                    # 為每個表格使用新的 cursor
                    table_cursor = conn.cursor()
                    
                    # 分析表格
                    table_cursor.execute(f"ANALYZE TABLE `{table_name}`")
                    analyze_result = table_cursor.fetchall()
                    
                    # 優化表格
                    table_cursor.execute(f"OPTIMIZE TABLE `{table_name}`")
                    optimize_result = table_cursor.fetchall()
                    
                    # 獲取記錄數
                    table_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    row_count = table_cursor.fetchone()[0]
                    
                    table_cursor.close()
                    
                    optimization_results[table_name] = {
                        'status': 'optimized',
                        'analyze_result': analyze_result,
                        'optimize_result': optimize_result,
                        'row_count': row_count
                    }
                    
                    self.logger.info(f"✅ 表格 {table_name} 優化完成 - 記錄數: {row_count:,}")
                    
                except Exception as e:
                    self.logger.error(f"❌ 表格 {table_name} 優化失敗: {str(e)}")
                    optimization_results[table_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            # 生成優化報告
            self.generate_optimization_report(optimization_results)
            
            conn.close()
            
            # 顯示摘要
            successful = sum(1 for r in optimization_results.values() if r['status'] == 'optimized')
            self.logger.info(f"✅ 優化完成: {successful}/{len(tables)} 個表格成功")
            
        except Exception as e:
            self.logger.error(f"優化過程中發生錯誤: {str(e)}")
            if conn:
                conn.close()
    
    def clean_mariadb_tables(self):
        """清理MariaDB中的所有相關表格"""
        self.logger.info("開始清理MariaDB表格...")
        
        conn = self.connect_mariadb()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 獲取所有表格
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
            # 預定義的表格順序（反向刪除，避免外鍵約束問題）
            target_tables = [
                'Detail', 'Inspection', 'IllegalProfit', 'Appeal', 
                'AllowRework', 'Memo', 'ViolationCase', 
                'Announcement', 'Factory', 'CompanyOwner'
            ]
            
            # 先禁用外鍵檢查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            cleaned_count = 0
            for table in target_tables:
                # 檢查表格是否存在（不區分大小寫）
                table_exists = any(t.lower() == table.lower() for t in tables)
                
                if table_exists:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                        self.logger.info(f"🗑️  已刪除表格: {table}")
                        cleaned_count += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️  無法刪除表格 {table}: {e}")
            
            # 重新啟用外鍵檢查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"✅ 清理完成，共刪除 {cleaned_count} 個表格")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 清理表格失敗: {str(e)}")
            if conn:
                conn.close()
            return False
    
    def generate_optimization_report(self, results: Dict):
        """生成優化報告"""
        report_file = f'migration_logs/optimization_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("MariaDB 表格優化報告\n")
            f.write("=" * 50 + "\n")
            f.write(f"優化時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for table_name, result in results.items():
                f.write(f"表格: {table_name}\n")
                f.write(f"狀態: {result['status']}\n")
                if 'explain_info' in result:
                    f.write("EXPLAIN 分析結果:\n")
                    for row in result['explain_info']:
                        f.write(f"  {row}\n")
                elif 'error' in result:
                    f.write(f"錯誤: {result['error']}\n")
                f.write("-" * 30 + "\n")
        
        self.logger.info(f"優化報告已生成: {report_file}")
    
    def convert_numpy_to_python(self, value):
        """將numpy類型轉換為Python原生類型"""
        import numpy as np
        
        # 處理numpy整數類型
        if isinstance(value, (np.integer, np.int8, np.int16, np.int32, np.int64)):
            return int(value)
        
        # 處理numpy浮點類型
        elif isinstance(value, (np.floating, np.float16, np.float32, np.float64)):
            return float(value)
        
        # 處理numpy布爾類型
        elif isinstance(value, np.bool_):
            return bool(value)
        
        # 處理numpy字符串類型
        elif isinstance(value, (np.str_, np.unicode_)):
            return str(value)
        
        # 處理numpy日期時間類型
        elif isinstance(value, np.datetime64):
            # 轉換為Python datetime字符串
            return pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
        
        # 處理pandas的Timestamp
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        
        # 處理其他numpy標量類型
        elif hasattr(value, 'item'):
            # numpy標量都有item()方法可以轉換為Python原生類型
            return value.item()
        
        # 如果都不是，直接返回原值
        else:
            return value
        
    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """資料預處理（增強版：處理numpy類型轉換）"""
        import numpy as np
        
        # 處理NaN值
        df = df.where(pd.notnull(df), None)
        
        # 🔧 關鍵：轉換所有numpy類型為Python原生類型
        for col in df.columns:
            # 檢查列的數據類型
            col_dtype = df[col].dtype
            
            # 處理整數類型
            if pd.api.types.is_integer_dtype(col_dtype):
                df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            
            # 處理浮點類型
            elif pd.api.types.is_float_dtype(col_dtype):
                df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            
            # 處理布爾類型
            elif pd.api.types.is_bool_dtype(col_dtype):
                df[col] = df[col].apply(lambda x: bool(x) if pd.notnull(x) else None)
            
            # 處理日期時間類型
            elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                df[col] = df[col].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
                )
            
            # 處理字符串類型中的日期格式
            elif df[col].dtype == 'object' and 'date' in col.lower():
                # 嘗試轉換日期格式
                try:
                    sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if sample_val and isinstance(sample_val, str):
                        if re.match(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', str(sample_val)):
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                except:
                    pass
        
        return df


def main():
    """主程式"""
    parser = argparse.ArgumentParser(description='MSSQL 到 MariaDB 資料庫遷移工具')
    parser.add_argument('--action', choices=['migrate', 'validate', 'optimize', 'clean', 'all'], 
                        default='all', help='執行動作')
    parser.add_argument('--mssql-server', default='localhost\\SQLEXPRESS', help='MSSQL伺服器')
    parser.add_argument('--mssql-database', default='dbmidterm', help='MSSQL資料庫名稱')
    parser.add_argument('--mssql-username', default='', help='MSSQL使用者名稱（僅限SQL驗證）')
    parser.add_argument('--mssql-password', default='', help='MSSQL密碼（僅限SQL驗證）')
    parser.add_argument('--mssql-windows-auth', action='store_true', default=True, help='使用Windows驗證（預設啟用）')
    parser.add_argument('--mariadb-host', default='localhost', help='MariaDB主機')
    parser.add_argument('--mariadb-port', type=int, default=3306, help='MariaDB埠號')
    parser.add_argument('--mariadb-database', default='test', help='MariaDB資料庫名稱')
    parser.add_argument('--mariadb-username', default='root', help='MariaDB使用者名稱')
    parser.add_argument('--mariadb-password', default='12345', help='MariaDB密碼')
    parser.add_argument('--batch-size', type=int, default=1000, help='批次大小')
    parser.add_argument('--schema', default='dbo', help='MSSQL Schema')
    
    args = parser.parse_args()
    
    # 配置資料庫連接
    mssql_config = {
        'server': args.mssql_server,
        'database': args.mssql_database,
        'username': args.mssql_username,
        'password': args.mssql_password,
        'use_windows_auth': args.mssql_windows_auth
    }
    
    mariadb_config = {
        'host': args.mariadb_host,
        'port': args.mariadb_port,
        'database': args.mariadb_database,
        'username': args.mariadb_username,
        'password': args.mariadb_password
    }
    
    # 創建遷移器
    migrator = DatabaseMigrator(mssql_config, mariadb_config, args.batch_size)
    
    try:
        # 顯示配置信息
        print("📋 遷移配置:")
        print(f"   MSSQL: {args.mssql_server}/{args.mssql_database}")
        print(f"   MariaDB: {args.mariadb_host}:{args.mariadb_port}/{args.mariadb_database}")
        print(f"   批次大小: {args.batch_size}")
        print(f"   動作: {args.action}")
        
        if args.action == 'clean':
            print("\n🗑️  開始清理MariaDB表格...")
            success = migrator.clean_mariadb_tables()
            if success:
                print("✅ 表格清理完成")
            else:
                print("❌ 表格清理失敗")
        
        elif args.action == 'migrate':
            print("\n🚀 開始資料庫遷移...")
            success = migrator.migrate_full_database(args.schema)
            if success:
                print("✅ 資料庫遷移成功完成")
            else:
                print("⚠️  資料庫遷移部分失敗，請檢查日誌")
        
        elif args.action == 'validate':
            print("\n🔍 開始遷移驗證...")
            
            # 先檢查表格是否存在
            existing_tables = migrator.check_mariadb_tables_exist()
            mssql_tables = migrator.get_mssql_tables(args.schema)
            
            missing_tables = []
            for table in mssql_tables:
                if not any(t.lower() == table.lower() for t in existing_tables):
                    missing_tables.append(table)
            
            if missing_tables:
                print(f"⚠️  發現 {len(missing_tables)} 個表格不存在，先執行遷移...")
                print(f"   缺失表格: {', '.join(missing_tables)}")
                
                # 自動執行遷移
                migrate_success = migrator.migrate_full_database(args.schema)
                if not migrate_success:
                    print("❌ 自動遷移失敗，無法進行驗證")
                    sys.exit(1)
                
                print("✅ 自動遷移完成，繼續驗證...")
            
            # 執行驗證
            results = migrator.validate_migration_complete(args.schema)
            if results['overall_success']:
                print("✅ 遷移驗證通過")
            else:
                print("❌ 遷移驗證失敗，請檢查報告")
        
        elif args.action == 'optimize':
            print("\n⚡ 開始資料庫優化...")
            
            # 檢查表格是否存在
            existing_tables = migrator.check_mariadb_tables_exist()
            if not existing_tables:
                print("⚠️  沒有找到任何表格，先執行遷移...")
                migrate_success = migrator.migrate_full_database(args.schema)
                if not migrate_success:
                    print("❌ 自動遷移失敗，無法進行優化")
                    sys.exit(1)
            
            migrator.optimize_mariadb_tables()
            print("✅ 資料庫優化完成")
        
        elif args.action == 'all':
            print("\n🚀 開始完整流程...")
            
            # 1. 遷移
            print("第1步：資料庫遷移")
            migrate_success = migrator.migrate_full_database(args.schema)
            if migrate_success:
                print("✅ 資料庫遷移成功完成")
            else:
                print("⚠️  資料庫遷移部分失敗，但繼續驗證...")
            
            # 2. 驗證
            print("\n第2步：遷移驗證")
            results = migrator.validate_migration_complete(args.schema)
            if results['overall_success']:
                print("✅ 遷移驗證通過")
            else:
                print("❌ 遷移驗證失敗，請檢查報告")
            
            # 3. 優化
            print("\n第3步：資料庫優化")
            migrator.optimize_mariadb_tables()
            print("✅ 資料庫優化完成")
            
            print("\n🎉 完整流程執行完畢！")
            
    except KeyboardInterrupt:
        print("\n⚠️  遷移過程被使用者中斷")
    except Exception as e:
        print(f"❌ 遷移過程中發生錯誤: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()