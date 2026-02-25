#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MSSQL 資料庫建立輔助腳本
用途：自動化建立測試資料庫
"""

import pyodbc
import argparse
import sys
import os

def test_connection(server, username=None, password=None, use_windows_auth=False):
    """測試 MSSQL 連接"""
    print(f"\n🔍 測試連接到 {server}...")
    
    try:
        if use_windows_auth:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password}"
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # 測試查詢
        cursor.execute("SELECT @@VERSION, @@SERVERNAME")
        result = cursor.fetchone()
        
        print("✅ 連接成功！")
        print(f"   伺服器名稱: {result[1]}")
        print(f"   SQL Server 版本: {result[0][:80]}...")
        
        # 列出現有資料庫
        cursor.execute("SELECT name FROM sys.databases ORDER BY name")
        databases = [row[0] for row in cursor.fetchall()]
        print(f"\n📋 現有資料庫 ({len(databases)} 個):")
        for db in databases:
            print(f"   - {db}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 連接失敗: {str(e)}")
        print("\n💡 可能的解決方案：")
        print("   1. 確認 SQL Server 服務正在運行")
        print("   2. 確認伺服器地址和埠號正確")
        print("   3. 確認防火牆設定")
        print("   4. 確認 ODBC Driver 已安裝")
        print("\n檢查已安裝的 ODBC Driver:")
        drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
        for driver in drivers:
            print(f"   - {driver}")
        return False

def create_database(server, username=None, password=None, use_windows_auth=False, sql_file='setup_database.sql'):
    """執行 SQL 腳本建立資料庫"""
    print(f"\n🚀 開始建立資料庫...")
    
    # 檢查 SQL 檔案是否存在
    if not os.path.exists(sql_file):
        print(f"❌ 找不到 SQL 檔案: {sql_file}")
        return False
    
    try:
        # 連接到 master 資料庫
        if use_windows_auth:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;Trusted_Connection=yes"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;UID={username};PWD={password}"
        
        conn = pyodbc.connect(conn_str, timeout=30)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print(f"📄 讀取 SQL 檔案: {sql_file}")
        
        # 讀取 SQL 檔案
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割 SQL 語句（以 GO 為分隔符）
        sql_commands = []
        current_command = []
        
        for line in sql_content.split('\n'):
            # 跳過註解
            if line.strip().startswith('--'):
                continue
            
            # 檢查是否為 GO 語句
            if line.strip().upper() == 'GO':
                if current_command:
                    sql_commands.append('\n'.join(current_command))
                    current_command = []
            else:
                current_command.append(line)
        
        # 加入最後一個命令
        if current_command:
            sql_commands.append('\n'.join(current_command))
        
        print(f"📝 準備執行 {len(sql_commands)} 個 SQL 命令...")
        
        # 執行每個 SQL 命令
        success_count = 0
        error_count = 0
        
        for i, command in enumerate(sql_commands, 1):
            command = command.strip()
            if not command or command.startswith('--'):
                continue
            
            try:
                # 顯示執行進度
                if i % 10 == 0:
                    print(f"   進度: {i}/{len(sql_commands)}")
                
                cursor.execute(command)
                
                # 如果有輸出訊息，顯示它
                while cursor.nextset():
                    pass
                
                success_count += 1
                
            except pyodbc.Error as e:
                # 某些錯誤可以忽略（例如物件已存在）
                error_msg = str(e)
                if 'already exists' not in error_msg.lower():
                    print(f"⚠️  命令 {i} 執行失敗: {error_msg[:100]}")
                    error_count += 1
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ 資料庫建立完成!")
        print(f"   成功: {success_count} 個命令")
        if error_count > 0:
            print(f"   失敗: {error_count} 個命令")
        
        return True
        
    except Exception as e:
        print(f"❌ 建立資料庫失敗: {str(e)}")
        return False

def verify_database(server, username=None, password=None, use_windows_auth=False, database='TestSourceDB'):
    """驗證資料庫建立結果"""
    print(f"\n🔍 驗證資料庫 {database}...")
    
    try:
        if use_windows_auth:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # 檢查表格
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"\n📊 資料表 ({len(tables)} 個):")
        for table in tables:
            # 計算每個表格的資料筆數
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            print(f"   ✓ {table}: {count} 筆資料")
        
        # 檢查檢視表
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.VIEWS
            ORDER BY TABLE_NAME
        """)
        views = [row[0] for row in cursor.fetchall()]
        
        if views:
            print(f"\n👁️  檢視表 ({len(views)} 個):")
            for view in views:
                print(f"   ✓ {view}")
        
        # 檢查預存程序
        cursor.execute("""
            SELECT ROUTINE_NAME 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_NAME
        """)
        procedures = [row[0] for row in cursor.fetchall()]
        
        if procedures:
            print(f"\n⚙️  預存程序 ({len(procedures)} 個):")
            for proc in procedures:
                print(f"   ✓ {proc}")
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ 資料庫驗證成功！")
        return True
        
    except Exception as e:
        print(f"❌ 驗證失敗: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='MSSQL 資料庫建立輔助腳本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用範例：
  # 測試連接（SQL Server 驗證）
  python setup_database.py --server "192.168.1.100,1433" --username sa --password YourPassword --action test
  
  # 測試連接（Windows 驗證）
  python setup_database.py --server "localhost\SQLEXPRESS" --windows-auth --action test
  
  # 建立資料庫
  python setup_database.py --server "192.168.1.100,1433" --username sa --password YourPassword --action create
  
  # 驗證資料庫
  python setup_database.py --server "192.168.1.100,1433" --username sa --password YourPassword --action verify
  
  # 完整流程（測試、建立、驗證）
  python setup_database.py --server "192.168.1.100,1433" --username sa --password YourPassword --action all
        """
    )
    
    parser.add_argument('--server', required=True, help='SQL Server 伺服器地址（例如：localhost,1433 或 192.168.1.100\\SQLEXPRESS）')
    parser.add_argument('--username', help='SQL Server 使用者名稱（使用 SQL Server 驗證時必填）')
    parser.add_argument('--password', help='SQL Server 密碼（使用 SQL Server 驗證時必填）')
    parser.add_argument('--windows-auth', action='store_true', help='使用 Windows 驗證')
    parser.add_argument('--action', choices=['test', 'create', 'verify', 'all'], default='all',
                      help='執行動作：test=測試連接, create=建立資料庫, verify=驗證資料庫, all=全部執行（預設）')
    parser.add_argument('--sql-file', default='setup_database.sql', help='SQL 腳本檔案路徑（預設：setup_database.sql）')
    parser.add_argument('--database', default='TestSourceDB', help='資料庫名稱（預設：TestSourceDB）')
    
    args = parser.parse_args()
    
    # 檢查驗證方式
    if not args.windows_auth and (not args.username or not args.password):
        print("❌ 錯誤：使用 SQL Server 驗證時必須提供 --username 和 --password")
        print("   或者使用 --windows-auth 參數使用 Windows 驗證")
        sys.exit(1)
    
    print("=" * 60)
    print("    MSSQL 資料庫建立輔助腳本")
    print("=" * 60)
    print(f"伺服器: {args.server}")
    print(f"驗證方式: {'Windows 驗證' if args.windows_auth else 'SQL Server 驗證'}")
    if not args.windows_auth:
        print(f"使用者: {args.username}")
    print(f"動作: {args.action}")
    print("=" * 60)
    
    success = True
    
    # 執行測試
    if args.action in ['test', 'all']:
        if not test_connection(args.server, args.username, args.password, args.windows_auth):
            success = False
            if args.action == 'all':
                print("\n❌ 連接測試失敗，無法繼續")
                sys.exit(1)
    
    # 執行建立
    if args.action in ['create', 'all'] and success:
        if not create_database(args.server, args.username, args.password, args.windows_auth, args.sql_file):
            success = False
            if args.action == 'all':
                print("\n⚠️  資料庫建立失敗，但繼續驗證...")
    
    # 執行驗證
    if args.action in ['verify', 'all'] and success:
        if not verify_database(args.server, args.username, args.password, args.windows_auth, args.database):
            success = False
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 所有操作完成！")
        print("\n下一步：")
        print(f"1. 使用 Azure Data Studio 或 SSMS 連接到 {args.server}")
        print(f"2. 瀏覽 {args.database} 資料庫")
        print("3. 開始進行資料庫轉移測試")
    else:
        print("⚠️  部分操作失敗，請檢查上方錯誤訊息")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  操作被使用者中斷")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 發生未預期的錯誤: {str(e)}")
        sys.exit(1)
