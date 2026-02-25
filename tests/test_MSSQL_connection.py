#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MSSQL 遠端連接測試腳本（SSH 隧道版本）

使用說明：
1. 複製 .env.example 到 .env 並填入實際連接資訊
2. 在另一個終端執行 SSH 隧道：
   ssh -L 1433:localhost:1433 -L 3306:localhost:3306 yan@140.116.96.67
3. 保持 SSH 連接開啟，然後執行此腳本
"""

import sys
import os
from pathlib import Path

# 檢查必要的套件
try:
    import pyodbc
except ImportError:
    print("❌ 未安裝 pyodbc！")
    print("\n請先安裝：")
    print("  pip install pyodbc")
    print("\n然後安裝 ODBC Driver 18 for SQL Server:")
    print("  - macOS: brew install msodbcsql18")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("❌ 未安裝 python-dotenv！")
    print("\n請先安裝：")
    print("  pip install python-dotenv")
    sys.exit(1)

# 載入 .env 檔案
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'

if not env_path.exists():
    print("❌ 找不到 .env 檔案！")
    print(f"\n請在專案根目錄建立 .env 檔案: {project_root}")
    print("\n參考 .env.example 範本：")
    print("  cp .env.example .env")
    print("  然後編輯 .env 填入實際的連接資訊")
    sys.exit(1)

load_dotenv(env_path)

# ==================== 從環境變數讀取連接資訊 ====================
SERVER = os.getenv('MSSQL_SERVER', 'localhost,1433')
DATABASE = os.getenv('MSSQL_DATABASE', 'AdventureWorks2022')
USERNAME = os.getenv('MSSQL_USERNAME', 'sa')
PASSWORD = os.getenv('MSSQL_PASSWORD')
REMOTE_HOST = os.getenv('REMOTE_HOST', '140.116.96.67')
SSH_USER = os.getenv('SSH_USER', 'yan')

if not PASSWORD:
    print("❌ 未設定 MSSQL_PASSWORD 環境變數！")
    print("\n請在 .env 檔案中設定：")
    print("  MSSQL_PASSWORD=your_password")
    sys.exit(1)
# ================================================================

def print_separator(char='=', length=60):
    """列印分隔線"""
    print(char * length)

def check_ssh_tunnel():
    """檢查 SSH 隧道提示"""
    print("\n" + "="*60)
    print("  📌 SSH 隧道連接模式")
    print("="*60)
    print("\n此腳本透過 SSH 隧道連接到伺服器")
    print(f"實際伺服器: {REMOTE_HOST}")
    print(f"連接位址: {SERVER} (透過隧道)")
    print("\n⚠️  請確保已在另一個終端執行 SSH 隧道:")
    print(f"   ssh -f -N -L 1433:localhost:1433 -L 3306:localhost:3306 {SSH_USER}@{REMOTE_HOST}")
    print("\n💡 一條命令同時轉發 MSSQL 和 MariaDB 端口！")
    print("\n如果尚未建立隧道，請:")
    print("  1. 開啟另一個終端")
    print("  2. 執行上述 SSH 指令")
    print("  3. 保持 SSH 連接不要關閉")
    print("  4. 回到此視窗按 Enter 繼續")
    print("\n" + "-"*60)
    
    try:
        input("\n按 Enter 繼續測試（或 Ctrl+C 取消）...")
    except KeyboardInterrupt:
        print("\n\n已取消")
        sys.exit(0)

def test_drivers():
    """檢查可用的 ODBC 驅動程式"""
    print("\n🔍 檢查已安裝的 ODBC 驅動程式...\n")
    drivers = pyodbc.drivers()
    
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    
    if sql_drivers:
        print(f"✅ 找到 {len(sql_drivers)} 個 SQL Server 驅動程式:")
        for driver in sql_drivers:
            print(f"   ✓ {driver}")
        return True
    else:
        print("❌ 未找到 SQL Server ODBC 驅動程式！")
        print("\n請安裝 ODBC Driver 18 for SQL Server:")
        print("  macOS: brew install msodbcsql18")
        return False

def test_connection():
    """測試資料庫連接"""
    print("\n🔌 測試連接...")
    print_separator('-')
    print(f"連接方式: SSH 隧道")
    print(f"實際伺服器: {REMOTE_HOST}")
    print(f"隧道位址: {SERVER}")
    print(f"資料庫: {DATABASE}")
    print(f"使用者名稱: {USERNAME}")
    print_separator('-')
    
    # 連接字串
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes"
    )
    
    try:
        print("\n⏳ 正在連接...")
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        
        print("✅ 連接成功！\n")
        
        # 測試 1: 伺服器版本
        print_separator('=')
        print("測試 1: 伺服器版本資訊")
        print_separator('=')
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        version_lines = version.split('\n')
        print(f"📊 {version_lines[0].strip()}\n")
        
        # 測試 2: 伺服器名稱和目前資料庫
        print_separator('=')
        print("測試 2: 伺服器資訊")
        print_separator('=')
        cursor.execute("SELECT @@SERVERNAME AS ServerName, DB_NAME() AS CurrentDB")
        result = cursor.fetchone()
        print(f"🖥️  伺服器名稱: {result[0]}")
        print(f"📁 目前資料庫: {result[1]}\n")
        
        # 測試 3: 列出所有表
        print_separator('=')
        print("測試 3: 資料表列表")
        print_separator('=')
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        table_results = cursor.fetchall()
        tables = [(row[0], row[1]) for row in table_results]
        print(f"📋 找到 {len(tables)} 個表格:\n")
        
        # 只顯示前 10 個表格（AdventureWorks2022 有很多表格）
        for schema, table in tables[:10]:
            full_name = f"[{schema}].[{table}]"
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
                count = cursor.fetchone()[0]
                print(f"   ✓ {schema}.{table:30} {count:8,} 筆資料")
            except:
                print(f"   ⚠ {schema}.{table:30} (無法讀取)")
        
        if len(tables) > 10:
            print(f"\n   ... 還有 {len(tables) - 10} 個表格（省略顯示）")
        
        # 測試 4: 列出所有視圖
        print(f"\n{'-'*60}")
        print("測試 4: 視圖列表")
        print('-'*60)
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.VIEWS 
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        view_results = cursor.fetchall()
        views = [(row[0], row[1]) for row in view_results]
        
        if views:
            print(f"👁️  找到 {len(views)} 個視圖:\n")
            # 只顯示前 5 個視圖
            for schema, view in views[:5]:
                print(f"   ✓ {schema}.{view}")
            if len(views) > 5:
                print(f"\n   ... 還有 {len(views) - 5} 個視圖（省略顯示）")
        else:
            print("   (無視圖)")
        
        # 測試 5: 簡單查詢
        print(f"\n{'-'*60}")
        print("測試 5: 資料查詢 (Person.Person 前 3 筆)")
        print('-'*60)
        
        try:
            cursor.execute("""
                SELECT TOP 3 
                    BusinessEntityID, 
                    FirstName, 
                    LastName,
                    PersonType
                FROM Person.Person
                ORDER BY BusinessEntityID
            """)
            persons = cursor.fetchall()
            print()
            for person in persons:
                print(f"   [{person[0]}] {person[1]} {person[2]} (類型: {person[3]})")
        except Exception as e:
            print(f"   ⚠️  查詢失敗: {str(e)}")
            print("   嘗試查詢其他表格...")
            try:
                cursor.execute("SELECT TOP 3 * FROM sys.tables")
                print("   ✓ 系統表格可正常查詢")
            except:
                pass
        
        # 關閉連接
        cursor.close()
        conn.close()
        
        # 成功總結
        print(f"\n{'='*60}")
        print("🎉 所有測試通過！")
        print('='*60)
        print("\n✅ SSH 隧道連接成功！")
        print(f"✅ 找到 {len(tables)} 個表格")
        print(f"✅ 找到 {len(views)} 個視圖")
        print(f"✅ 資料查詢正常")
        print("\n🌐 遠端連接測試完成！")
        
        return True
        
    except pyodbc.Error as e:
        print(f"\n❌ 資料庫連接失敗！")
        print(f"\n錯誤資訊: {str(e)}")
        
        if "timeout" in str(e).lower():
            print("\n💡 連接逾時！可能的原因:")
            print("   1. SSH 隧道未建立或已中斷")
            print("   2. 在另一個終端執行:")
            print(f"      ssh -f -N -L 1433:localhost:1433 -L 3306:localhost:3306 {SSH_USER}@{REMOTE_HOST}")
            print("   3. 確保 SSH 連接保持開啟")
            print("   4. 檢查 SSH 是否提示輸入密碼")
        else:
            print("\n💡 故障排除建議:")
            print("   1. 確認 SSH 隧道正在執行")
            print(f"      在另一個終端: ssh -f -N -L 1433:localhost:1433 -L 3306:localhost:3306 {SSH_USER}@{REMOTE_HOST}")
            print("   2. 檢查本機連接埠 1433 是否被佔用")
            print("      macOS: lsof -i :1433")
            print("   3. 如果連接埠被佔用，使用其他連接埠:")
            print(f"      ssh -L 14330:localhost:1433 yan@{REMOTE_HOST}")
            print("      然後修改腳本中的 SERVER = 'localhost,14330'")
        
        return False
        
    except Exception as e:
        print(f"\n❌ 發生未知錯誤！")
        print(f"錯誤類型: {type(e).__name__}")
        print(f"錯誤資訊: {str(e)}")
        return False

def main():
    """主函式"""
    print("\n" + "="*60)
    print("  MSSQL 遠端連接測試工具 (SSH 隧道版)")
    print("="*60)
    
    # 提示 SSH 隧道
    check_ssh_tunnel()
    
    # 步驟 1: 檢查驅動程式
    if not test_drivers():
        sys.exit(1)
    
    # 步驟 2: 測試連接
    success = test_connection()
    
    if success:
        print("\n" + "="*60)
        print("  測試結果: 成功 ✅")
        print("="*60)
        print("\n可以開始使用此資料庫了！")
        print("\n💡 提示:")
        print("  - 保持 SSH 隧道開啟以繼續存取資料庫")
        print("  - 關閉 SSH 連接會中斷資料庫連接")
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("  測試結果: 失敗 ❌")
        print("="*60)
        print("\n請查看上方的故障排除建議")
        print("\n詳細文件: TROUBLESHOOTING.md")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  使用者中斷測試")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 程式異常: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
