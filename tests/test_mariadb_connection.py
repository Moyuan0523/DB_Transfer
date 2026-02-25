#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MariaDB 連接測試腳本
用於驗證 MariaDB 資料庫連接和基本功能

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
    import pymysql
except ImportError:
    print("❌ 未安裝 pymysql！")
    print("\n請先安裝：")
    print("  pip install pymysql")
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

# ==================== 從環境變數讀取連接設定 ====================
HOST = os.getenv('MARIADB_HOST', 'localhost')
PORT = int(os.getenv('MARIADB_PORT', '3306'))
USER = os.getenv('MARIADB_USER', 'root')
PASSWORD = os.getenv('MARIADB_PASSWORD')
REMOTE_HOST = os.getenv('REMOTE_HOST', '140.116.96.67')
SSH_USER = os.getenv('SSH_USER', 'yan')

if not PASSWORD:
    print("❌ 未設定 MARIADB_PASSWORD 環境變數！")
    print("\n請在 .env 檔案中設定：")
    print("  MARIADB_PASSWORD=your_password")
    sys.exit(1)
# ================================================================

def print_separator(char='=', length=70):
    """列印分隔線"""
    print(char * length)

def check_ssh_tunnel():
    """提示 SSH 隧道"""
    print("\n💡 遠端連接提示:")
    print(f"   如果連接到遠端 MariaDB ({REMOTE_HOST})，")
    print("   請確保已在另一個終端執行 SSH 隧道：")
    print(f"   ssh -f -N -L 1433:localhost:1433 -L 3306:localhost:3306 {SSH_USER}@{REMOTE_HOST}")
    print("\n💡 一條命令同時轉發 MSSQL 和 MariaDB 端口！")
    print()

def test_connection():
    """測試 MariaDB 連接"""
    print_separator()
    print("  MariaDB 連接測試")
    print_separator()
    
    import pymysql
    
    # SSH 隧道提示
    check_ssh_tunnel()
    
    # 嘗試連接
    print(f"正在連接到 MariaDB...")
    print(f"  主機: {HOST}")
    print(f"  連接埠: {PORT}")
    print(f"  使用者: {USER}")
    
    try:
        import pymysql
        
        conn = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            connect_timeout=10,
            charset='utf8mb4'
        )
        
        print("✅ 連接成功！\n")
        
        cursor = conn.cursor()
        
        # 取得 MariaDB 版本
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"📊 MariaDB 版本: {version}")
        
        # 取得目前時間
        cursor.execute("SELECT NOW()")
        current_time = cursor.fetchone()[0]
        print(f"🕐 伺服器時間: {current_time}")
        
        # 列出所有資料庫
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"\n📁 資料庫列表 ({len(databases)} 個):")
        for db in databases:
            print(f"   • {db[0]}")
        
        # 取得字元集
        cursor.execute("SHOW VARIABLES LIKE 'character_set%'")
        print("\n🔤 字元集設定:")
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]}")
        
        # 測試建立和刪除資料庫
        print("\n🧪 測試資料庫操作...")
        test_db = 'test_connection_db'
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            print(f"   ✅ 建立測試資料庫: {test_db}")
            
            cursor.execute(f"DROP DATABASE {test_db}")
            print(f"   ✅ 刪除測試資料庫: {test_db}")
        except Exception as e:
            print(f"   ⚠️  資料庫操作受限: {e}")
        
        # 清理
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("🎉 所有測試通過！")
        print("=" * 70)
        print("\n可以開始使用 MariaDB 了！")
        return True
        
    except Exception as e:
        print(f"\n❌ 連接失敗: {e}")
        print("\n💡 故障排除:")
        
        if "Can't connect" in str(e) or "Connection refused" in str(e):
            print("  1. 檢查 SSH 隧道是否已建立")
            print(f"     在另一個終端執行: ssh -f -N -L 1433:localhost:1433 -L 3306:localhost:3306 {SSH_USER}@{REMOTE_HOST}")
            print("  2. 檢查 MariaDB 服務是否執行")
            print("     Docker: docker ps | grep mariadb")
            print("     系統服務: sudo systemctl status mariadb")
        elif "Access denied" in str(e):
            print("  1. 檢查使用者名稱和密碼是否正確")
            print(f"     目前使用者: {USER}")
            print("  2. 檢查使用者權限")
        else:
            print("  1. 檢查網路連接")
            print("  2. 檢查防火牆設定")
            print("  3. 查看詳細錯誤訊息（如下）")
        
        import traceback
        print("\n" + "-" * 70)
        print("詳細錯誤訊息:")
        print("-" * 70)
        traceback.print_exc()
        return False

if __name__ == "__main__":
    try:
        success = test_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  使用者中斷測試")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 程式異常: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
