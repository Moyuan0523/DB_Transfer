#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MSSQL 远程连接测试脚本（SSH 隧道版本）

使用说明：
1. 在另一个终端运行 SSH 隧道：
   ssh -L 1433:localhost:1433 yan@140.116.96.67
   
2. 保持 SSH 连接开启，然后运行此脚本
"""

import sys

# 检查 pyodbc 是否已安装
try:
    import pyodbc
except ImportError:
    print("❌ 未安装 pyodbc！")
    print("\n请先安装：")
    print("  pip install pyodbc")
    print("\n然后安装 ODBC Driver 18 for SQL Server:")
    print("  - macOS: brew install msodbcsql18")
    sys.exit(1)

# ==================== 连接信息 ====================
# 通过 SSH 隧道连接到本地端口
SERVER = 'localhost,1433'
DATABASE = 'TestSourceDB'
USERNAME = 'sa'
PASSWORD = 'Shoco105621!'
REMOTE_HOST = '140.116.96.67'  # 实际服务器地址（用于显示）
# =================================================

def print_separator(char='=', length=60):
    """打印分隔线"""
    print(char * length)

def check_ssh_tunnel():
    """检查 SSH 隧道提示"""
    print("\n" + "="*60)
    print("  📌 SSH 隧道连接模式")
    print("="*60)
    print("\n此脚本通过 SSH 隧道连接到服务器")
    print(f"实际服务器: {REMOTE_HOST}")
    print(f"连接地址: {SERVER} (通过隧道)")
    print("\n⚠️  请确保已在另一个终端运行 SSH 隧道:")
    print(f"   ssh -L 1433:localhost:1433 yan@{REMOTE_HOST}")
    print("\n如果尚未建立隧道，请:")
    print("  1. 打开另一个终端")
    print("  2. 运行上述 SSH 命令")
    print("  3. 保持 SSH 连接不要关闭")
    print("  4. 回到此窗口按 Enter 继续")
    print("\n" + "-"*60)
    
    try:
        input("\n按 Enter 继续测试（或 Ctrl+C 取消）...")
    except KeyboardInterrupt:
        print("\n\n已取消")
        sys.exit(0)

def test_drivers():
    """检查可用的 ODBC 驱动"""
    print("\n🔍 检查已安装的 ODBC 驱动...\n")
    drivers = pyodbc.drivers()
    
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    
    if sql_drivers:
        print(f"✅ 找到 {len(sql_drivers)} 个 SQL Server 驱动:")
        for driver in sql_drivers:
            print(f"   ✓ {driver}")
        return True
    else:
        print("❌ 未找到 SQL Server ODBC 驱动！")
        print("\n请安装 ODBC Driver 18 for SQL Server:")
        print("  macOS: brew install msodbcsql18")
        return False

def test_connection():
    """测试数据库连接"""
    print("\n🔌 测试连接...")
    print_separator('-')
    print(f"连接方式: SSH 隧道")
    print(f"实际服务器: {REMOTE_HOST}")
    print(f"隧道地址: {SERVER}")
    print(f"数据库: {DATABASE}")
    print(f"用户名: {USERNAME}")
    print_separator('-')
    
    # 连接字符串
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes"
    )
    
    try:
        print("\n⏳ 正在连接...")
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        
        print("✅ 连接成功！\n")
        
        # 测试 1: 服务器版本
        print_separator('=')
        print("测试 1: 服务器版本信息")
        print_separator('=')
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        version_lines = version.split('\n')
        print(f"📊 {version_lines[0].strip()}\n")
        
        # 测试 2: 服务器名称和当前数据库
        print_separator('=')
        print("测试 2: 服务器信息")
        print_separator('=')
        cursor.execute("SELECT @@SERVERNAME AS ServerName, DB_NAME() AS CurrentDB")
        result = cursor.fetchone()
        print(f"🖥️  服务器名称: {result[0]}")
        print(f"📁 当前数据库: {result[1]}\n")
        
        # 测试 3: 列出所有表
        print_separator('=')
        print("测试 3: 数据表列表")
        print_separator('=')
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]
        print(f"📋 找到 {len(tables)} 个表:\n")
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   ✓ {table:20} {count:5,} 笔数据")
        
        # 测试 4: 列出所有视图
        print(f"\n{'-'*60}")
        print("测试 4: 视图列表")
        print('-'*60)
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.VIEWS 
            ORDER BY TABLE_NAME
        """)
        views = [row[0] for row in cursor.fetchall()]
        if views:
            print(f"👁️  找到 {len(views)} 个视图:\n")
            for view in views:
                print(f"   ✓ {view}")
        else:
            print("   (无视图)")
        
        # 测试 5: 简单查询
        print(f"\n{'-'*60}")
        print("测试 5: 数据查询 (Customers 前 3 笔)")
        print('-'*60)
        cursor.execute("SELECT TOP 3 CustomerID, CustomerCode, CustomerName, City FROM Customers")
        customers = cursor.fetchall()
        print()
        for customer in customers:
            print(f"   [{customer[0]}] {customer[1]} - {customer[2]} ({customer[3]})")
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 成功总结
        print(f"\n{'='*60}")
        print("🎉 所有测试通过！")
        print('='*60)
        print("\n✅ SSH 隧道连接成功！")
        print(f"✅ 找到 {len(tables)} 个表")
        print(f"✅ 找到 {len(views)} 个视图")
        print(f"✅ 数据查询正常")
        print("\n🌐 远程连接测试完成！")
        
        return True
        
    except pyodbc.Error as e:
        print(f"\n❌ 数据库连接失败！")
        print(f"\n错误信息: {str(e)}")
        
        if "timeout" in str(e).lower():
            print("\n💡 连接超时！可能的原因:")
            print("   1. SSH 隧道未建立或已断开")
            print("   2. 在另一个终端运行:")
            print(f"      ssh -L 1433:localhost:1433 yan@{REMOTE_HOST}")
            print("   3. 确保 SSH 连接保持开启")
            print("   4. 检查 SSH 是否提示输入密码")
        else:
            print("\n💡 故障排除建议:")
            print("   1. 确认 SSH 隧道正在运行")
            print(f"      在另一个终端: ssh -L 1433:localhost:1433 yan@{REMOTE_HOST}")
            print("   2. 检查本地端口 1433 是否被占用")
            print("      macOS: lsof -i :1433")
            print("   3. 如果端口被占用，使用其他端口:")
            print(f"      ssh -L 14330:localhost:1433 yan@{REMOTE_HOST}")
            print("      然后修改脚本中的 SERVER = 'localhost,14330'")
        
        return False
        
    except Exception as e:
        print(f"\n❌ 发生未知错误！")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {str(e)}")
        return False

def main():
    """主函数"""
    print("\n" + "="*60)
    print("  MSSQL 远程连接测试工具 (SSH 隧道版)")
    print("="*60)
    
    # 提示 SSH 隧道
    check_ssh_tunnel()
    
    # 步骤 1: 检查驱动
    if not test_drivers():
        sys.exit(1)
    
    # 步骤 2: 测试连接
    success = test_connection()
    
    if success:
        print("\n" + "="*60)
        print("  测试结果: 成功 ✅")
        print("="*60)
        print("\n可以开始使用此数据库了！")
        print("\n💡 提示:")
        print("  - 保持 SSH 隧道开启以继续访问数据库")
        print("  - 关闭 SSH 连接会断开数据库连接")
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("  测试结果: 失败 ❌")
        print("="*60)
        print("\n请查看上方的故障排除建议")
        print("\n详细文档: TROUBLESHOOTING.md")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断测试")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 程序异常: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
