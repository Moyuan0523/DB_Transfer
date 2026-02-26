using System;
using System.Collections.Generic;
using DBTransfer.Core.Interfaces;
using DBTransfer.Core.Models;
using Microsoft.Data.SqlClient;

namespace DBTransfer.Infrastructure.Database;

/// <summary>
/// MSSQL 資料庫連接器
/// </summary>
public class MsSqlConnector : IDatabaseConnector
{
    private SqlConnection? _connection; //_ 表示私有
    private string _connectionString;

    /// <summary>
    /// 初始化連接字串
    /// </summary>
    /// <param name="connectionString">連線資訊（server ip, port, user...），從 .env 檔讀取</param> 
    public MsSqlConnector(string connectionString)
    {
        _connectionString = connectionString;
    }

    // ========== 第一組：連線管理方法 ==========

    /// <summary>
    /// 連接資料庫
    /// </summary>
    public bool Connect()
    {
        try
        {
            _connection = new SqlConnection(_connectionString);
            _connection.Open(); //開啟連線
            return true;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"連線錯誤：{ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// 斷開連線
    /// </summary>
    public bool Disconnect()
    {
        try
        {
            if(_connection != null && _connection.State == System.Data.ConnectionState.Open)
            { 
                _connection.Close();
                Console.WriteLine("資料庫連線已關閉");
            }
            return true;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"斷開連線錯誤：{ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// 測試連線
    /// </summary>
    public bool TestConnection()
    {
        try
        {
            using(var testConnection = new SqlConnection(_connectionString))
            {
                testConnection.Open(); //建立連線
                return true;
            } // using 結束後自動丟棄物件
        }
        catch(Exception ex)
        {
            return false;
        }

    }

    /// <summary>
    /// 取得當前連接字串
    /// </summary>
    public string GetConnectionString()
    {
        if(string.IsNullOrEmpty(_connectionString))
        {
            return "連線配置尚未設定";
        }
        var builder = new SqlConnectionStringBuilder(_connectionString);
        if(!string.IsNullOrEmpty(builder.Password))
        {
            builder.Password = "****"; // 隱藏密碼
        }
        return builder.ConnectionString;
    }

    // ========== 第二組：資料庫查詢方法 ==========

    /// <summary>
    /// 獲取所有表的名稱
    /// </summary>
    public List<string> GetTableNames()
    {
        // TODO: 待實作
        throw new NotImplementedException();
    }

    /// <summary>
    /// 獲取指定表的結構資訊
    /// </summary>
    /// <param name="tableName">表名稱</param>
    /// <returns>如果表存在則回傳 TableInfo，否則回傳 null</returns>
    public TableInfo? GetTableStructure(string tableName)
    {
        // TODO: 待實作
        throw new NotImplementedException();
    }

    // ========== 第三組：資料讀寫方法 ==========

    /// <summary>
    /// 讀取指定表的資料
    /// </summary>
    /// <param name="tableName">表名稱</param>
    public List<Dictionary<string, object>> GetTableData(string tableName)
    {
        // TODO: 待實作
        throw new NotImplementedException();
    }

    /// <summary>
    /// 寫入資料到指定表
    /// </summary>
    /// <param name="tableName">表名稱</param>
    /// <param name="data">要寫入的資料</param>
    public bool InsertData(string tableName, List<Dictionary<string, object>> data)
    {
        // TODO: 待實作
        throw new NotImplementedException();
    }
}
