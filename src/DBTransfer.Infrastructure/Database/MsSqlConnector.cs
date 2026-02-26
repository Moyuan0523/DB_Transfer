using System;
using System.Linq;
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
        catch
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
       try
       {
            if(_connection == null || _connection.State != System.Data.ConnectionState.Open)
            {
                Console.WriteLine("資料庫連線未開啟");
                return new List<string>();
            }
            // 查詢所有表名稱（包含 schema）
            string sql = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME";
            using(var command = new SqlCommand(sql, _connection))
            {
                using(var reader = command.ExecuteReader())
                {
                    var res = new List<string>();
                    while(reader.Read()){
                        string value = reader.GetString(0);
                        // 把 value push 進 res
                        res.Add(value);
                    }
                    return res;
                }
            }

       }
       catch(Exception ex)
       {
            Console.WriteLine($"查詢資料表名稱失敗：{ex.Message}");
            return new List<string>();
       }
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
        try
        {
            if(string.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("資料表名稱不可為空");
                return new List<Dictionary<string, object>>();
            }
            if(_connection == null || _connection.State != System.Data.ConnectionState.Open)
            {
                Console.WriteLine("資料庫連線未開啟");
                return new List<Dictionary<string, object>>();
            }

            // 白名單驗證
            List<string> validTables = GetTableNames();
            if(!validTables.Contains(tableName))
            {
                throw new ArgumentException($"表 {tableName} 不存在");
            }
            
            // 表名直接用字串插值，不能用參數化查詢
            string sql = $"SELECT * FROM {tableName}";
            
            using(var command = new SqlCommand(sql, _connection))
            {
                using(var reader = command.ExecuteReader())
                {
                    var data = new List<Dictionary<string, object>>();
                    while(reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for(int i=0; i<reader.FieldCount; i++)
                        {
                            string columnName = reader.GetName(i); //取得欄位名稱
                            if(reader.IsDBNull(i))
                            {
                                row[columnName] = DBNull.Value;
                            }
                            else
                            {
                                row[columnName] = reader.GetValue(i);
                            }
                        }
                        data.Add(row);
                    }
                    return data;
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"讀取表資料失敗：{ex.Message}");
            return new List<Dictionary<string, object>>();
        }
    }

    /// <summary>
    /// 寫入資料到指定表
    /// </summary>
    /// <param name="tableName">表名稱</param>
    /// <param name="data">要寫入的資料</param>
    public bool InsertData(string tableName, List<Dictionary<string, object>> data)
    {
        try
        {
            if(string.IsNullOrEmpty(tableName) || data == null || data.Count == 0)
            {
                Console.WriteLine("表名稱或寫入之資料不可為空");
                return false;
            }
            if(_connection == null || _connection.State != System.Data.ConnectionState.Open)
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }

            int successCount = 0;

            foreach(var row in data)
            {
                if(row.Count == 0) continue;

                // 取得欄位名稱
                List<string> columns = row.Keys.ToList(); 
                // 結果：["CurrencyCode", "Name", "ModifiedDate"]
        
                // 生成欄位部分
                string columnsPart = string.Join(", ", columns);
                // 結果："CurrencyCode, Name, ModifiedDate"

                // 生成參數部分（@欄位名稱）
                List<string> parameters = columns.Select(col => "@"+col).ToList();
                string parametersPart = string.Join(", ", parameters);
                // 結果："@CurrencyCode, @Name, @ModifiedDate"

                string sql = $"INSERT INTO {tableName}({columnsPart}) VALUES ({parametersPart})";

                using(var command = new SqlCommand(sql, _connection))
                {
                    foreach(var column in row.Keys)
                    {
                        object value = row[column];
                        if(value == DBNull.Value)
                        {
                            command.Parameters.AddWithValue("@" + column, DBNull.Value);
                        }
                        else{
                            command.Parameters.AddWithValue("@" + column, row[column]);
                        }
                    }
                    command.ExecuteNonQuery();
                    successCount++;
                }
            }
            Console.WriteLine($"成功插入 {successCount} 筆資料");
            return true;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"資料寫入失敗：{ex.Message}");
            return false;
        }
    }
}
