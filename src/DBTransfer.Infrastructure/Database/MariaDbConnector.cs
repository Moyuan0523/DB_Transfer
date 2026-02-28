using System;
using System.Linq;
using System.Collections.Generic;
using DBTransfer.Core.Interfaces;
using DBTransfer.Core.Models;
using MySqlConnector;
using DBTransfer.Core.Utils;
using Microsoft.Data.SqlClient;

namespace DBTransfer.Infrastructure.Database;

/// <summary>
/// MariaDB 資料庫連接器
/// </summary>
public class MariaDbConnector : IDatabaseConnector
{
    private MySqlConnection? _connection;
    private string _connectionString;

    /// <summary>
    /// 初始化連接字串
    /// </summary>
    /// <param name="connectionString">連線資訊（server ip, port, database, user...），從 .env 檔讀取</param>
    public MariaDbConnector(string connectionString)
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
            _connection = new MySqlConnection(_connectionString);
            _connection.Open();
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
            if(IsConnectionOpen() && _connection != null)
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
            using(var testConnection = new MySqlConnection(_connectionString))
            {
                testConnection.Open();
                return true;
            }
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
        
        var builder = new MySqlConnectionStringBuilder(_connectionString);
        if(_connection != null && IsConnectionOpen())
        {
            builder.Database = _connection.Database;
        }
        if(!string.IsNullOrEmpty(builder.Password))
        {
            builder.Password = "****";
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
            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return new List<string>();
            }

            // MySQL 的 INFORMATION_SCHEMA 查詢
            // 注意：MySQL 中 TABLE_SCHEMA 就是資料庫名稱
            string sql = @"SELECT TABLE_NAME 
                           FROM INFORMATION_SCHEMA.TABLES 
                           WHERE TABLE_SCHEMA = DATABASE() 
                           AND TABLE_TYPE = 'BASE TABLE'
                           ORDER BY TABLE_NAME";

            using(var command = new MySqlCommand(sql, _connection))
            {
                using(var reader = command.ExecuteReader())
                {
                    var res = new List<string>();
                    while(reader.Read())
                    {
                        string value = reader.GetString(0);
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
        try
        {
            if(string.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("資料表名稱不可為空");
                return null;
            }

            if(!IsConnectionOpen() || _connection == null)
            {
                Console.WriteLine("連線尚未建立");
                return null;
            }

            // 白名單驗證
            if(!WhiteListValidation(tableName))
            {
                Console.WriteLine($"表 {tableName} 不存在");
                return null;
            }

            // MySQL 中不需要處理 schema，直接使用表名
            // 取得資料庫名稱作為 schema
            string schema = _connection.Database;

            // 查詢欄位名稱
            string sql1 = @"SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = @tableName
                            ORDER BY ORDINAL_POSITION";

            var colNames = new List<string>();

            using(var command1 = new MySqlCommand(sql1, _connection))
            {
                command1.Parameters.AddWithValue("@tableName", tableName);
                
                using(var reader = command1.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        string value = reader.GetString(0);
                        colNames.Add(value);
                    }
                }
            }

            // 統計行數
            string sql2 = $"SELECT COUNT(*) FROM `{tableName}`";

            int rowCount;

            using(var command2 = new MySqlCommand(sql2, _connection))
            {
                object? res = command2.ExecuteScalar();
                if(res == null)
                {
                    rowCount = 0;
                }
                else
                {
                    rowCount = Convert.ToInt32(res);
                }       
            }

            return new TableInfo(
                tableName,
                schema,
                colNames,
                rowCount
            );
        }
        catch(Exception ex)
        {
            Console.WriteLine($"讀取表資料失敗：{ex.Message}");
            return null;
        }
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

            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return new List<Dictionary<string, object>>();
            }

            // 白名單驗證
            if(!WhiteListValidation(tableName))
            {
                Console.WriteLine($"表 {tableName} 不存在");
                return new List<Dictionary<string, object>>();
            }

            // MySQL 使用反引號包裹表名
            string sql = $"SELECT * FROM `{tableName}`";

            using(var command = new MySqlCommand(sql, _connection))
            {
                using(var reader = command.ExecuteReader())
                {
                    var data = new List<Dictionary<string, object>>();
                    
                    while(reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        
                        for(int i = 0; i < reader.FieldCount; i++)
                        {
                            string columnName = reader.GetName(i);
                            
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

            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }

            if(!WhiteListValidation(tableName))
            {
                Console.WriteLine($"表 {tableName} 不存在");
                return false;
            }

            int successCount = 0;

            foreach(var row in data)
            {
                if(row.Count == 0) continue;

                // 取得欄位名稱
                List<string> columns = row.Keys.ToList();

                // 生成欄位部分（使用反引號）
                string columnsPart = string.Join(", ", columns.Select(c => $"`{c}`"));

                // 生成參數部分
                List<string> parameters = columns.Select(col => "@" + col).ToList();
                string parametersPart = string.Join(", ", parameters);

                // MySQL 使用反引號包裹表名和欄位名
                string sql = $"INSERT INTO `{tableName}` ({columnsPart}) VALUES ({parametersPart})";

                using(var command = new MySqlCommand(sql, _connection))
                {
                    foreach(var column in columns)
                    {
                        object value = row[column];
                        
                        if(value == DBNull.Value)
                        {
                            command.Parameters.AddWithValue("@" + column, DBNull.Value);
                        }
                        else
                        {
                            command.Parameters.AddWithValue("@" + column, value);
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

    // ========== 私有輔助方法 ==========

    /// <summary>
    /// 檢查連線
    /// </summary>
    private bool IsConnectionOpen()
    {
        return _connection != null && _connection.State == System.Data.ConnectionState.Open;
    }

    /// <summary>
    /// 白名單驗證
    /// </summary>
    private bool WhiteListValidation(string tableName)
    {
        List<string> validNames = GetTableNames();
        return validNames.Contains(tableName);
    }

    // ========== 第三組：資料庫管理方法 ==========
    // TODO: 實作以下三個方法
    public bool DatabaseExists(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("資料庫名稱不可為空");
                return false;
            }
            string sql = @"SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.SCHEMATA
                        WHERE SCHEMA_NAME = @dbName";
            using(var command = new MySqlCommand(sql, _connection))
            {
                command.Parameters.AddWithValue("@dbName", databaseName);
                object? res = command.ExecuteScalar();
                int count;
                if(res == null)
                {
                    count = 0;
                } 
                else
                {
                    count = Convert.ToInt32(res);
                }
                return count > 0;
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"資料庫存在查詢失敗：{ex.Message}");
            return false;
        }
    }
    public bool CreateDatabase(string databaseName) 
    {
        try
        {
            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("資料庫名稱不可為空");
                return false;
            }
            if(DatabaseExists(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 已存在");
                return true;
            }
            
            if(!TableNameConverter.IsValidMariaDBTableName(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 名稱不合法，只允許英文字母、數字、底線");
                return false;
            }
            
            string sql = $@"CREATE DATABASE `{databaseName}`
                           CHARACTER SET utf8mb4
                           COLLATE utf8mb4_unicode_ci";
            using(var command = new MySqlCommand(sql, _connection))
            {
                command.ExecuteNonQuery();
                if(DatabaseExists(databaseName))
                {
                    Console.WriteLine($"資料庫 '{databaseName}' 建立成功");
                    return true;
                }
                else
                {
                    Console.WriteLine($"資料庫 '{databaseName}' 建立失敗");
                    return false;
                }
            }
            
        }
        catch(Exception ex)
        {
            Console.WriteLine($"資料庫建立失敗：{ex.Message}");
            return false;
        }
    }

    public bool DeleteDatabase(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen() || _connection == null)
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("資料庫名稱不可為空");
                return false;
            }
            if(!DatabaseExists(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 不存在");
                return false;
            }
            if(!TableNameConverter.IsValidMariaDBTableName(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 名稱不合法，只允許英文字母、數字、底線");
                return false;
            }

            string sql = $"DROP DATABASE `{databaseName}`";
            using(var command = new MySqlCommand(sql, _connection))
            {
                command.ExecuteNonQuery();
                if(!DatabaseExists(databaseName))
                {
                    Console.WriteLine($"資料庫 '{databaseName}' 刪除成功");
                    return true;
                }
                else
                {
                    Console.WriteLine($"資料庫 '{databaseName}' 刪除失敗");
                    return false;
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"刪除資料庫失敗：{ex.Message}");
            return false;
        }
    }

    public bool UseDatabase(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen() || _connection == null)
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("資料庫名稱不可為空");
                return false;
            }
            if(!DatabaseExists(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 不存在");
                return false;
            }

            // 查詢目前連接之資料庫
            if(_connection != null && !_connection.Database.Equals(databaseName))
            {
                Console.WriteLine($"目前連線資料庫為 '{_connection.Database}'，開始切換...");
            }
            else
            {
                Console.WriteLine($"'{databaseName}' 便為目前連接之資料庫");
                return true;
            }

            // 切換資料庫
            _connection.ChangeDatabase(databaseName);
            Console.WriteLine($"切換成功，目前資料庫為 '{_connection.Database}'");
            return true;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"資料庫切換失敗：{ex.Message}");
            return false;
        }
    }
}
