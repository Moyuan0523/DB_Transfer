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
        if(IsConnectionOpen() && _connection != null)
        {
            builder.InitialCatalog = _connection.Database;
        }
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
            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return new List<string>();
            }
            // 查詢所有表名稱（包含 schema）
            string sql = @"SELECT TABLE_SCHEMA + '.' + TABLE_NAME 
                           FROM INFORMATION_SCHEMA.TABLES 
                           WHERE TABLE_TYPE = 'BASE TABLE' 
                           ORDER BY TABLE_SCHEMA, TABLE_NAME";
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
        try
        {
            if(string.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("資料表名稱不可為空");
                return null;
            }
            if(!IsConnectionOpen())
            {
                Console.WriteLine("連線尚未建立");
                return null;
            }

            //白名單驗證
            if(!WhiteListValidation(tableName))
            {
                Console.WriteLine($"表 {tableName} 不存在");
                return null;
            }

            // tableName 可能包含 or 不包含 shema
            var parts = tableName.Split('.', 2, StringSplitOptions.RemoveEmptyEntries);
            string schema = parts.Length == 2 ? parts[0] : "dbo";
            string spiltTableName = parts.Length == 1 ? parts[0] : parts[1];

            string sql1 = $@"SELECT COLUMN_NAME 
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{spiltTableName}'
                           ORDER BY ORDINAL_POSITION";
            var colNames = new List<string>();            
            using(var command1 = new SqlCommand(sql1, _connection))
            {
                using(var reader = command1.ExecuteReader())
                {
                    while(reader.Read()){
                        string value = reader.GetString(0);
                        // 把 value push 進 colNames
                        colNames.Add(value);
                    }
                }
            }

            string sql2 = $@"SELECT COUNT(*)
                             FROM {tableName}";
            int rowCount;
            using(var command2 = new SqlCommand(sql2, _connection))
            {
                object res = command2.ExecuteScalar();
                rowCount = Convert.ToInt32(res);
            }

            return  new TableInfo(
                spiltTableName,
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
            if(!WhiteListValidation(tableName)){
                Console.WriteLine($"表 {tableName} 不存在");
                return new List<Dictionary<string, object>>();                
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
            if(!IsConnectionOpen())
            {
                Console.WriteLine("資料庫連線未開啟");
                return false;
            }
            if(!WhiteListValidation(tableName)){
                Console.WriteLine($"表 {tableName} 不存在");
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

    /// <summary>
    /// 檢查連線
    /// </summary>
    private bool IsConnectionOpen(){
        return _connection != null && _connection.State == System.Data.ConnectionState.Open;
    }

    /// <summary>
    /// 白名單驗證
    /// </summary>
    private bool WhiteListValidation(string tableName){
        List<string> VaildNames = GetTableNames();
        return VaildNames.Contains(tableName);
    }

    // ========== 第三組：資料庫管理方法 ==========
    // TODO: 實作以下三個方法
    public bool DatabaseExists(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen())
            {
                Console.WriteLine("連線未開啟");
                return false;
            } 
            if(string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("資料庫名稱不可為空");
                return false;
            }
            string sql = @"SELECT COUNT(*)
                            FROM master.sys.databases
                            WHERE name = @dbName";
            using(var command = new SqlCommand(sql, _connection))
            {
                // 添加參數，參數化查詢
                command.Parameters.AddWithValue("@dbName", databaseName);
                object res = command.ExecuteScalar(); // 若無，回傳 0
                int count = Convert.ToInt32(res);
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
        Console.WriteLine("不支援 MSSQL 建立資料庫");
        Console.WriteLine("此連接器用於讀取現有 MSSQL 資料庫");
        return false;
    }

    public bool DeleteDatabase(string databaseName)
    {
        Console.WriteLine("不支援 MSSQL 刪除資料庫");
        Console.WriteLine("此連接器用於讀取現有 MSSQL 資料庫");
        return false;
    }

    public bool UseDatabase(string databaseName)
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
            if(!DatabaseExists(databaseName))
            {
                Console.WriteLine($"資料庫 '{databaseName}' 不存在");
            }

            // 查詢目前連接之資料庫
            if(_connection != null && !databaseName.Equals(_connection.Database))
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
            Console.WriteLine($"資料庫切換錯誤：{ex.Message}");
            return false;
        }
    }
}
