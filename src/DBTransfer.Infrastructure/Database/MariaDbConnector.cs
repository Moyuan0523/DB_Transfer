using System;
using System.Linq;
using System.Collections.Generic;
using DBTransfer.Core.Interfaces;
using DBTransfer.Core.Models;
using MySqlConnector;
using DBTransfer.Core.Logging;
using DBTransfer.Core.Utils;

namespace DBTransfer.Infrastructure.Database;

/// <summary>
/// MariaDB 資料庫連接器
/// </summary>
public class MariaDbConnector : IDatabaseConnector
{
    private MySqlConnection? _connection;
    private string _connectionString;
    private readonly ITransferLogger _logger;

    /// <summary>
    /// 初始化連接字串
    /// </summary>
    /// <param name="connectionString">連線資訊（server ip, port, database, user...），從 .env 檔讀取</param>
    /// <param name="logger">日誌記錄器</param>
    public MariaDbConnector(string connectionString, ITransferLogger logger)
    {
        _connectionString = connectionString;
        _logger = logger;
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
            _logger.Error($"連線錯誤：{ex.Message}");
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
                _logger.Info("資料庫連線已關閉");
            }
            return true;
        }
        catch(Exception ex)
        {
            _logger.Error($"斷開連線錯誤：{ex.Message}");
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
                _logger.Warn("資料庫連線未開啟");
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
            _logger.Error($"查詢資料表名稱失敗：{ex.Message}");
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
                _logger.Warn("資料表名稱不可為空");
                return null;
            }

            if(!IsConnectionOpen() || _connection == null)
            {
                _logger.Warn("連線尚未建立");
                return null;
            }

            // 白名單驗證
            if(!WhiteListValidation(tableName))
            {
                _logger.Warn($"表 {tableName} 不存在");
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
            _logger.Error($"讀取表資料失敗：{ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 獲取指定表的欄位詳細資訊（含資料類型）
    /// </summary>
    public List<ColumnInfo> GetColumnDetails(string tableName)
    {
        try
        {
            if(string.IsNullOrEmpty(tableName))
            {
                _logger.Warn("資料表名稱不可為空");
                return new List<ColumnInfo>();
            }
            if(!IsConnectionOpen() || _connection == null)
            {
                _logger.Warn("連線尚未建立");
                return new List<ColumnInfo>();
            }
            if(!WhiteListValidation(tableName))
            {
                _logger.Warn($"表 {tableName} 不存在");
                return new List<ColumnInfo>();
            }

            string sql = @"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    ORDINAL_POSITION,
                    EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName
                ORDER BY ORDINAL_POSITION";

            var columns = new List<ColumnInfo>();
            using(var command = new MySqlCommand(sql, _connection))
            {
                command.Parameters.AddWithValue("@tableName", tableName);

                using(var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        // CHARACTER_MAXIMUM_LENGTH 在 MariaDB 中為 bigint，需安全轉換
                        long? rawMaxLen = reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2));
                        int? maxLength = rawMaxLen switch
                        {
                            null => null,
                            > int.MaxValue => -1,
                            _ => (int)rawMaxLen.Value
                        };

                        var col = new ColumnInfo(
                            columnName: reader.GetString(0),
                            dataType: reader.GetString(1),
                            maxLength: maxLength,
                            precision: reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                            scale: reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4)),
                            isNullable: reader.GetString(5) == "YES",
                            isIdentity: !reader.IsDBNull(7) && reader.GetString(7).Contains("auto_increment"),
                            ordinalPosition: Convert.ToInt32(reader.GetValue(6))
                        );
                        columns.Add(col);
                    }
                }
            }

            return columns;
        }
        catch(Exception ex)
        {
            _logger.Error($"讀取欄位詳細資訊失敗：{ex.Message}");
            return new List<ColumnInfo>();
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
                _logger.Warn("資料表名稱不可為空");
                return new List<Dictionary<string, object>>();
            }

            if(!IsConnectionOpen())
            {
                _logger.Warn("資料庫連線未開啟");
                return new List<Dictionary<string, object>>();
            }

            // 白名單驗證
            if(!WhiteListValidation(tableName))
            {
                _logger.Warn($"表 {tableName} 不存在");
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
            _logger.Error($"讀取表資料失敗：{ex.Message}");
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
                _logger.Warn("表名稱或寫入之資料不可為空");
                return false;
            }

            if(!IsConnectionOpen() || _connection == null)
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }

            if(!WhiteListValidation(tableName))
            {
                _logger.Warn($"表 {tableName} 不存在");
                return false;
            }

            const int batchSize = 500;
            int totalInserted = 0;

            // 以第一筆資料的欄位為基準
            List<string> columns = data[0].Keys.ToList();
            string columnsPart = string.Join(", ", columns.Select(c => $"`{c}`"));

            using var transaction = _connection.BeginTransaction();
            try
            {
                for(int i = 0; i < data.Count; i += batchSize)
                {
                    var batch = data.Skip(i).Take(batchSize).ToList();

                    using var command = new MySqlCommand();
                    command.Connection = _connection;
                    command.Transaction = transaction;

                    var valueGroups = new List<string>();
                    int paramCounter = 0;

                    for(int j = 0; j < batch.Count; j++)
                    {
                        var row = batch[j];
                        var paramNames = new List<string>();

                        foreach(var col in columns)
                        {
                            string paramName = $"@p{paramCounter++}";
                            paramNames.Add(paramName);
                            object value = row.ContainsKey(col) ? row[col] : DBNull.Value;
                            command.Parameters.AddWithValue(paramName, ConvertValue(value));
                        }

                        valueGroups.Add($"({string.Join(", ", paramNames)})");
                    }

                    command.CommandText = $"INSERT INTO `{tableName}` ({columnsPart}) VALUES {string.Join(", ", valueGroups)}";
                    command.ExecuteNonQuery();
                    totalInserted += batch.Count;
                }

                transaction.Commit();
                _logger.Info($"  成功插入 {totalInserted} 筆資料（批次寫入，每批 {batchSize}）");
                return true;
            }
            catch
            {
                transaction.Rollback();
                throw; // 拋給外層 catch 處理
            }
        }
        catch(Exception ex)
        {
            _logger.Error($"資料寫入失敗（已回滾）：{ex.Message}");
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

    /// <summary>
    /// 將資料值轉換為 MariaDB 相容格式
    /// </summary>
    private static object ConvertValue(object value)
    {
        if(value == null || value == DBNull.Value) return DBNull.Value;
        if(value is Guid guid) return guid.ToString();  // uniqueidentifier → VARCHAR(36)
        return value;
    }

    // ========== 第三組：資料庫管理方法 ==========
    public bool DatabaseExists(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen())
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                _logger.Warn("資料庫名稱不可為空");
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
            _logger.Error($"資料庫存在查詢失敗：{ex.Message}");
            return false;
        }
    }
    public bool CreateTable(string tableName, List<ColumnDefinition> columns)
    {
        try
        {
            if(string.IsNullOrEmpty(tableName) || columns == null || columns.Count == 0)
            {
                _logger.Warn("表名稱或欄位定義不可為空");
                return false;
            }
            if(!IsConnectionOpen())
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }
            if(!TableNameConverter.IsValidMariaDBTableName(tableName))
            {
                _logger.Warn($"表名 '{tableName}' 不合法");
                return false;
            }

            // 組合 CREATE TABLE DDL
            var columnDefs = new List<string>();
            foreach(var col in columns)
            {
                string nullable = col.IsNullable ? "NULL" : "NOT NULL";
                string autoIncrement = col.IsAutoIncrement ? " AUTO_INCREMENT" : "";
                columnDefs.Add($"  `{col.ColumnName}` {col.TypeDefinition} {nullable}{autoIncrement}");
            }

            string ddl = $"CREATE TABLE IF NOT EXISTS `{tableName}` (\n"
                + string.Join(",\n", columnDefs)
                + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

            using(var command = new MySqlCommand(ddl, _connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.Info($"  ✅ 資料表 '{tableName}' 建立成功");
            return true;
        }
        catch(Exception ex)
        {
            _logger.Error($"建立資料表失敗：{ex.Message}");
            return false;
        }
    }

    public bool CreateDatabase(string databaseName) 
    {
        try
        {
            if(!IsConnectionOpen())
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                _logger.Warn("資料庫名稱不可為空");
                return false;
            }
            if(DatabaseExists(databaseName))
            {
                _logger.Info($"資料庫 '{databaseName}' 已存在");
                return true;
            }
            
            if(!TableNameConverter.IsValidMariaDBTableName(databaseName))
            {
                _logger.Warn($"資料庫 '{databaseName}' 名稱不合法，只允許英文字母、數字、底線");
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
                    _logger.Info($"資料庫 '{databaseName}' 建立成功");
                    return true;
                }
                else
                {
                    _logger.Error($"資料庫 '{databaseName}' 建立失敗");
                    return false;
                }
            }
            
        }
        catch(Exception ex)
        {
            _logger.Error($"資料庫建立失敗：{ex.Message}");
            return false;
        }
    }

    public bool DeleteDatabase(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen() || _connection == null)
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                _logger.Warn("資料庫名稱不可為空");
                return false;
            }
            if(!DatabaseExists(databaseName))
            {
                _logger.Warn($"資料庫 '{databaseName}' 不存在");
                return false;
            }
            if(!TableNameConverter.IsValidMariaDBTableName(databaseName))
            {
                _logger.Warn($"資料庫 '{databaseName}' 名稱不合法，只允許英文字母、數字、底線");
                return false;
            }

            string sql = $"DROP DATABASE `{databaseName}`";
            using(var command = new MySqlCommand(sql, _connection))
            {
                command.ExecuteNonQuery();
                if(!DatabaseExists(databaseName))
                {
                    _logger.Info($"資料庫 '{databaseName}' 刪除成功");
                    return true;
                }
                else
                {
                    _logger.Error($"資料庫 '{databaseName}' 刪除失敗");
                    return false;
                }
            }
        }
        catch(Exception ex)
        {
            _logger.Error($"刪除資料庫失敗：{ex.Message}");
            return false;
        }
    }

    public bool UseDatabase(string databaseName)
    {
        try
        {
            if(!IsConnectionOpen() || _connection == null)
            {
                _logger.Warn("資料庫連線未開啟");
                return false;
            }
            if(string.IsNullOrEmpty(databaseName))
            {
                _logger.Warn("資料庫名稱不可為空");
                return false;
            }
            if(!DatabaseExists(databaseName))
            {
                _logger.Warn($"資料庫 '{databaseName}' 不存在");
                return false;
            }

            // 查詢目前連接之資料庫
            if(_connection != null && !_connection.Database.Equals(databaseName))
            {
                _logger.Info($"目前連線資料庫為 '{_connection.Database}'，開始切換...");
            }
            else
            {
                _logger.Info($"'{databaseName}' 便為目前連接之資料庫");
                return true;
            }

            // 切換資料庫
            _connection.ChangeDatabase(databaseName);
            _logger.Info($"切換成功，目前資料庫為 '{_connection.Database}'");
            return true;
        }
        catch(Exception ex)
        {
            _logger.Error($"資料庫切換失敗：{ex.Message}");
            return false;
        }
    }
}
