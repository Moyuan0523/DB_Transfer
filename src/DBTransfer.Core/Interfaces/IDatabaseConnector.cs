using System.Collections.Generic;
using DBTransfer.Core.Models;  // ← 加上這行，引用 TableInfo

namespace DBTransfer.Core.Interfaces;

/// <summary>
/// 数据库连接器接口
/// 定义所有数据库连接器必须实现的方法
/// </summary>
public interface IDatabaseConnector
{
    /// <summary>
    /// 連接資料庫
    /// </summary>
    bool Connect(); //成功連線 true, 連線失敗 false;
    
    /// <summary>
    /// 斷開連線
    /// </summary>
    bool Disconnect(); //成功斷開 true, 斷開失敗 false;
    
    /// <summary>
    /// 測試連線
    /// </summary>
    bool TestConnection(); //測試成功 true, 測試失敗 false;

    /// <summary>
    /// 當前連接到哪裡
    /// </summary>
    string GetConnectionString();
        
    /// <summary>
    /// 獲取所有表的名稱
    /// </summary>
    List<string> GetTableNames(); 

    /// <summary>
    /// 獲取指定表的結構資訊
    /// </summary>
    /// <param name="tableName">表名稱</param>
    /// <returns>如果表存在則回傳 TableInfo，否則回傳 null</returns>
    TableInfo? GetTableStructure(string tableName);

    /// <summary>
    /// 讀取數據
    /// </summary>
    List<Dictionary<string, object>> GetTableData(string tableName);

    /// <summary>
    /// 寫入數據
    /// </summary>
    bool InsertData(string TableName, List<Dictionary<string, object>> data); // 寫入成功 true, 寫入失敗 false

    // ========== 第三組：資料庫管理方法 ==========
    
    /// <summary>
    /// 檢查資料庫是否存在
    /// </summary>
    /// <param name="databaseName">資料庫名稱</param>
    /// <returns>存在返回 true，不存在返回 false</returns>
    bool DatabaseExists(string databaseName);

    /// <summary>
    /// 創建資料庫
    /// </summary>
    /// <param name="databaseName">資料庫名稱</param>
    /// <returns>創建成功返回 true，失敗返回 false</returns>
    bool CreateDatabase(string databaseName);

    /// <summary>
    /// 切換到指定資料庫
    /// </summary>
    /// <param name="databaseName">資料庫名稱</param>
    /// <returns>切換成功返回 true，失敗返回 false</returns>
    bool UseDatabase(string databaseName);

    /// <summary>
    /// 刪除指定資料庫
    /// </summary>
    /// <param name="databaseName">資料庫名稱</param>
    /// <returns>切換成功返回 true，失敗返回 false</returns>
    bool DeleteDatabase(string databaseName);
}