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
}