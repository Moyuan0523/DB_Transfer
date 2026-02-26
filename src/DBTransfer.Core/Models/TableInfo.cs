using System;
using System.Collections.Generic;

namespace DBTransfer.Core.Models;

/// <summary>
/// 表格結構資訊（唯讀）
/// </summary>

// 『 會被 connector 確認實際有資料表後 new 出來 』

public class TableInfo
{
    /// <summary>
    /// 建構 TableInfo 物件
    /// </summary>
    /// <param name="tableName">表名稱（必填）</param>
    /// <param name="schema">Schema 名稱（選填，預設為空字串）</param>
    /// <param name="columnNames">欄位名稱清單（選填，預設為空清單）</param>
    /// <param name="tableRowNum">資料筆數（選填，預設為 0）</param>
    public TableInfo(
        string tableName, 
        string? schema = null, 
        List<string>? columnNames = null, 
        int tableRowNum = 0)
    {
        // 驗證：只有表名不能為空
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("表名稱不能為空", nameof(tableName));

        TableName = tableName;
        Schema = schema ?? string.Empty;  // 如果為 null，使用空字串
        ColumnNames = columnNames ?? new List<string>();  // 如果為 null，使用空清單
        TableRowNum = tableRowNum;
    }

    /// <summary>
    /// 表名
    /// </summary>
    public string TableName { get; } 

    /// <summary>
    /// Schema（如 dbo, Person）
    /// </summary>     
    public string Schema { get; }    

    /// <summary>
    /// 欄位名稱清單
    /// </summary>     
    public List<string> ColumnNames { get; }

    /// <summary>
    /// 資料筆數
    /// </summary>      
    public int TableRowNum { get; }
}