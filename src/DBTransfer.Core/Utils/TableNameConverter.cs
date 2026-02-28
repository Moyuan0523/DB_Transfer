using System;

namespace DBTransfer.Core.Utils;

/// <summary>
/// 表名轉換器
/// 用於 MSSQL 與 MariaDB 之間的表名格式轉換
/// </summary>
public static class TableNameConverter
{
    /// <summary>
    /// 將 MSSQL 表名轉換為 MariaDB 表名
    /// 範例：Sales.Currency → Sales_Currency
    ///      [Sales].[Currency] → Sales_Currency
    /// </summary>
    /// <param name="mssqlTableName">MSSQL 表名（可包含 Schema）</param>
    /// <returns>MariaDB 表名（前綴命名法）</returns>
    public static string ConvertToMariaDB(string mssqlTableName)
    {
        if (string.IsNullOrWhiteSpace(mssqlTableName))
        {
            throw new ArgumentException("表名不可為空", nameof(mssqlTableName));
        }

        // 1. 移除方括號
        string cleaned = RemoveBrackets(mssqlTableName);

        // 2. 將點號替換為底線
        string converted = cleaned.Replace('.', '_');

        // 3. 驗證結果是否合法
        if (!IsValidMariaDBTableName(converted))
        {
            throw new ArgumentException($"轉換後的表名 '{converted}' 不合法", nameof(mssqlTableName));
        }

        return converted;
    }

    /// <summary>
    /// 將 MariaDB 表名轉換回 MSSQL 表名（反向轉換）
    /// 範例：Sales_Currency → Sales.Currency
    /// 注意：此方法無法完美還原（因為資訊有遺失）
    /// </summary>
    /// <param name="mariadbTableName">MariaDB 表名</param>
    /// <returns>MSSQL 表名</returns>
    public static string ConvertToMSSQL(string mariadbTableName)
    {
        if (string.IsNullOrWhiteSpace(mariadbTableName))
        {
            throw new ArgumentException("表名不可為空", nameof(mariadbTableName));
        }

        // 簡單替換：底線 → 點號
        // 注意：這是簡化版本，可能不完美
        string converted = mariadbTableName.Replace('_', '.');

        return converted;
    }

    /// <summary>
    /// 移除 MSSQL 方括號
    /// 範例：[Sales].[Currency] → Sales.Currency
    /// </summary>
    /// <param name="tableName">可能包含方括號的表名</param>
    /// <returns>移除方括號後的表名</returns>
    private static string RemoveBrackets(string tableName)
    {
        // 移除所有的 [ 和 ]
        return tableName.Replace("[", "").Replace("]", "");
    }

    /// <summary>
    /// 驗證 MariaDB 表名是否合法
    /// 只允許：英文字母、數字、底線
    /// </summary>
    /// <param name="tableName">要驗證的表名</param>
    /// <returns>合法返回 true，否則返回 false</returns>
    public static bool IsValidMariaDBTableName(string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            return false;
        }

        // 檢查每個字元
        foreach (char c in tableName)
        {
            // 只允許：字母、數字、底線
            if (!char.IsLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 從完整表名中提取 Schema 名稱
    /// 範例：Sales.Currency → Sales
    /// </summary>
    /// <param name="mssqlTableName">MSSQL 表名</param>
    /// <returns>Schema 名稱，如果沒有則返回 null</returns>
    public static string? ExtractSchema(string mssqlTableName)
    {
        if (string.IsNullOrWhiteSpace(mssqlTableName))
        {
            return null;
        }

        // 移除方括號
        string cleaned = RemoveBrackets(mssqlTableName);

        // 尋找最後一個點號
        int lastDotIndex = cleaned.LastIndexOf('.');
        
        if (lastDotIndex > 0)
        {
            // 返回點號前的部分
            return cleaned.Substring(0, lastDotIndex);
        }

        return null;
    }

    /// <summary>
    /// 從完整表名中提取表格名稱（不含 Schema）
    /// 範例：Sales.Currency → Currency
    /// </summary>
    /// <param name="mssqlTableName">MSSQL 表名</param>
    /// <returns>表格名稱</returns>
    public static string ExtractTableName(string mssqlTableName)
    {
        if (string.IsNullOrWhiteSpace(mssqlTableName))
        {
            throw new ArgumentException("表名不可為空", nameof(mssqlTableName));
        }

        // 移除方括號
        string cleaned = RemoveBrackets(mssqlTableName);

        // 尋找最後一個點號
        int lastDotIndex = cleaned.LastIndexOf('.');
        
        if (lastDotIndex >= 0 && lastDotIndex < cleaned.Length - 1)
        {
            // 返回點號後的部分
            return cleaned.Substring(lastDotIndex + 1);
        }

        // 如果沒有點號，返回整個字串
        return cleaned;
    }
}
