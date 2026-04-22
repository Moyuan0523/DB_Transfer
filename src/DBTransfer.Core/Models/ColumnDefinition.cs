namespace DBTransfer.Core.Models;

/// <summary>
/// 建立資料表時的欄位定義（已映射為目標資料庫類型）
/// </summary>
public class ColumnDefinition
{
    /// <summary>
    /// 欄位名稱
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// 目標資料庫的類型定義（如 VARCHAR(50), DECIMAL(18,2)）
    /// </summary>
    public string TypeDefinition { get; }

    /// <summary>
    /// 是否允許 NULL
    /// </summary>
    public bool IsNullable { get; }

    /// <summary>
    /// 是否為自動遞增（MariaDB AUTO_INCREMENT）
    /// </summary>
    public bool IsAutoIncrement { get; }

    public ColumnDefinition(
        string columnName,
        string typeDefinition,
        bool isNullable,
        bool isAutoIncrement = false)
    {
        ColumnName = columnName;
        TypeDefinition = typeDefinition;
        IsNullable = isNullable;
        IsAutoIncrement = isAutoIncrement;
    }
}
