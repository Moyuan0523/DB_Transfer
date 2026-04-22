using System;

namespace DBTransfer.Core.Models;

/// <summary>
/// 資料庫欄位詳細資訊（從來源資料庫讀取）
/// 包含資料類型、長度、精度等結構資訊
/// </summary>
public class ColumnInfo
{
    /// <summary>
    /// 欄位名稱
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// 資料類型（如 varchar, int, decimal）
    /// </summary>
    public string DataType { get; }

    /// <summary>
    /// 最大長度（字串/二進位類型適用，-1 表示 MAX）
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// 數值精度（decimal/numeric 適用）
    /// </summary>
    public int? Precision { get; }

    /// <summary>
    /// 數值小數位數（decimal/numeric 適用）
    /// </summary>
    public int? Scale { get; }

    /// <summary>
    /// 是否允許 NULL
    /// </summary>
    public bool IsNullable { get; }

    /// <summary>
    /// 是否為自動遞增欄位（MSSQL Identity / MariaDB AUTO_INCREMENT）
    /// </summary>
    public bool IsIdentity { get; }

    /// <summary>
    /// 欄位在表中的順序位置
    /// </summary>
    public int OrdinalPosition { get; }

    public ColumnInfo(
        string columnName,
        string dataType,
        int? maxLength = null,
        int? precision = null,
        int? scale = null,
        bool isNullable = true,
        bool isIdentity = false,
        int ordinalPosition = 0)
    {
        if (string.IsNullOrWhiteSpace(columnName))
            throw new ArgumentException("欄位名稱不能為空", nameof(columnName));
        if (string.IsNullOrWhiteSpace(dataType))
            throw new ArgumentException("資料類型不能為空", nameof(dataType));

        ColumnName = columnName;
        DataType = dataType.ToLower();
        MaxLength = maxLength;
        Precision = precision;
        Scale = scale;
        IsNullable = isNullable;
        IsIdentity = isIdentity;
        OrdinalPosition = ordinalPosition;
    }

    public override string ToString()
    {
        return $"{ColumnName} ({DataType}, MaxLen={MaxLength}, Nullable={IsNullable})";
    }
}
