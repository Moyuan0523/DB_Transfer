using System;
using System.Collections.Generic;
using DBTransfer.Core.Logging;
using DBTransfer.Core.Models;

namespace DBTransfer.Core.Utils;

/// <summary>
/// MSSQL → MariaDB 資料類型映射器
/// 負責將 MSSQL 的欄位類型轉換為對應的 MariaDB 類型
/// </summary>
public static class TypeMapper
{
    private static readonly Dictionary<string, string> BaseTypeMapping = new(StringComparer.OrdinalIgnoreCase)
    {
        // 整數類型
        { "int", "INT" },
        { "bigint", "BIGINT" },
        { "smallint", "SMALLINT" },
        { "tinyint", "TINYINT" },
        { "bit", "TINYINT(1)" },

        // 精確數值
        { "decimal", "DECIMAL" },
        { "numeric", "DECIMAL" },
        { "money", "DECIMAL(19,4)" },
        { "smallmoney", "DECIMAL(10,4)" },

        // 浮點數
        { "float", "DOUBLE" },
        { "real", "FLOAT" },

        // 日期時間
        { "datetime", "DATETIME" },
        { "datetime2", "DATETIME(6)" },
        { "smalldatetime", "DATETIME" },
        { "date", "DATE" },
        { "time", "TIME" },
        { "datetimeoffset", "DATETIME(6)" },

        // 字串
        { "varchar", "VARCHAR" },
        { "nvarchar", "VARCHAR" },
        { "char", "CHAR" },
        { "nchar", "CHAR" },
        { "text", "TEXT" },
        { "ntext", "LONGTEXT" },

        // 二進位
        { "binary", "BINARY" },
        { "varbinary", "VARBINARY" },
        { "image", "LONGBLOB" },

        // 特殊類型
        { "uniqueidentifier", "VARCHAR(36)" },
        { "xml", "LONGTEXT" },
        { "sql_variant", "TEXT" },
        { "hierarchyid", "VARCHAR(892)" },
        { "geography", "LONGTEXT" },
        { "geometry", "LONGTEXT" },
        { "timestamp", "BINARY(8)" },
        { "rowversion", "BINARY(8)" },
    };

    /// <summary>
    /// 將 MSSQL 欄位類型映射為 MariaDB 類型定義字串
    /// </summary>
    /// <param name="column">來源欄位資訊</param>
    /// <returns>MariaDB 類型定義（如 VARCHAR(50), DECIMAL(18,2)）</returns>
    public static string MapToMariaDB(ColumnInfo column, ITransferLogger? logger = null)
    {
        string baseType = column.DataType.ToLower().Trim();

        if (!BaseTypeMapping.TryGetValue(baseType, out string? mariaType))
        {
            logger?.Warn($"  ⚠️ 未知類型 '{column.DataType}'，使用 TEXT 作為預設");
            return "TEXT";
        }

        return baseType switch
        {
            "varchar" or "nvarchar" => MapVarchar(column),
            "char" or "nchar" => MapChar(column),
            "decimal" or "numeric" => MapDecimal(column),
            "binary" => MapBinary(column),
            "varbinary" => MapVarbinary(column),
            _ => mariaType
        };
    }

    /// <summary>
    /// 將來源 ColumnInfo 轉換為目標 ColumnDefinition
    /// </summary>
    /// <param name="source">來源欄位資訊</param>
    /// <param name="preserveValues">
    /// true（預設）：轉移資料時使用，不設定 AUTO_INCREMENT 以便插入原始值
    /// false：建立空表時使用，保留 AUTO_INCREMENT 設定
    /// </param>
    public static ColumnDefinition ToColumnDefinition(ColumnInfo source, bool preserveValues = true, ITransferLogger? logger = null)
    {
        string mariaDbType = MapToMariaDB(source, logger);

        return new ColumnDefinition(
            source.ColumnName,
            mariaDbType,
            source.IsNullable,
            isAutoIncrement: source.IsIdentity && !preserveValues
        );
    }

    /// <summary>
    /// varchar / nvarchar 映射
    /// nvarchar(max) 的 MaxLength = -1 → LONGTEXT
    /// </summary>
    private static string MapVarchar(ColumnInfo column)
    {
        if (column.MaxLength == -1 || column.MaxLength > 16383)
            return "LONGTEXT";
        if (column.MaxLength != null && column.MaxLength > 0)
            return $"VARCHAR({column.MaxLength})";
        return "VARCHAR(255)";
    }

    /// <summary>
    /// char / nchar 映射
    /// </summary>
    private static string MapChar(ColumnInfo column)
    {
        if (column.MaxLength != null && column.MaxLength > 0 && column.MaxLength <= 255)
            return $"CHAR({column.MaxLength})";
        if (column.MaxLength != null && column.MaxLength > 255)
            return $"VARCHAR({column.MaxLength})";
        return "CHAR(1)";
    }

    /// <summary>
    /// decimal / numeric 映射
    /// </summary>
    private static string MapDecimal(ColumnInfo column)
    {
        int precision = column.Precision ?? 18;
        int scale = column.Scale ?? 0;
        return $"DECIMAL({precision},{scale})";
    }

    /// <summary>
    /// binary 映射
    /// </summary>
    private static string MapBinary(ColumnInfo column)
    {
        if (column.MaxLength != null && column.MaxLength > 0)
            return $"BINARY({column.MaxLength})";
        return "BINARY(1)";
    }

    /// <summary>
    /// varbinary 映射
    /// varbinary(max) 的 MaxLength = -1 → LONGBLOB
    /// </summary>
    private static string MapVarbinary(ColumnInfo column)
    {
        if (column.MaxLength == -1 || column.MaxLength > 65535)
            return "LONGBLOB";
        if (column.MaxLength != null && column.MaxLength > 0)
            return $"VARBINARY({column.MaxLength})";
        return "VARBINARY(255)";
    }
}
