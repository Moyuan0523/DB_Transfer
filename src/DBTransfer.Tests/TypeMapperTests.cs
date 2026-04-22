using DBTransfer.Core.Models;
using DBTransfer.Core.Utils;

namespace DBTransfer.Tests;

/// <summary>
/// TypeMapper 單元測試
/// 驗證 30+ 種 MSSQL → MariaDB 資料類型映射的正確性
/// </summary>
public class TypeMapperTests
{
    private static ColumnInfo MakeColumn(
        string dataType,
        int? maxLength = null,
        int? precision = null,
        int? scale = null,
        bool isIdentity = false)
    {
        return new ColumnInfo("TestCol", dataType,
            maxLength: maxLength, precision: precision, scale: scale,
            isIdentity: isIdentity);
    }

    // ========== 整數類型 ==========

    [Theory]
    [InlineData("int", "INT")]
    [InlineData("bigint", "BIGINT")]
    [InlineData("smallint", "SMALLINT")]
    [InlineData("tinyint", "TINYINT")]
    [InlineData("bit", "TINYINT(1)")]
    public void MapToMariaDB_IntegerTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 字串類型 ==========

    [Theory]
    [InlineData("varchar", 50, "VARCHAR(50)")]
    [InlineData("varchar", 255, "VARCHAR(255)")]
    [InlineData("nvarchar", 100, "VARCHAR(100)")]
    [InlineData("varchar", -1, "LONGTEXT")]         // varchar(max)
    [InlineData("nvarchar", -1, "LONGTEXT")]         // nvarchar(max)
    [InlineData("nvarchar", 20000, "LONGTEXT")]      // 超過 16383 上限
    public void MapToMariaDB_VarcharTypes(string mssqlType, int maxLength, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType, maxLength: maxLength)));
    }

    [Fact]
    public void MapToMariaDB_VarcharNoLength_DefaultsTo255()
    {
        Assert.Equal("VARCHAR(255)", TypeMapper.MapToMariaDB(MakeColumn("varchar")));
    }

    [Theory]
    [InlineData("char", 10, "CHAR(10)")]
    [InlineData("nchar", 1, "CHAR(1)")]
    [InlineData("char", 300, "VARCHAR(300)")]        // char > 255 → VARCHAR
    public void MapToMariaDB_CharTypes(string mssqlType, int maxLength, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType, maxLength: maxLength)));
    }

    [Theory]
    [InlineData("text", "TEXT")]
    [InlineData("ntext", "LONGTEXT")]
    public void MapToMariaDB_TextTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 數值類型 ==========

    [Theory]
    [InlineData(18, 2, "DECIMAL(18,2)")]
    [InlineData(10, 4, "DECIMAL(10,4)")]
    [InlineData(5, 0, "DECIMAL(5,0)")]
    public void MapToMariaDB_DecimalTypes(int precision, int scale, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn("decimal", precision: precision, scale: scale)));
    }

    [Fact]
    public void MapToMariaDB_DecimalNoParams_DefaultsTo18_0()
    {
        Assert.Equal("DECIMAL(18,0)", TypeMapper.MapToMariaDB(MakeColumn("decimal")));
    }

    [Theory]
    [InlineData("money", "DECIMAL(19,4)")]
    [InlineData("smallmoney", "DECIMAL(10,4)")]
    [InlineData("float", "DOUBLE")]
    [InlineData("real", "FLOAT")]
    public void MapToMariaDB_NumericTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 日期時間 ==========

    [Theory]
    [InlineData("datetime", "DATETIME")]
    [InlineData("datetime2", "DATETIME(6)")]
    [InlineData("smalldatetime", "DATETIME")]
    [InlineData("date", "DATE")]
    [InlineData("time", "TIME")]
    [InlineData("datetimeoffset", "DATETIME(6)")]
    public void MapToMariaDB_DateTimeTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 二進位 ==========

    [Theory]
    [InlineData("binary", 8, "BINARY(8)")]
    [InlineData("varbinary", 100, "VARBINARY(100)")]
    [InlineData("varbinary", -1, "LONGBLOB")]        // varbinary(max)
    [InlineData("varbinary", 70000, "LONGBLOB")]     // 超過 65535 上限
    public void MapToMariaDB_BinaryTypes(string mssqlType, int maxLength, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType, maxLength: maxLength)));
    }

    [Theory]
    [InlineData("image", "LONGBLOB")]
    [InlineData("timestamp", "BINARY(8)")]
    [InlineData("rowversion", "BINARY(8)")]
    public void MapToMariaDB_SpecialBinaryTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 特殊類型 ==========

    [Theory]
    [InlineData("uniqueidentifier", "VARCHAR(36)")]
    [InlineData("xml", "LONGTEXT")]
    [InlineData("hierarchyid", "VARCHAR(892)")]
    [InlineData("geography", "LONGTEXT")]
    [InlineData("geometry", "LONGTEXT")]
    public void MapToMariaDB_SpecialTypes(string mssqlType, string expected)
    {
        Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
    }

    // ========== 未知類型 ==========

    [Fact]
    public void MapToMariaDB_UnknownType_ReturnsTEXT()
    {
        Assert.Equal("TEXT", TypeMapper.MapToMariaDB(MakeColumn("customtype")));
    }

    // ========== 大小寫不敏感 ==========

    [Theory]
    [InlineData("INT", "INT")]
    [InlineData("Int", "INT")]
    [InlineData("VARCHAR", 50, "VARCHAR(50)")]
    public void MapToMariaDB_CaseInsensitive(string mssqlType, object arg2 = null!, string expected = null!)
    {
        // 處理有 maxLength 和沒有的情況
        if (expected == null)
        {
            expected = (string)arg2;
            Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType)));
        }
        else
        {
            Assert.Equal(expected, TypeMapper.MapToMariaDB(MakeColumn(mssqlType, maxLength: (int)arg2)));
        }
    }

    // ========== ToColumnDefinition ==========

    [Fact]
    public void ToColumnDefinition_PreserveValues_NoAutoIncrement()
    {
        var col = MakeColumn("int", isIdentity: true);
        var result = TypeMapper.ToColumnDefinition(col, preserveValues: true);

        Assert.Equal("INT", result.TypeDefinition);
        Assert.False(result.IsAutoIncrement);
    }

    [Fact]
    public void ToColumnDefinition_NotPreserveValues_HasAutoIncrement()
    {
        var col = MakeColumn("int", isIdentity: true);
        var result = TypeMapper.ToColumnDefinition(col, preserveValues: false);

        Assert.Equal("INT", result.TypeDefinition);
        Assert.True(result.IsAutoIncrement);
    }

    [Fact]
    public void ToColumnDefinition_PreservesNullable()
    {
        var col = new ColumnInfo("NullableCol", "varchar", maxLength: 50, isNullable: true);
        var result = TypeMapper.ToColumnDefinition(col);
        Assert.True(result.IsNullable);

        var col2 = new ColumnInfo("NotNullCol", "int", isNullable: false);
        var result2 = TypeMapper.ToColumnDefinition(col2);
        Assert.False(result2.IsNullable);
    }
}
