using DBTransfer.Core.Models;

namespace DBTransfer.Tests;

/// <summary>
/// ColumnInfo 單元測試
/// 驗證模型建構、驗證邏輯、屬性設定
/// </summary>
public class ColumnInfoTests
{
    [Fact]
    public void Constructor_ValidParams_SetsAllProperties()
    {
        var col = new ColumnInfo(
            columnName: "Price",
            dataType: "DECIMAL",
            maxLength: null,
            precision: 18,
            scale: 2,
            isNullable: false,
            isIdentity: false,
            ordinalPosition: 3);

        Assert.Equal("Price", col.ColumnName);
        Assert.Equal("decimal", col.DataType);   // 會自動轉小寫
        Assert.Null(col.MaxLength);
        Assert.Equal(18, col.Precision);
        Assert.Equal(2, col.Scale);
        Assert.False(col.IsNullable);
        Assert.False(col.IsIdentity);
        Assert.Equal(3, col.OrdinalPosition);
    }

    [Fact]
    public void Constructor_DataType_IsLowercased()
    {
        var col = new ColumnInfo("Col", "VARCHAR", maxLength: 50);
        Assert.Equal("varchar", col.DataType);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_EmptyColumnName_ThrowsArgumentException(string? name)
    {
        Assert.Throws<ArgumentException>(() => new ColumnInfo(name!, "int"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_EmptyDataType_ThrowsArgumentException(string? dataType)
    {
        Assert.Throws<ArgumentException>(() => new ColumnInfo("Col", dataType!));
    }

    [Fact]
    public void Constructor_Defaults_NullableAndNoIdentity()
    {
        var col = new ColumnInfo("Col", "int");

        Assert.True(col.IsNullable);          // 預設 nullable
        Assert.False(col.IsIdentity);         // 預設非 identity
        Assert.Equal(0, col.OrdinalPosition); // 預設 0
    }

    [Fact]
    public void ToString_ReturnsReadableFormat()
    {
        var col = new ColumnInfo("Name", "nvarchar", maxLength: 100, isNullable: true);
        string str = col.ToString();

        Assert.Contains("Name", str);
        Assert.Contains("nvarchar", str);
        Assert.Contains("100", str);
    }
}
