using DBTransfer.Core.Utils;

namespace DBTransfer.Tests;

/// <summary>
/// TableNameConverter 單元測試
/// 驗證 MSSQL ↔ MariaDB 表名轉換邏輯
/// </summary>
public class TableNameConverterTests
{
    // ========== ConvertToMariaDB ==========

    [Theory]
    [InlineData("Sales.Currency", "Sales_Currency")]
    [InlineData("Person.Address", "Person_Address")]
    [InlineData("HumanResources.Employee", "HumanResources_Employee")]
    public void ConvertToMariaDB_WithSchema_ReplaceDotWithUnderscore(string input, string expected)
    {
        Assert.Equal(expected, TableNameConverter.ConvertToMariaDB(input));
    }

    [Theory]
    [InlineData("[Sales].[Currency]", "Sales_Currency")]
    [InlineData("[dbo].[Users]", "dbo_Users")]
    public void ConvertToMariaDB_WithBrackets_RemovesBracketsAndConverts(string input, string expected)
    {
        Assert.Equal(expected, TableNameConverter.ConvertToMariaDB(input));
    }

    [Fact]
    public void ConvertToMariaDB_NoSchema_ReturnsAsIs()
    {
        Assert.Equal("Currency", TableNameConverter.ConvertToMariaDB("Currency"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ConvertToMariaDB_EmptyOrNull_ThrowsArgumentException(string? input)
    {
        Assert.Throws<ArgumentException>(() => TableNameConverter.ConvertToMariaDB(input!));
    }

    // ========== ConvertToMSSQL ==========

    [Theory]
    [InlineData("Sales_Currency", "Sales.Currency")]
    [InlineData("Person_Address", "Person.Address")]
    public void ConvertToMSSQL_ReplacesUnderscoreWithDot(string input, string expected)
    {
        Assert.Equal(expected, TableNameConverter.ConvertToMSSQL(input));
    }

    // ========== IsValidMariaDBTableName ==========

    [Theory]
    [InlineData("Sales_Currency", true)]
    [InlineData("Users", true)]
    [InlineData("table123", true)]
    [InlineData("Sales.Currency", false)]   // 含點號
    [InlineData("my table", false)]          // 含空格
    [InlineData("table-name", false)]        // 含連字號
    [InlineData("", false)]
    public void IsValidMariaDBTableName_ValidatesCorrectly(string input, bool expected)
    {
        Assert.Equal(expected, TableNameConverter.IsValidMariaDBTableName(input));
    }

    // ========== ExtractSchema ==========

    [Theory]
    [InlineData("Sales.Currency", "Sales")]
    [InlineData("dbo.Users", "dbo")]
    [InlineData("[Sales].[Currency]", "Sales")]
    public void ExtractSchema_WithSchema_ReturnsSchema(string input, string expected)
    {
        Assert.Equal(expected, TableNameConverter.ExtractSchema(input));
    }

    [Fact]
    public void ExtractSchema_NoSchema_ReturnsNull()
    {
        Assert.Null(TableNameConverter.ExtractSchema("Currency"));
    }

    // ========== ExtractTableName ==========

    [Theory]
    [InlineData("Sales.Currency", "Currency")]
    [InlineData("[Sales].[Currency]", "Currency")]
    [InlineData("Currency", "Currency")]
    public void ExtractTableName_ReturnsTableNameOnly(string input, string expected)
    {
        Assert.Equal(expected, TableNameConverter.ExtractTableName(input));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ExtractTableName_EmptyOrNull_ThrowsArgumentException(string? input)
    {
        Assert.Throws<ArgumentException>(() => TableNameConverter.ExtractTableName(input!));
    }
}
