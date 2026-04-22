using DBTransfer.Core.Models;

namespace DBTransfer.Tests;

/// <summary>
/// TransferResult / VerificationResult 單元測試
/// 驗證結果模型的計算屬性與初始狀態
/// </summary>
public class TransferResultTests
{
    // ========== VerificationResult 計算屬性 ==========

    [Fact]
    public void VerificationResult_EqualCounts_IsValid()
    {
        var v = new VerificationResult
        {
            SourceRowCount = 100,
            TargetRowCount = 100,
            SourceColumnCount = 5,
            TargetColumnCount = 5
        };

        Assert.True(v.RowCountMatch);
        Assert.True(v.ColumnCountMatch);
        Assert.True(v.IsValid);
    }

    [Fact]
    public void VerificationResult_RowMismatch_NotValid()
    {
        var v = new VerificationResult
        {
            SourceRowCount = 100,
            TargetRowCount = 99,
            SourceColumnCount = 5,
            TargetColumnCount = 5
        };

        Assert.False(v.RowCountMatch);
        Assert.True(v.ColumnCountMatch);
        Assert.False(v.IsValid);
    }

    [Fact]
    public void VerificationResult_ColumnMismatch_NotValid()
    {
        var v = new VerificationResult
        {
            SourceRowCount = 100,
            TargetRowCount = 100,
            SourceColumnCount = 5,
            TargetColumnCount = 4
        };

        Assert.True(v.RowCountMatch);
        Assert.False(v.ColumnCountMatch);
        Assert.False(v.IsValid);
    }

    [Fact]
    public void VerificationResult_BothZero_IsValid()
    {
        var v = new VerificationResult
        {
            SourceRowCount = 0,
            TargetRowCount = 0,
            SourceColumnCount = 0,
            TargetColumnCount = 0
        };

        Assert.True(v.IsValid);
    }

    // ========== TransferResult 初始狀態 ==========

    [Fact]
    public void TransferResult_DefaultState()
    {
        var r = new TransferResult();

        Assert.False(r.Success);
        Assert.Null(r.ErrorMessage);
        Assert.Equal(0, r.SuccessCount);
        Assert.Equal(0, r.FailCount);
        Assert.Empty(r.TableResults);
        Assert.Empty(r.VerificationResults);
        Assert.Equal(0, r.VerifyPassCount);
        Assert.Equal(0, r.VerifyFailCount);
    }

    // ========== TableTransferResult ==========

    [Fact]
    public void TableTransferResult_DefaultState()
    {
        var tr = new TableTransferResult();

        Assert.Equal(string.Empty, tr.SourceTableName);
        Assert.Equal(string.Empty, tr.TargetTableName);
        Assert.False(tr.Success);
        Assert.Equal(0, tr.RowCount);
        Assert.Null(tr.ErrorMessage);
    }
}
