using System.Collections.Generic;

namespace DBTransfer.Core.Models;

/// <summary>
/// 整體資料庫轉移結果
/// </summary>
public class TransferResult
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public int SuccessCount { get; set; }
    public int FailCount { get; set; }
    public List<TableTransferResult> TableResults { get; set; } = new();

    // 驗證結果
    public List<VerificationResult> VerificationResults { get; set; } = new();
    public int VerifyPassCount { get; set; }
    public int VerifyFailCount { get; set; }
}

/// <summary>
/// 單一資料表轉移結果
/// </summary>
public class TableTransferResult
{
    public string SourceTableName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public bool Success { get; set; }
    public int RowCount { get; set; }
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// 單一資料表驗證結果
/// </summary>
public class VerificationResult
{
    public string SourceTableName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public int SourceRowCount { get; set; }
    public int TargetRowCount { get; set; }
    public int SourceColumnCount { get; set; }
    public int TargetColumnCount { get; set; }
    public bool RowCountMatch => SourceRowCount == TargetRowCount;
    public bool ColumnCountMatch => SourceColumnCount == TargetColumnCount;
    public bool IsValid => RowCountMatch && ColumnCountMatch;
    public string? ErrorMessage { get; set; }
}
