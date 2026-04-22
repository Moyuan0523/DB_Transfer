using System;
using System.Collections.Generic;
using System.Linq;
using DBTransfer.Core.Interfaces;
using DBTransfer.Core.Models;
using DBTransfer.Core.Logging;
using DBTransfer.Core.Utils;

namespace DBTransfer.Core.Services;

/// <summary>
/// 資料庫轉移服務
/// 負責協調從來源資料庫（MSSQL）到目標資料庫（MariaDB）的完整轉移流程
/// </summary>
public class DatabaseTransferService
{
    private readonly IDatabaseConnector _source;
    private readonly IDatabaseConnector _target;
    private readonly ITransferLogger _logger;

    /// <summary>
    /// 建立轉移服務
    /// </summary>
    /// <param name="source">來源資料庫連接器（MSSQL）</param>
    /// <param name="target">目標資料庫連接器（MariaDB）</param>
    /// <param name="logger">日誌記錄器</param>
    public DatabaseTransferService(IDatabaseConnector source, IDatabaseConnector target, ITransferLogger logger)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _target = target ?? throw new ArgumentNullException(nameof(target));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// 轉移所有資料表
    /// </summary>
    /// <param name="targetDatabaseName">目標資料庫名稱</param>
    /// <returns>轉移結果</returns>
    public TransferResult TransferAll(string targetDatabaseName)
    {
        var result = new TransferResult();
        string separator = new string('=', 60);

        _logger.Info($"\n{separator}");
        _logger.Info($"  開始資料庫轉移：目標資料庫 '{targetDatabaseName}'");
        _logger.Info($"{separator}\n");

        // Step 1: 準備目標資料庫
        if (!PrepareTargetDatabase(targetDatabaseName))
        {
            result.Success = false;
            result.ErrorMessage = "目標資料庫準備失敗";
            return result;
        }

        // Step 2: 取得來源資料表清單
        List<string> sourceTableNames = _source.GetTableNames();
        if (sourceTableNames.Count == 0)
        {
            _logger.Warn("⚠️ 來源資料庫沒有資料表");
            result.Success = true;
            return result;
        }

        _logger.Info($"📋 找到 {sourceTableNames.Count} 個來源資料表\n");

        // Step 3: 逐一轉移每個資料表
        foreach (string tableName in sourceTableNames)
        {
            var tableResult = TransferTable(tableName);
            result.TableResults.Add(tableResult);

            if (tableResult.Success)
                result.SuccessCount++;
            else
                result.FailCount++;
        }

        // Step 4: 列印轉移摘要
        PrintSummary(result);

        // Step 5: 驗證轉移結果（比對行數與欄位數）
        VerifyAll(result);

        result.Success = result.FailCount == 0 && result.VerifyFailCount == 0;
        return result;
    }

    /// <summary>
    /// 轉移單一資料表
    /// 流程：讀取結構 → 類型映射 → 建表 → 搬資料
    /// </summary>
    /// <param name="sourceTableName">來源表名稱（如 Sales.Currency）</param>
    /// <returns>單一資料表轉移結果</returns>
    public TableTransferResult TransferTable(string sourceTableName)
    {
        var result = new TableTransferResult { SourceTableName = sourceTableName };
        string targetTableName = TableNameConverter.ConvertToMariaDB(sourceTableName);
        result.TargetTableName = targetTableName;

        string line = new string('─', 50);
        _logger.Info(line);
        _logger.Info($"📋 轉移：{sourceTableName} → {targetTableName}");
        _logger.Info(line);

        try
        {
            // Step 1: 讀取來源表結構（含欄位類型）
            _logger.Info("  🔍 讀取來源表結構...");
            List<ColumnInfo> sourceColumns = _source.GetColumnDetails(sourceTableName);
            if (sourceColumns.Count == 0)
            {
                _logger.Warn("  ⚠️ 無法讀取欄位資訊，跳過此表");
                result.ErrorMessage = "無法讀取欄位資訊";
                return result;
            }
            _logger.Info($"  ✅ 共 {sourceColumns.Count} 個欄位");

            // Step 2: 映射資料類型（MSSQL → MariaDB）
            _logger.Info("  🔄 映射資料類型...");
            List<ColumnDefinition> targetColumns = sourceColumns
                .Select(col => TypeMapper.ToColumnDefinition(col, logger: _logger))
                .ToList();

            // Step 3: 在目標資料庫建立表
            _logger.Info($"  🏗️ 建立目標表 '{targetTableName}'...");
            if (!_target.CreateTable(targetTableName, targetColumns))
            {
                _logger.Error("  ❌ 建立目標表失敗");
                result.ErrorMessage = "建立目標表失敗";
                return result;
            }

            // Step 4: 讀取來源資料
            _logger.Info("  📖 讀取來源資料...");
            List<Dictionary<string, object>> data = _source.GetTableData(sourceTableName);
            result.RowCount = data.Count;
            _logger.Info($"  ✅ 共 {data.Count} 筆資料");

            // Step 5: 寫入目標表
            if (data.Count > 0)
            {
                _logger.Info("  📝 寫入目標表...");
                if (!_target.InsertData(targetTableName, data))
                {
                    _logger.Error("  ❌ 資料寫入失敗");
                    result.ErrorMessage = "資料寫入失敗";
                    return result;
                }
            }

            result.Success = true;
            _logger.Info($"  ✅ 完成（{data.Count} 筆）\n");
        }
        catch (Exception ex)
        {
            _logger.Error($"  ❌ 轉移失敗：{ex.Message}\n");
            result.ErrorMessage = ex.Message;
        }

        return result;
    }

    /// <summary>
    /// 準備目標資料庫（不存在則建立，然後切換）
    /// </summary>
    private bool PrepareTargetDatabase(string targetDatabaseName)
    {
        if (!_target.DatabaseExists(targetDatabaseName))
        {
            _logger.Info($"📦 建立目標資料庫 '{targetDatabaseName}'...");
            if (!_target.CreateDatabase(targetDatabaseName))
            {
                _logger.Error("❌ 無法建立目標資料庫");
                return false;
            }
            _logger.Info($"✅ 資料庫 '{targetDatabaseName}' 建立完成");
        }
        else
        {
            _logger.Info($"✅ 目標資料庫 '{targetDatabaseName}' 已存在");
        }

        _logger.Info($"🔄 切換到目標資料庫 '{targetDatabaseName}'...");
        if (!_target.UseDatabase(targetDatabaseName))
        {
            _logger.Error("❌ 無法切換到目標資料庫");
            return false;
        }

        return true;
    }

    /// <summary>
    /// 列印轉移結果摘要
    /// </summary>
    private void PrintSummary(TransferResult result)
    {
        string separator = new string('=', 60);
        int totalRows = result.TableResults
            .Where(t => t.Success)
            .Sum(t => t.RowCount);

        _logger.Info($"\n{separator}");
        _logger.Info("  📊 轉移結果摘要");
        _logger.Info(separator);
        _logger.Info($"  ✅ 成功：{result.SuccessCount} 個資料表");
        _logger.Error($"  ❌ 失敗：{result.FailCount} 個資料表");
        _logger.Info($"  📊 總計：{result.TableResults.Count} 個資料表，{totalRows} 筆資料");

        if (result.FailCount > 0)
        {
            _logger.Info("\n  失敗的資料表：");
            foreach (var tr in result.TableResults.Where(t => !t.Success))
            {
                _logger.Error($"    ❌ {tr.SourceTableName}: {tr.ErrorMessage}");
            }
        }

        _logger.Info(separator + "\n");
    }

    /// <summary>
    /// 驗證所有已成功轉移的資料表
    /// 比對來源與目標的行數、欄位數
    /// </summary>
    private void VerifyAll(TransferResult result)
    {
        var successfulTables = result.TableResults.Where(t => t.Success).ToList();
        if (successfulTables.Count == 0) return;

        string separator = new string('=', 60);

        _logger.Info(separator);
        _logger.Info("  🔍 驗證轉移結果");
        _logger.Info(separator + "\n");

        _logger.Info($"  {"來源表",-35} {"行數(源/目)",-18} {"欄位(源/目)",-18} 結果");
        _logger.Info($"  {new string('─', 35)} {new string('─', 18)} {new string('─', 18)} {new string('─', 4)}");

        foreach (var tableResult in successfulTables)
        {
            var verification = VerifyTable(tableResult.SourceTableName, tableResult.TargetTableName);
            result.VerificationResults.Add(verification);

            if (verification.IsValid)
                result.VerifyPassCount++;
            else
                result.VerifyFailCount++;
        }

        // 驗證摘要
        _logger.Info("");
        _logger.Info($"  驗證結果：✅ 通過 {result.VerifyPassCount} / ❌ 失敗 {result.VerifyFailCount} / 共 {result.VerificationResults.Count} 個表");
        _logger.Info(separator + "\n");
    }

    /// <summary>
    /// 驗證單一資料表的轉移正確性
    /// </summary>
    private VerificationResult VerifyTable(string sourceName, string targetName)
    {
        var verification = new VerificationResult
        {
            SourceTableName = sourceName,
            TargetTableName = targetName
        };

        try
        {
            // 取得來源表結構
            var sourceInfo = _source.GetTableStructure(sourceName);
            if (sourceInfo != null)
            {
                verification.SourceRowCount = sourceInfo.TableRowNum;
                verification.SourceColumnCount = sourceInfo.ColumnNames.Count;
            }

            // 取得目標表結構
            var targetInfo = _target.GetTableStructure(targetName);
            if (targetInfo != null)
            {
                verification.TargetRowCount = targetInfo.TableRowNum;
                verification.TargetColumnCount = targetInfo.ColumnNames.Count;
            }

            // 輸出單行驗證結果
            string rowStatus = verification.RowCountMatch ? "✅" : "❌";
            string colStatus = verification.ColumnCountMatch ? "✅" : "❌";
            string overall = verification.IsValid ? "✅" : "❌";

            _logger.Info(
                $"  {sourceName,-35} " +
                $"{rowStatus} {verification.SourceRowCount,7}/{verification.TargetRowCount,-7}  " +
                $"{colStatus} {verification.SourceColumnCount,3}/{verification.TargetColumnCount,-3}         " +
                $"{overall}");
        }
        catch (Exception ex)
        {
            verification.ErrorMessage = ex.Message;
            _logger.Error($"  {sourceName,-35} ⚠️ 驗證失敗：{ex.Message}");
        }

        return verification;
    }
}
