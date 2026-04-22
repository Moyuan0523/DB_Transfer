namespace DBTransfer.Core.Logging;

/// <summary>
/// 日誌介面，支援多層級記錄
/// </summary>
public interface ITransferLogger
{
    void Info(string message);
    void Warn(string message);
    void Error(string message);
    void Error(string message, Exception ex);
}
