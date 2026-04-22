using System.Text;

namespace DBTransfer.Core.Logging;

/// <summary>
/// 日誌實作：同時輸出至 Console 與 Log 檔案
/// </summary>
public class TransferLogger : ITransferLogger, IDisposable
{
    private readonly StreamWriter? _fileWriter;
    private readonly object _lock = new();

    public TransferLogger(string? logFilePath = null)
    {
        if (!string.IsNullOrEmpty(logFilePath))
        {
            var dir = Path.GetDirectoryName(logFilePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            _fileWriter = new StreamWriter(logFilePath, append: true, encoding: Encoding.UTF8)
            {
                AutoFlush = true
            };
        }
    }

    public void Info(string message)
    {
        Console.WriteLine(message);
        WriteToFile("INFO ", message);
    }

    public void Warn(string message)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine(message);
        Console.ResetColor();
        WriteToFile("WARN ", message);
    }

    public void Error(string message)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(message);
        Console.ResetColor();
        WriteToFile("ERROR", message);
    }

    public void Error(string message, Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(message);
        Console.ResetColor();
        WriteToFile("ERROR", $"{message}\n  Exception: {ex}");
    }

    private void WriteToFile(string level, string message)
    {
        if (_fileWriter == null) return;

        lock (_lock)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            // 移除 ANSI/emoji 前後的空白，但保留 emoji 本身
            _fileWriter.WriteLine($"[{timestamp}] [{level}] {message}");
        }
    }

    public void Dispose()
    {
        _fileWriter?.Flush();
        _fileWriter?.Dispose();
        GC.SuppressFinalize(this);
    }
}
