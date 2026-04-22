using DBTransfer.Infrastructure.Database;
using DBTransfer.Core.Services;
using DBTransfer.Core.Logging;
using DotNetEnv;

// ========== 載入環境變數 ==========
string baseDir = AppContext.BaseDirectory;
string projectRoot = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", ".."));
string envPath = Path.Combine(projectRoot, ".env");

// ========== 初始化日誌系統 ==========
string logDir = Path.Combine(projectRoot, "logs");
string logFilePath = Path.Combine(logDir, $"transfer_{DateTime.Now:yyyyMMdd_HHmmss}.log");
using var logger = new TransferLogger(logFilePath);

string separator = new string('=', 60);

logger.Info($"\n{separator}");
logger.Info("  資料庫轉移工具 (MSSQL → MariaDB)");
logger.Info($"{separator}\n");
logger.Info($"📝 日誌檔案：{logFilePath}\n");

if (File.Exists(envPath))
{
    Env.Load(envPath);
    logger.Info($"✅ .env 載入成功：{envPath}\n");
}
else
{
    logger.Error($"❌ 找不到 .env 檔案：{envPath}");
    logger.Info("   請參考 .env.example 建立 .env 檔案");
    return;
}

// ========== 讀取連線設定 ==========
string mssqlServer   = Env.GetString("MSSQL_SERVER",   "localhost,1433");
string mssqlDatabase = Env.GetString("MSSQL_DATABASE", "AdventureWorks2022");
string mssqlUsername = Env.GetString("MSSQL_USERNAME", "sa");
string mssqlPassword = Env.GetString("MSSQL_PASSWORD", "");

string mariaHost     = Env.GetString("MARIADB_HOST",     "localhost");
string mariaPort     = Env.GetString("MARIADB_PORT",     "3306");
string mariaUser     = Env.GetString("MARIADB_USER",     "root");
string mariaPassword = Env.GetString("MARIADB_PASSWORD", "");

// 目標資料庫名稱（可在 .env 自訂）
string targetDbName = Env.GetString("MARIADB_TARGET_DATABASE", "AdventureWorks2022_Transfer");

logger.Info("📌 來源 (MSSQL)");
logger.Info($"   Server:   {mssqlServer}");
logger.Info($"   Database: {mssqlDatabase}");
logger.Info($"   User:     {mssqlUsername}");
logger.Info("");
logger.Info("📌 目標 (MariaDB)");
logger.Info($"   Host:      {mariaHost}:{mariaPort}");
logger.Info($"   User:      {mariaUser}");
logger.Info($"   Target DB: {targetDbName}");
logger.Info("");

// ========== 組合連線字串 ==========
string mssqlConnectionString =
    $"Server={mssqlServer};" +
    $"Database={mssqlDatabase};" +
    $"User Id={mssqlUsername};" +
    $"Password={mssqlPassword};" +
    "TrustServerCertificate=True;";

string mariaConnectionString =
    $"Server={mariaHost};" +
    $"Port={mariaPort};" +
    $"User={mariaUser};" +
    $"Password={mariaPassword};";

// ========== 建立連接器並測試連線 ==========
var mssqlConnector = new MsSqlConnector(mssqlConnectionString, logger);
var mariaConnector = new MariaDbConnector(mariaConnectionString, logger);

logger.Info("🔌 測試連線...");

if (!mssqlConnector.TestConnection())
{
    logger.Error("❌ MSSQL 連線失敗！請檢查 SSH 隧道和連線設定");
    return;
}
logger.Info("  ✅ MSSQL 連線正常");

if (!mariaConnector.TestConnection())
{
    logger.Error("❌ MariaDB 連線失敗！請檢查 SSH 隧道和連線設定");
    return;
}
logger.Info("  ✅ MariaDB 連線正常\n");

// ========== 建立持續連線 ==========
if (!mssqlConnector.Connect() || !mariaConnector.Connect())
{
    logger.Error("❌ 無法建立持續連線");
    return;
}

try
{
    // ========== 執行轉移 ==========
    var transferService = new DatabaseTransferService(mssqlConnector, mariaConnector, logger);
    var result = transferService.TransferAll(targetDbName);

    // ========== 最終結果 ==========
    if (result.Success)
    {
        logger.Info("🎉 資料庫轉移全部完成，驗證通過！");
    }
    else
    {
        logger.Warn($"⚠️ 轉移完成，但有部分失敗");
        logger.Warn($"   轉移：成功 {result.SuccessCount} / 失敗 {result.FailCount}");
        logger.Warn($"   驗證：通過 {result.VerifyPassCount} / 失敗 {result.VerifyFailCount}");
    }
}
finally
{
    mssqlConnector.Disconnect();
    mariaConnector.Disconnect();
    logger.Info("\n🔌 已斷開所有連線");
}
