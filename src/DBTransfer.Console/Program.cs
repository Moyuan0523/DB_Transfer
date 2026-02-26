using DBTransfer.Infrastructure.Database;
using DotNetEnv;

// 載入 .env 檔案（使用相對路徑）
string baseDir = AppContext.BaseDirectory;
string projectRoot = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", ".."));
string envPath = Path.Combine(projectRoot, ".env");

Console.WriteLine($"🔍 執行檔位置: {baseDir}");
Console.WriteLine($"🔍 專案根目錄: {projectRoot}");
Console.WriteLine($"🔍 .env 檔案位置: {envPath}\n");

if (File.Exists(envPath))
{
    Env.Load(envPath);
    Console.WriteLine("✅ .env 檔案載入成功\n");
}
else
{
    Console.WriteLine("❌ 找不到 .env 檔案！\n");
}

Console.WriteLine("=== Testing MSSQL Connection ===\n");

// 從環境變數讀取連接資訊
string server = Env.GetString("MSSQL_SERVER", "localhost,1433");
string database = Env.GetString("MSSQL_DATABASE", "AdventureWorks2022");
string username = Env.GetString("MSSQL_USERNAME", "sa");
string password = Env.GetString("MSSQL_PASSWORD", "");

Console.WriteLine($"Server: {server}");
Console.WriteLine($"Database: {database}");
Console.WriteLine($"Username: {username}");
Console.WriteLine($"Password: {(string.IsNullOrEmpty(password) ? "❌ 未設定" : "✅ ****")}\n");

// 組合連接字串
string connectionString = 
    $"Server={server};" +
    $"Database={database};" +
    $"User Id={username};" +
    $"Password={password};" +
    "TrustServerCertificate=True;";

var connector = new MsSqlConnector(connectionString);

// 測試 1: GetConnectionString() - 顯示隱藏密碼的連接字串
Console.WriteLine("=== 測試 GetConnectionString() ===");
string maskedConnectionString = connector.GetConnectionString();
Console.WriteLine($"連接字串（隱藏密碼）: {maskedConnectionString}\n");

// 測試 2: TestConnection() - 測試連線但不保持連線
Console.WriteLine("=== 測試 TestConnection() ===");
Console.WriteLine("正在測試連線（不保持連線）...");
bool testResult = connector.TestConnection();
if(testResult)
{
    Console.WriteLine("✅ 測試連線成功！\n");
}
else
{
    Console.WriteLine("❌ 測試連線失敗！\n");
}

// 測試 3: Connect() - 建立持續連線
Console.WriteLine("=== 測試 Connect() ===");
Console.WriteLine("正在建立持續連線...");
bool connected = connector.Connect();

if(connected)
{
    Console.WriteLine("✅ 連線成功！連線已保持開啟\n");

    // 測試 4: GetTableNames() - 查詢所有資料表名稱
    Console.WriteLine("=== 測試 GetTableNames() ===");
    Console.WriteLine("正在查詢所有資料表名稱...\n");
    List<string> tableNames = connector.GetTableNames();
    
    if(tableNames.Count > 0)
    {
        Console.WriteLine($"✅ 成功查詢到 {tableNames.Count} 個資料表：\n");
        foreach(string tableName in tableNames)
        {
            Console.WriteLine($"  📋 {tableName}");
        }
        Console.WriteLine();
    }
    else
    {
        Console.WriteLine("❌ 沒有查詢到任何資料表\n");
    }

    // 測試 5: GetTableData() - 讀取資料表內容
    Console.WriteLine("=== 測試 GetTableData() ===");
    Console.WriteLine("正在讀取 'Sales.Currency' 資料表的內容...\n");
    
    List<Dictionary<string, object>> currencyData = connector.GetTableData("Sales.Currency");
    
    if (currencyData.Count > 0)
    {
        Console.WriteLine($"✅ 成功讀取到 {currencyData.Count} 筆資料\n");
        
        // 顯示前 5 筆資料
        int displayCount = Math.Min(5, currencyData.Count);
        Console.WriteLine($"顯示前 {displayCount} 筆資料：\n");
        
        for (int i = 0; i < displayCount; i++)
        {
            Console.WriteLine($"--- 第 {i + 1} 筆資料 ---");
            foreach (var column in currencyData[i])
            {
                string value = column.Value == DBNull.Value ? "NULL" : column.Value.ToString() ?? "NULL";
                Console.WriteLine($"  {column.Key}: {value}");
            }
            Console.WriteLine();
        }
    }
    else
    {
        Console.WriteLine("❌ 沒有讀取到任何資料\n");
    }

    // 測試 6: Disconnect() - 關閉連線
    Console.WriteLine("=== 測試 Disconnect() ===");
    Console.WriteLine("正在斷開連線...");
    connector.Disconnect();
}
else
{
    Console.WriteLine("❌ 連線失敗！\n");
}

Console.WriteLine("\n=== 所有測試完成 ===");
