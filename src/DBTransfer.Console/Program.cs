using DBTransfer.Infrastructure.Database;
using DBTransfer.Core.Utils;
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

    // 測試 6: GetTableStructure() - 取得資料表結構資訊
    Console.WriteLine("=== 測試 GetTableStructure() ===");
    Console.WriteLine("正在查詢 'Sales.Currency' 資料表的結構...\n");
    
    var tableInfo = connector.GetTableStructure("Sales.Currency");
    
    if (tableInfo != null)
    {
        Console.WriteLine("✅ 成功取得資料表結構資訊\n");
        Console.WriteLine($"  📌 Schema: {tableInfo.Schema}");
        Console.WriteLine($"  📌 Table Name: {tableInfo.TableName}");
        Console.WriteLine($"  📌 Row Count: {tableInfo.TableRowNum}");
        Console.WriteLine($"  📌 Columns ({tableInfo.ColumnNames.Count}):");
        foreach (var columnName in tableInfo.ColumnNames)
        {
            Console.WriteLine($"     - {columnName}");
        }
        Console.WriteLine();
    }
    else
    {
        Console.WriteLine("❌ 無法取得資料表結構資訊\n");
    }

    // 測試 7: DatabaseExists() - 檢查資料庫是否存在
    Console.WriteLine("=== 測試 DatabaseExists() ===");
    Console.WriteLine("檢查當前資料庫是否存在...\n");
    
    bool dbExists = connector.DatabaseExists("AdventureWorks2022");
    if(dbExists)
    {
        Console.WriteLine("✅ 資料庫 'AdventureWorks2022' 存在\n");
    }
    else
    {
        Console.WriteLine("❌ 資料庫 'AdventureWorks2022' 不存在\n");
    }
    
    // 測試不存在的資料庫
    Console.WriteLine("檢查不存在的資料庫...");
    bool dbNotExists = connector.DatabaseExists("NonExistentDB");
    if(!dbNotExists)
    {
        Console.WriteLine("✅ 正確識別：資料庫 'NonExistentDB' 不存在\n");
    }
    else
    {
        Console.WriteLine("❌ 錯誤：資料庫 'NonExistentDB' 應該不存在\n");
    }

    // 測試 8: UseDatabase() - 切換資料庫
    Console.WriteLine("=== 測試 UseDatabase() ===");
    
    // 先切換到 master
    Console.WriteLine("嘗試切換到 'master' 資料庫...");
    bool switchedToMaster = connector.UseDatabase("master");
    if(switchedToMaster)
    {
        Console.WriteLine("✅ 成功切換到 'master' 資料庫");
        Console.WriteLine($"當前連接字串: {connector.GetConnectionString()}\n");
    }
    
    // 再切換回 AdventureWorks2022
    Console.WriteLine("嘗試切換回 'AdventureWorks2022' 資料庫...");
    bool switchedBack = connector.UseDatabase("AdventureWorks2022");
    if(switchedBack)
    {
        Console.WriteLine("✅ 成功切換回 'AdventureWorks2022' 資料庫");
        Console.WriteLine($"當前連接字串: {connector.GetConnectionString()}\n");
    }
    
    // 測試切換到不存在的資料庫
    Console.WriteLine("嘗試切換到不存在的資料庫...");
    bool switchFailed = connector.UseDatabase("NonExistentDB");
    if(!switchFailed)
    {
        Console.WriteLine("✅ 正確處理：無法切換到不存在的資料庫\n");
    }
    
    // 測試切換到相同的資料庫
    Console.WriteLine("嘗試切換到當前資料庫...");
    bool switchSame = connector.UseDatabase("AdventureWorks2022");
    if(switchSame)
    {
        Console.WriteLine("✅ 正確處理：已經在目標資料庫中\n");
    }

    // 測試 9: CreateDatabase() - 測試創建資料庫（應該不支援）
    Console.WriteLine("=== 測試 CreateDatabase() ===");
    Console.WriteLine("嘗試創建資料庫（應該不支援）...");
    bool created = connector.CreateDatabase("TestDB");
    if(!created)
    {
        Console.WriteLine("✅ 正確：MsSqlConnector 不支援創建資料庫\n");
    }
    else
    {
        Console.WriteLine("❌ 錯誤：不應該支援創建資料庫\n");
    }

    // 測試 10: Disconnect() - 關閉連線
    Console.WriteLine("=== 測試 Disconnect() ===");
    Console.WriteLine("正在斷開連線...");
    connector.Disconnect();
}
else
{
    Console.WriteLine("❌ 連線失敗！\n");
}

Console.WriteLine("\n" + new string('=', 60));
Console.WriteLine("=== Testing MariaDB Connection ===");
Console.WriteLine(new string('=', 60) + "\n");

// 從環境變數讀取 MariaDB 連接資訊
string mariaHost = Env.GetString("MARIADB_HOST", "localhost");
string mariaPort = Env.GetString("MARIADB_PORT", "3306");
string mariaUser = Env.GetString("MARIADB_USER", "root");
string mariaPassword = Env.GetString("MARIADB_PASSWORD", "");

Console.WriteLine($"Host: {mariaHost}");
Console.WriteLine($"Port: {mariaPort}");
Console.WriteLine($"User: {mariaUser}");
Console.WriteLine($"Password: {(string.IsNullOrEmpty(mariaPassword) ? "❌ 未設定" : "✅ ****")}\n");

// 組合 MariaDB 連接字串（不指定資料庫）
string mariaConnectionString = 
    $"Server={mariaHost};" +
    $"Port={mariaPort};" +
    $"User={mariaUser};" +
    $"Password={mariaPassword};";

var mariaConnector = new MariaDbConnector(mariaConnectionString);

// 測試 1: GetConnectionString()
Console.WriteLine("=== 測試 GetConnectionString() ===");
string mariaMaskedConnectionString = mariaConnector.GetConnectionString();
Console.WriteLine($"連接字串（隱藏密碼）: {mariaMaskedConnectionString}\n");

// 測試 2: TestConnection()
Console.WriteLine("=== 測試 TestConnection() ===");
Console.WriteLine("正在測試連線（不保持連線）...");
bool mariaTestResult = mariaConnector.TestConnection();
if(mariaTestResult)
{
    Console.WriteLine("✅ MariaDB 測試連線成功！\n");
}
else
{
    Console.WriteLine("❌ MariaDB 測試連線失敗！\n");
}

// 測試 3: Connect()
Console.WriteLine("=== 測試 Connect() ===");
Console.WriteLine("正在建立持續連線...");
bool mariaConnected = mariaConnector.Connect();

if(mariaConnected)
{
    Console.WriteLine("✅ MariaDB 連線成功！連線已保持開啟\n");
    
    Console.WriteLine("💡 提示：目前連接到 MariaDB 伺服器（未指定資料庫）");
    Console.WriteLine("💡 後續將測試資料庫管理功能\n");
    
    // 測試 4: DatabaseExists() - 檢查資料庫是否存在
    Console.WriteLine("=== 測試 DatabaseExists() ===");
    string testDbName = "TestDB_Migration";
    
    Console.WriteLine($"檢查測試資料庫 '{testDbName}' 是否存在...");
    bool testDbExists = mariaConnector.DatabaseExists(testDbName);
    if(testDbExists)
    {
        Console.WriteLine($"⚠️ 資料庫 '{testDbName}' 已存在，將先刪除\n");
        mariaConnector.DeleteDatabase(testDbName);
    }
    else
    {
        Console.WriteLine($"✅ 資料庫 '{testDbName}' 不存在（正常）\n");
    }
    
    // 測試 5: CreateDatabase() - 創建測試資料庫
    Console.WriteLine("=== 測試 CreateDatabase() ===");
    Console.WriteLine($"嘗試創建測試資料庫 '{testDbName}'...");
    bool dbCreated = mariaConnector.CreateDatabase(testDbName);
    if(dbCreated)
    {
        Console.WriteLine($"✅ 成功創建資料庫 '{testDbName}'\n");
        
        // 驗證資料庫確實被創建
        bool verifyExists = mariaConnector.DatabaseExists(testDbName);
        if(verifyExists)
        {
            Console.WriteLine($"✅ 驗證成功：資料庫 '{testDbName}' 確實存在\n");
        }
    }
    else
    {
        Console.WriteLine($"❌ 創建資料庫 '{testDbName}' 失敗\n");
    }
    
    // 測試重複創建（應該返回 true 但不會重新創建）
    Console.WriteLine("嘗試重複創建同一個資料庫...");
    bool dbCreatedAgain = mariaConnector.CreateDatabase(testDbName);
    if(dbCreatedAgain)
    {
        Console.WriteLine("✅ 正確處理：資料庫已存在，不重複創建\n");
    }
    
    // 測試 6: UseDatabase() - 切換到新創建的資料庫
    Console.WriteLine("=== 測試 UseDatabase() ===");
    Console.WriteLine($"嘗試切換到 '{testDbName}' 資料庫...");
    bool switched = mariaConnector.UseDatabase(testDbName);
    if(switched)
    {
        Console.WriteLine($"✅ 成功切換到 '{testDbName}' 資料庫");
        Console.WriteLine($"當前連接字串: {mariaConnector.GetConnectionString()}\n");
    }
    else
    {
        Console.WriteLine($"❌ 切換到 '{testDbName}' 失敗\n");
    }
    
    // 測試切換到不存在的資料庫
    Console.WriteLine("嘗試切換到不存在的資料庫...");
    bool switchFailed = mariaConnector.UseDatabase("NonExistentDB_123");
    if(!switchFailed)
    {
        Console.WriteLine("✅ 正確處理：無法切換到不存在的資料庫\n");
    }
    
    // 測試切換到相同的資料庫
    Console.WriteLine($"嘗試切換到當前資料庫 '{testDbName}'...");
    bool switchSame = mariaConnector.UseDatabase(testDbName);
    if(switchSame)
    {
        Console.WriteLine("✅ 正確處理：已經在目標資料庫中\n");
    }
    
    // 測試 7: DeleteDatabase() - 刪除測試資料庫
    Console.WriteLine("=== 測試 DeleteDatabase() ===");
    Console.WriteLine($"嘗試刪除測試資料庫 '{testDbName}'...");
    bool dbDeleted = mariaConnector.DeleteDatabase(testDbName);
    if(dbDeleted)
    {
        Console.WriteLine($"✅ 成功刪除資料庫 '{testDbName}'\n");
        
        // 驗證資料庫確實被刪除
        bool verifyDeleted = mariaConnector.DatabaseExists(testDbName);
        if(!verifyDeleted)
        {
            Console.WriteLine($"✅ 驗證成功：資料庫 '{testDbName}' 已被刪除\n");
        }
        else
        {
            Console.WriteLine($"❌ 驗證失敗：資料庫 '{testDbName}' 仍然存在\n");
        }
    }
    else
    {
        Console.WriteLine($"❌ 刪除資料庫 '{testDbName}' 失敗\n");
    }
    
    // 測試刪除不存在的資料庫
    Console.WriteLine("嘗試刪除不存在的資料庫...");
    bool deleteNonExistent = mariaConnector.DeleteDatabase("NonExistentDB_123");
    if(!deleteNonExistent)
    {
        Console.WriteLine("✅ 正確處理：無法刪除不存在的資料庫\n");
    }
    
    // 測試非法資料庫名稱
    Console.WriteLine("嘗試創建非法名稱的資料庫...");
    bool invalidName = mariaConnector.CreateDatabase("Test@DB#Invalid");
    if(!invalidName)
    {
        Console.WriteLine("✅ 正確處理：拒絕非法資料庫名稱\n");
    }
    
    // 測試 8: Disconnect()
    Console.WriteLine("=== 測試 Disconnect() ===");
    Console.WriteLine("正在斷開連線...");
    mariaConnector.Disconnect();
}
else
{
    Console.WriteLine("❌ MariaDB 連線失敗！\n");
    Console.WriteLine("請檢查：");
    Console.WriteLine("  1. MariaDB 服務是否啟動");
    Console.WriteLine("  2. SSH Tunnel 是否建立 (Port 3306)");
    Console.WriteLine("  3. .env 檔案中的帳號密碼是否正確\n");
}

Console.WriteLine("\n" + new string('=', 60));
Console.WriteLine("=== Testing TableNameConverter ===");
Console.WriteLine(new string('=', 60) + "\n");

// 測試 1: 基本轉換 (Schema.Table → Schema_Table)
Console.WriteLine("=== 測試 1: 基本轉換 ===");
string test1 = "Sales.Currency";
string result1 = TableNameConverter.ConvertToMariaDB(test1);
Console.WriteLine($"輸入：{test1}");
Console.WriteLine($"輸出：{result1}");
Console.WriteLine($"預期：Sales_Currency");
Console.WriteLine(result1 == "Sales_Currency" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 2: 帶方括號的表名
Console.WriteLine("=== 測試 2: 移除方括號 ===");
string test2 = "[Sales].[Currency]";
string result2 = TableNameConverter.ConvertToMariaDB(test2);
Console.WriteLine($"輸入：{test2}");
Console.WriteLine($"輸出：{result2}");
Console.WriteLine($"預期：Sales_Currency");
Console.WriteLine(result2 == "Sales_Currency" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 3: 預設 Schema (dbo)
Console.WriteLine("=== 測試 3: dbo Schema ===");
string test3 = "dbo.ErrorLog";
string result3 = TableNameConverter.ConvertToMariaDB(test3);
Console.WriteLine($"輸入：{test3}");
Console.WriteLine($"輸出：{result3}");
Console.WriteLine($"預期：dbo_ErrorLog");
Console.WriteLine(result3 == "dbo_ErrorLog" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 4: 只有表名（無 Schema）
Console.WriteLine("=== 測試 4: 無 Schema 的表名 ===");
string test4 = "MyTable";
string result4 = TableNameConverter.ConvertToMariaDB(test4);
Console.WriteLine($"輸入：{test4}");
Console.WriteLine($"輸出：{result4}");
Console.WriteLine($"預期：MyTable");
Console.WriteLine(result4 == "MyTable" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 5: 複雜的表名
Console.WriteLine("=== 測試 5: 複雜的表名 ===");
string test5 = "[HumanResources].[EmployeeDepartmentHistory]";
string result5 = TableNameConverter.ConvertToMariaDB(test5);
Console.WriteLine($"輸入：{test5}");
Console.WriteLine($"輸出：{result5}");
Console.WriteLine($"預期：HumanResources_EmployeeDepartmentHistory");
Console.WriteLine(result5 == "HumanResources_EmployeeDepartmentHistory" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 6: ExtractSchema() - 提取 Schema
Console.WriteLine("=== 測試 6: 提取 Schema ===");
string test6 = "Sales.Currency";
string? schema6 = TableNameConverter.ExtractSchema(test6);
Console.WriteLine($"輸入：{test6}");
Console.WriteLine($"輸出：{schema6 ?? "null"}");
Console.WriteLine($"預期：Sales");
Console.WriteLine(schema6 == "Sales" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 7: ExtractTableName() - 提取表名
Console.WriteLine("=== 測試 7: 提取表名 ===");
string test7 = "Sales.Currency";
string tableName7 = TableNameConverter.ExtractTableName(test7);
Console.WriteLine($"輸入：{test7}");
Console.WriteLine($"輸出：{tableName7}");
Console.WriteLine($"預期：Currency");
Console.WriteLine(tableName7 == "Currency" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 8: 反向轉換 (MariaDB → MSSQL)
Console.WriteLine("=== 測試 8: 反向轉換 ===");
string test8 = "Sales_Currency";
string result8 = TableNameConverter.ConvertToMSSQL(test8);
Console.WriteLine($"輸入：{test8}");
Console.WriteLine($"輸出：{result8}");
Console.WriteLine($"預期：Sales.Currency");
Console.WriteLine(result8 == "Sales.Currency" ? "✅ 通過\n" : "❌ 失敗\n");

// 測試 9: 真實表名列表測試
Console.WriteLine("=== 測試 9: 批次轉換真實表名 ===");
var realTableNames = new[] 
{
    "Sales.Currency",
    "Sales.Customer", 
    "Person.Address",
    "HumanResources.Employee",
    "Production.Product",
    "dbo.DatabaseLog"
};

Console.WriteLine("轉換結果：");
foreach (var tableName in realTableNames)
{
    string converted = TableNameConverter.ConvertToMariaDB(tableName);
    Console.WriteLine($"  {tableName,-30} → {converted}");
}
Console.WriteLine();

// 測試 10: 錯誤處理
Console.WriteLine("=== 測試 10: 錯誤處理 ===");
try
{
    TableNameConverter.ConvertToMariaDB("");
    Console.WriteLine("❌ 應該拋出例外但沒有\n");
}
catch (ArgumentException ex)
{
    Console.WriteLine($"✅ 正確處理空字串：{ex.Message}\n");
}

Console.WriteLine("\n=== 所有測試完成 ===");
