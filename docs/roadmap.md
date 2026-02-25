# 📋 資料庫轉移專案學習計劃

## 專案目標

1. 建立起自己的 MSSQL 資料庫內容
2. 將轉移程式碼以 C# 來改寫，並做好 clean code
3. 測試轉移成功
4. 透過這個專案來學習 C# 開發

---

## **階段一：環境準備與 MSSQL 資料庫建立** (1-2 天)

### 1.1 安裝必要軟體
- [ ] 安裝 SQL Server 2019/2022 Express Edition (免費版本)
- [ ] 安裝 SQL Server Management Studio (SSMS)
- [ ] 安裝 MariaDB Server
- [ ] 安裝 Visual Studio 2022 Community Edition (含 .NET 8)
- [ ] 安裝 HeidiSQL 或 DBeaver (MariaDB 管理工具)

### 1.2 建立 MSSQL 測試資料庫
- [ ] 使用 SSMS 建立新資料庫 (例如: `TestSourceDB`)
- [ ] 根據報告內容建立範例資料表結構
- [ ] 插入測試資料 (至少 3-5 個表格，每個表格 10-100 筆資料)
- [ ] 建立主鍵、外鍵、索引等約束條件

### 1.3 建立 MariaDB 目標資料庫  
- [ ] 建立新資料庫 (例如: `TestTargetDB`)
- [ ] 確認可以正常連線

---

## **階段二：C# 專案架構設計** (2-3 天)

### 2.1 專案結構規劃 (Clean Architecture)
```
DatabaseMigration.sln
├── src/
│   ├── DatabaseMigration.Core/          # 核心業務邏輯
│   │   ├── Entities/                    # 資料實體
│   │   ├── Interfaces/                  # 介面定義
│   │   ├── Services/                    # 業務服務
│   │   └── Models/                      # DTOs, 配置模型
│   │
│   ├── DatabaseMigration.Infrastructure/ # 基礎設施層
│   │   ├── Database/                    # 資料庫連接實作
│   │   │   ├── MsSqlRepository.cs
│   │   │   └── MariaDbRepository.cs
│   │   ├── Mapping/                     # 資料類型映射
│   │   └── Logging/                     # 日誌實作
│   │
│   └── DatabaseMigration.Console/       # 主控台應用程式
│       ├── Program.cs
│       └── Commands/                     # 命令處理
│
├── tests/
│   ├── DatabaseMigration.Core.Tests/
│   └── DatabaseMigration.Integration.Tests/
│
└── docs/
    ├── API-Documentation.md
    └── Migration-Guide.md
```

### 2.2 核心類別設計
- [ ] `IDatabaseConnector` - 資料庫連接介面
- [ ] `IDataTypeMapper` - 資料類型映射介面
- [ ] `IMigrationService` - 遷移服務介面
- [ ] `MigrationConfiguration` - 配置類別
- [ ] `MigrationLogger` - 日誌記錄器

---

## **階段三：C# 核心功能開發** (5-7 天)

### 3.1 資料庫連接模組
**學習重點：**
- ADO.NET 基礎
- Connection String 管理
- 使用 `SqlConnection` (MSSQL) 和 `MySqlConnection` (MariaDB)
- 連接池管理
- 異常處理

**實作任務：**
- [ ] 實作 `MsSqlConnector` 類別
- [ ] 實作 `MariaDbConnector` 類別
- [ ] 實作連接測試方法
- [ ] 實作連接重試機制

### 3.2 Schema 讀取模組
**學習重點：**
- INFORMATION_SCHEMA 查詢
- Metadata 讀取
- LINQ 查詢語法

**實作任務：**
- [ ] 讀取資料表清單
- [ ] 讀取欄位結構（名稱、類型、長度、精度）
- [ ] 讀取主鍵資訊
- [ ] 讀取外鍵關係
- [ ] 讀取索引資訊

### 3.3 資料類型映射模組
**學習重點：**
- Dictionary 資料結構
- 策略模式 (Strategy Pattern)
- 擴充方法 (Extension Methods)

**實作任務：**
- [ ] 建立 MSSQL 到 MariaDB 的類型映射表
- [ ] 處理特殊類型轉換 (如: UNIQUEIDENTIFIER → VARCHAR(36))
- [ ] 實作類型驗證邏輯

### 3.4 DDL 生成模組
**學習重點：**
- StringBuilder 使用
- SQL 語法生成
- Template Method Pattern

**實作任務：**
- [ ] 生成 CREATE TABLE 語句
- [ ] 生成 PRIMARY KEY 約束
- [ ] 生成 FOREIGN KEY 約束
- [ ] 生成 INDEX 語句

### 3.5 資料遷移模組
**學習重點：**
- Bulk Insert 處理
- 交易管理 (Transaction)
- 批次處理
- 進度追蹤

**實作任務：**
- [ ] 實作分批讀取資料
- [ ] 實作批次插入 (`MySqlBulkCopy` 或參數化查詢)
- [ ] 實作錯誤處理與回滾機制
- [ ] 實作進度條顯示

### 3.6 驗證模組
**學習重點：**
- 資料完整性檢查
- Checksum 計算
- 平行處理 (Parallel.ForEach)

**實作任務：**
- [ ] 比對資料筆數
- [ ] 抽樣驗證資料內容
- [ ] 生成驗證報告

---

## **階段四：Clean Code 實踐** (2-3 天)

### 4.1 程式碼品質改進
- [ ] 套用 SOLID 原則
  - Single Responsibility: 每個類別只做一件事
  - Open/Closed: 開放擴充，封閉修改
  - Liskov Substitution: 子類別可以替換父類別
  - Interface Segregation: 介面細分化
  - Dependency Inversion: 依賴抽象而非具體實作

- [ ] 使用依賴注入 (Dependency Injection)
  - 使用 `Microsoft.Extensions.DependencyInjection`
  - 註冊服務生命週期

- [ ] 實作設計模式
  - Repository Pattern (資料存取層)
  - Factory Pattern (連接器工廠)
  - Strategy Pattern (類型映射策略)

### 4.2 程式碼規範
- [ ] 遵循 C# Coding Conventions
- [ ] 使用有意義的命名
- [ ] 新增 XML 文件註解
- [ ] 實作 async/await 非同步操作
- [ ] 使用 `IDisposable` 管理資源釋放

### 4.3 配置管理
- [ ] 使用 `appsettings.json` 儲存資料庫配置
- [ ] 使用 `IConfiguration` 讀取配置
- [ ] 支援環境變數覆寫

---

## **階段五：測試與驗證** (2-3 天)

### 5.1 單元測試
**學習重點：**
- xUnit 測試框架
- Moq 模擬框架
- 測試驅動開發 (TDD) 概念

**實作任務：**
- [ ] 測試資料類型映射邏輯
- [ ] 測試 DDL 生成正確性
- [ ] 測試錯誤處理機制
- [ ] 測試配置讀取

### 5.2 整合測試
- [ ] 測試實際資料庫連接
- [ ] 測試小規模資料遷移 (10-100 筆)
- [ ] 測試大規模資料遷移 (10000+ 筆)
- [ ] 測試交易回滾機制

### 5.3 效能測試
- [ ] 測試不同批次大小的效能
- [ ] 使用 `BenchmarkDotNet` 進行基準測試
- [ ] 優化瓶頸點

---

## **階段六：文檔與部署** (1-2 天)

### 6.1 文檔撰寫
- [ ] README.md (使用說明)
- [ ] 架構設計文件
- [ ] API 文檔
- [ ] 遷移指南

### 6.2 部署準備
- [ ] 建立發布配置 (Release Build)
- [ ] 打包為單一執行檔 (Self-Contained)
- [ ] 撰寫部署腳本

---

## **學習資源推薦**

### C# 基礎
- Microsoft Learn: C# 官方教學
- C# 10 in a Nutshell (書籍)
- Pluralsight/Udemy C# 課程

### Clean Code 與設計模式
- Clean Code (Robert C. Martin)
- Refactoring Guru (設計模式圖解)
- C# Design Patterns (Pluralsight)

### 資料庫開發
- ADO.NET 官方文檔
- Entity Framework Core (進階選項)
- SQL Performance Tuning

---

## **時程預估**

- **快速版：** 2-3 週 (每天 4-6 小時)
- **標準版：** 4-6 週 (每天 2-3 小時)
- **完整版：** 8-10 週 (每天 1-2 小時，包含深入學習)

---

## **檢查點與里程碑**

- ✅ **里程碑 1：** 成功連接兩個資料庫  
- ✅ **里程碑 2：** 成功讀取並輸出 Schema 結構  
- ✅ **里程碑 3：** 成功遷移第一個資料表  
- ✅ **里程碑 4：** 完成所有資料表的遷移  
- ✅ **里程碑 5：** 通過所有測試  
- ✅ **里程碑 6：** 完成文檔與部署  

---

## 下一步行動

選擇你想開始的階段：
1. 🗄️ 建立 MSSQL 測試資料庫腳本
2. 🏗️ 建立 C# 專案架構
3. 💻 開始實作核心功能
4. 📚 提供學習資源清單

**專案開始日期：** 2026-02-25  
**預計完成日期：** _待定_
