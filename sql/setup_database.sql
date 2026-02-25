-- =============================================
-- MSSQL 測試資料庫建立腳本
-- 用途：為資料庫轉移專案建立測試資料
-- =============================================

USE master;
GO

-- 檢查並刪除現有資料庫
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'TestSourceDB')
BEGIN
    ALTER DATABASE TestSourceDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE TestSourceDB;
    PRINT '✓ 已刪除現有資料庫';
END
GO

-- 建立新資料庫
CREATE DATABASE TestSourceDB
ON PRIMARY 
(
    NAME = N'TestSourceDB_Data',
    FILENAME = N'/var/opt/mssql/data/TestSourceDB.mdf',
    SIZE = 100MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
LOG ON 
(
    NAME = N'TestSourceDB_Log',
    FILENAME = N'/var/opt/mssql/data/TestSourceDB_log.ldf',
    SIZE = 50MB,
    MAXSIZE = 500MB,
    FILEGROWTH = 10MB
);
GO

PRINT '✓ 資料庫 TestSourceDB 建立成功';
GO

-- 切換至新資料庫
USE TestSourceDB;
GO

-- =============================================
-- 表格一：Customers (客戶資料表)
-- =============================================
CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerCode NVARCHAR(20) NOT NULL UNIQUE,
    CustomerName NVARCHAR(100) NOT NULL,
    ContactPerson NVARCHAR(50),
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    Address NVARCHAR(200),
    City NVARCHAR(50),
    Country NVARCHAR(50) DEFAULT 'Taiwan',
    PostalCode NVARCHAR(10),
    CreditLimit DECIMAL(18, 2) DEFAULT 0,
    Balance DECIMAL(18, 2) DEFAULT 0,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE(),
    Notes NVARCHAR(MAX)
);
GO

PRINT '✓ Customers 表格建立成功';
GO

-- =============================================
-- 表格二：Products (產品資料表)
-- =============================================
CREATE TABLE Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductCode NVARCHAR(30) NOT NULL UNIQUE,
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50),
    UnitPrice DECIMAL(18, 2) NOT NULL,
    CostPrice DECIMAL(18, 2),
    StockQuantity INT DEFAULT 0,
    ReorderLevel INT DEFAULT 10,
    UnitOfMeasure NVARCHAR(20) DEFAULT 'PCS',
    Barcode NVARCHAR(50),
    Supplier NVARCHAR(100),
    IsDiscontinued BIT DEFAULT 0,
    Weight DECIMAL(10, 3),
    Dimensions NVARCHAR(50),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE(),
    Description NVARCHAR(MAX)
);
GO

PRINT '✓ Products 表格建立成功';
GO

-- =============================================
-- 表格三：Orders (訂單主檔)
-- =============================================
CREATE TABLE Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    OrderNumber NVARCHAR(30) NOT NULL UNIQUE,
    CustomerID INT NOT NULL,
    OrderDate DATETIME DEFAULT GETDATE(),
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    OrderStatus NVARCHAR(20) DEFAULT 'Pending',
    TotalAmount DECIMAL(18, 2) DEFAULT 0,
    DiscountAmount DECIMAL(18, 2) DEFAULT 0,
    TaxAmount DECIMAL(18, 2) DEFAULT 0,
    ShippingCost DECIMAL(18, 2) DEFAULT 0,
    NetAmount DECIMAL(18, 2) DEFAULT 0,
    PaymentMethod NVARCHAR(30),
    PaymentStatus NVARCHAR(20) DEFAULT 'Unpaid',
    ShippingAddress NVARCHAR(200),
    ShippingCity NVARCHAR(50),
    ShippingCountry NVARCHAR(50),
    ShippingPostalCode NVARCHAR(10),
    CreatedBy NVARCHAR(50) DEFAULT SYSTEM_USER,
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE(),
    Remarks NVARCHAR(MAX),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) 
        REFERENCES Customers(CustomerID)
);
GO

PRINT '✓ Orders 表格建立成功';
GO

-- =============================================
-- 表格四：OrderDetails (訂單明細)
-- =============================================
CREATE TABLE OrderDetails (
    OrderDetailID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18, 2) NOT NULL,
    Discount DECIMAL(5, 2) DEFAULT 0,
    LineTotal AS (Quantity * UnitPrice * (1 - Discount / 100)) PERSISTED,
    DeliveryStatus NVARCHAR(20) DEFAULT 'Pending',
    CreatedDate DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_OrderDetails_Orders FOREIGN KEY (OrderID) 
        REFERENCES Orders(OrderID) ON DELETE CASCADE,
    CONSTRAINT FK_OrderDetails_Products FOREIGN KEY (ProductID) 
        REFERENCES Products(ProductID)
);
GO

PRINT '✓ OrderDetails 表格建立成功';
GO

-- =============================================
-- 表格五：Employees (員工資料表)
-- =============================================
CREATE TABLE Employees (
    EmployeeID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeCode NVARCHAR(20) NOT NULL UNIQUE,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    FullName AS (LastName + ' ' + FirstName) PERSISTED,
    Email NVARCHAR(100) UNIQUE,
    Phone NVARCHAR(20),
    HireDate DATE NOT NULL,
    JobTitle NVARCHAR(50),
    Department NVARCHAR(50),
    Salary DECIMAL(18, 2),
    ManagerID INT,
    IsActive BIT DEFAULT 1,
    BirthDate DATE,
    Address NVARCHAR(200),
    City NVARCHAR(50),
    Country NVARCHAR(50) DEFAULT 'Taiwan',
    PostalCode NVARCHAR(10),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_Employees_Manager FOREIGN KEY (ManagerID) 
        REFERENCES Employees(EmployeeID)
);
GO

PRINT '✓ Employees 表格建立成功';
GO

-- =============================================
-- 建立索引以提升查詢效能
-- =============================================

-- Customers 索引
CREATE INDEX IX_Customers_CustomerCode ON Customers(CustomerCode);
CREATE INDEX IX_Customers_Email ON Customers(Email);
CREATE INDEX IX_Customers_City ON Customers(City);
GO

-- Products 索引
CREATE INDEX IX_Products_ProductCode ON Products(ProductCode);
CREATE INDEX IX_Products_Category ON Products(Category);
CREATE INDEX IX_Products_Supplier ON Products(Supplier);
GO

-- Orders 索引
CREATE INDEX IX_Orders_CustomerID ON Orders(CustomerID);
CREATE INDEX IX_Orders_OrderDate ON Orders(OrderDate);
CREATE INDEX IX_Orders_OrderStatus ON Orders(OrderStatus);
CREATE INDEX IX_Orders_OrderNumber ON Orders(OrderNumber);
GO

-- OrderDetails 索引
CREATE INDEX IX_OrderDetails_OrderID ON OrderDetails(OrderID);
CREATE INDEX IX_OrderDetails_ProductID ON OrderDetails(ProductID);
GO

-- Employees 索引
CREATE INDEX IX_Employees_EmployeeCode ON Employees(EmployeeCode);
CREATE INDEX IX_Employees_Department ON Employees(Department);
CREATE INDEX IX_Employees_ManagerID ON Employees(ManagerID);
GO

PRINT '✓ 所有索引建立成功';
GO

-- =============================================
-- 插入測試資料
-- =============================================

-- 插入客戶資料
SET IDENTITY_INSERT Customers ON;
INSERT INTO Customers (CustomerID, CustomerCode, CustomerName, ContactPerson, Email, Phone, Address, City, Country, PostalCode, CreditLimit, Balance, IsActive, Notes)
VALUES 
    (1, 'CUST001', '台灣電子股份有限公司', '陳大明', 'chen@taiwanelec.com', '02-2345-6789', '台北市信義區信義路五段7號', '台北市', 'Taiwan', '110', 500000.00, 125000.00, 1, '長期合作客戶'),
    (2, 'CUST002', '高雄機械工業', '林小美', 'lin@khmachine.com', '07-1234-5678', '高雄市前鎮區中山二路123號', '高雄市', 'Taiwan', '806', 300000.00, 85000.00, 1, 'VIP客戶'),
    (3, 'CUST003', '台中科技有限公司', '王建國', 'wang@tctech.com', '04-2222-3333', '台中市西屯區台灣大道三段99號', '台中市', 'Taiwan', '407', 200000.00, 45000.00, 1, NULL),
    (4, 'CUST004', '新竹半導體', '張志明', 'chang@hcsemi.com', '03-5555-6666', '新竹市東區光復路二段101號', '新竹市', 'Taiwan', '300', 800000.00, 320000.00, 1, '大客戶，需特別注意'),
    (5, 'CUST005', '桃園製造廠', '劉美玲', 'liu@tymanuf.com', '03-3333-4444', '桃園市桃園區中正路888號', '桃園市', 'Taiwan', '330', 150000.00, 30000.00, 1, NULL),
    (6, 'CUST006', '台南精密工業', '黃文龍', 'huang@tnprecision.com', '06-2222-3333', '台南市永康區中華路567號', '台南市', 'Taiwan', '710', 250000.00, 60000.00, 1, NULL),
    (7, 'CUST007', '基隆港務公司', '吳雅婷', 'wu@klport.com', '02-2469-1234', '基隆市仁愛區港西街9號', '基隆市', 'Taiwan', '200', 400000.00, 95000.00, 1, '港口相關業務'),
    (8, 'CUST008', '宜蘭農業科技', '蔡明德', 'tsai@ylagri.com', '03-9876-5432', '宜蘭縣宜蘭市中山路三段88號', '宜蘭縣', 'Taiwan', '260', 100000.00, 15000.00, 1, NULL),
    (9, 'CUST009', '花蓮觀光開發', '鄭淑芬', 'cheng@hltour.com', '03-8521-3456', '花蓮市中華路321號', '花蓮市', 'Taiwan', '970', 180000.00, 42000.00, 1, NULL),
    (10, 'CUST010', '屏東能源公司', '謝明哲', 'hsieh@ptenergy.com', '08-7654-3210', '屏東市廣東路777號', '屏東市', 'Taiwan', '900', 350000.00, 78000.00, 1, '綠能產業');
SET IDENTITY_INSERT Customers OFF;
GO

PRINT '✓ 已插入 10 筆客戶資料';
GO

-- 插入產品資料
SET IDENTITY_INSERT Products ON;
INSERT INTO Products (ProductID, ProductCode, ProductName, Category, UnitPrice, CostPrice, StockQuantity, ReorderLevel, UnitOfMeasure, Barcode, Supplier, Weight, Dimensions, Description)
VALUES 
    (1, 'PROD-A001', '工業控制器 Model X1', '電子元件', 15800.00, 10200.00, 250, 50, 'PCS', '4710123456789', '台灣電子', 0.850, '15x10x8cm', '高效能工業級控制器'),
    (2, 'PROD-A002', '精密感測器 S200', '感測器', 8500.00, 5500.00, 180, 30, 'PCS', '4710123456796', '新竹科技', 0.120, '8x5x3cm', '高精度溫度感測器'),
    (3, 'PROD-B001', '伺服馬達 M500', '馬達', 28000.00, 19000.00, 95, 20, 'PCS', '4710234567890', '台中機械', 5.200, '25x25x30cm', '低噪音高扭力'),
    (4, 'PROD-B002', '步進馬達 SM100', '馬達', 12500.00, 8500.00, 150, 25, 'PCS', '4710234567906', '台中機械', 2.800, '15x15x20cm', '精確定位專用'),
    (5, 'PROD-C001', '工業電源供應器 PS1000', '電源設備', 18900.00, 12800.00, 75, 15, 'PCS', '4710345678901', '台北電機', 3.500, '20x15x10cm', '1000W高效率電源'),
    (6, 'PROD-C002', '穩壓器 SR500', '電源設備', 9800.00, 6500.00, 120, 20, 'PCS', '4710345678918', '台北電機', 1.850, '18x12x8cm', '500W自動穩壓'),
    (7, 'PROD-D001', 'PLC可程式控制器 PLC200', '控制系統', 45000.00, 32000.00, 45, 10, 'PCS', '4710456789012', '高雄工業', 2.200, '30x20x15cm', '16點I/O含觸控螢幕'),
    (8, 'PROD-D002', 'HMI人機介面 HMI7', '控制系統', 22000.00, 15000.00, 68, 12, 'PCS', '4710456789029', '高雄工業', 0.980, '20x15x5cm', '7吋彩色觸控螢幕'),
    (9, 'PROD-E001', '工業相機 CAM1080', '視覺設備', 35000.00, 24000.00, 32, 8, 'PCS', '4710567890123', '新竹光電', 0.650, '12x8x8cm', 'Full HD高速抓圖'),
    (10, 'PROD-E002', '鏡頭組 LENS50', '視覺設備', 18500.00, 12500.00, 55, 10, 'PCS', '4710567890130', '新竹光電', 0.380, '8x8x12cm', '50mm焦距低失真'),
    (11, 'PROD-F001', '氣壓缸 CYL100', '氣動元件', 5800.00, 3800.00, 200, 40, 'PCS', '4710678901234', '台南氣動', 1.250, '15x6x6cm', '標準型雙動作'),
    (12, 'PROD-F002', '電磁閥 VALVE24', '氣動元件', 3200.00, 2100.00, 250, 50, 'PCS', '4710678901241', '台南氣動', 0.420, '10x8x6cm', 'DC24V二位五通'),
    (13, 'PROD-G001', '編碼器 ENC1024', '感測器', 11200.00, 7500.00, 85, 15, 'PCS', '4710789012345', '桃園精密', 0.180, '8x8x5cm', '1024脈衝增量型'),
    (14, 'PROD-G002', '光電開關 PS-E18', '感測器', 2800.00, 1800.00, 300, 60, 'PCS', '4710789012352', '桃園精密', 0.085, '5x3x3cm', '對射型18mm檢測距離'),
    (15, 'PROD-H001', '變頻器 INV2.2K', '變頻設備', 25000.00, 17000.00, 48, 10, 'PCS', '4710890123456', '台北電機', 4.800, '25x20x15cm', '2.2KW三相變頻'),
    (16, 'PROD-H002', '伺服驅動器 SD1K', '變頻設備', 32000.00, 22000.00, 38, 8, 'PCS', '4710890123463', '台北電機', 3.200, '22x18x12cm', '1KW高速響應'),
    (17, 'PROD-I001', '溫控器 TC-PID', '溫控設備', 6500.00, 4300.00, 140, 25, 'PCS', '4710901234567', '新竹儀表', 0.280, '12x8x6cm', 'PID智慧溫控'),
    (18, 'PROD-I002', '加熱器 HEAT500', '溫控設備', 8900.00, 5900.00, 95, 18, 'PCS', '4710901234574', '新竹儀表', 1.450, '20x8x8cm', '500W陶瓷加熱器'),
    (19, 'PROD-J001', '工業交換機 SW8P', '網路設備', 12800.00, 8500.00, 72, 15, 'PCS', '4711012345678', '台灣網通', 0.850, '18x12x5cm', '8埠Gigabit PoE'),
    (20, 'PROD-J002', '無線模組 WIFI-M', '網路設備', 4500.00, 3000.00, 150, 30, 'PCS', '4711012345685', '台灣網通', 0.065, '8x6x2cm', 'WiFi/BT雙模組');
SET IDENTITY_INSERT Products OFF;
GO

PRINT '✓ 已插入 20 筆產品資料';
GO

-- 插入員工資料
SET IDENTITY_INSERT Employees ON;
INSERT INTO Employees (EmployeeID, EmployeeCode, FirstName, LastName, Email, Phone, HireDate, JobTitle, Department, Salary, ManagerID, BirthDate, Address, City)
VALUES 
    (1, 'EMP001', '志明', '王', 'wang.cm@company.com', '0912-345-678', '2018-03-15', '總經理', '管理部', 120000.00, NULL, '1975-05-20', '台北市大安區和平東路123號', '台北市'),
    (2, 'EMP002', '美玲', '林', 'lin.ml@company.com', '0923-456-789', '2019-06-01', '業務經理', '業務部', 85000.00, 1, '1982-08-15', '新北市板橋區中山路二段456號', '新北市'),
    (3, 'EMP003', '建國', '陳', 'chen.jg@company.com', '0934-567-890', '2019-09-10', '技術經理', '技術部', 90000.00, 1, '1980-11-30', '台北市內湖區民權東路六段789號', '台北市'),
    (4, 'EMP004', '淑芬', '張', 'chang.sf@company.com', '0945-678-901', '2020-01-20', '財務經理', '財務部', 80000.00, 1, '1985-03-25', '台北市松山區南京東路四段321號', '台北市'),
    (5, 'EMP005', '明哲', '黃', 'huang.mc@company.com', '0956-789-012', '2020-04-15', '業務專員', '業務部', 52000.00, 2, '1990-07-12', '桃園市中壢區中央西路258號', '桃園市'),
    (6, 'EMP006', '雅婷', '劉', 'liu.yt@company.com', '0967-890-123', '2020-07-01', '業務專員', '業務部', 50000.00, 2, '1992-02-18', '新竹市東區光復路一段147號', '新竹市'),
    (7, 'EMP007', '文龍', '吳', 'wu.wl@company.com', '0978-901-234', '2020-10-12', '工程師', '技術部', 65000.00, 3, '1988-09-05', '台中市西屯區文心路三段369號', '台中市'),
    (8, 'EMP008', '佩君', '蔡', 'tsai.pj@company.com', '0989-012-345', '2021-02-01', '工程師', '技術部', 63000.00, 3, '1991-12-22', '台南市東區東門路一段258號', '台南市'),
    (9, 'EMP009', '俊傑', '鄭', 'cheng.jj@company.com', '0911-123-456', '2021-05-18', '會計', '財務部', 48000.00, 4, '1993-04-08', '高雄市三民區建國二路159號', '高雄市'),
    (10, 'EMP010', '詩涵', '謝', 'hsieh.sh@company.com', '0922-234-567', '2021-08-25', '助理', '管理部', 38000.00, 1, '1995-06-14', '台北市大同區承德路三段753號', '台北市');
SET IDENTITY_INSERT Employees OFF;
GO

PRINT '✓ 已插入 10 筆員工資料';
GO

-- 插入訂單資料
SET IDENTITY_INSERT Orders ON;
INSERT INTO Orders (OrderID, OrderNumber, CustomerID, OrderDate, RequiredDate, OrderStatus, TotalAmount, DiscountAmount, TaxAmount, ShippingCost, NetAmount, PaymentMethod, PaymentStatus, ShippingAddress, ShippingCity, ShippingCountry, Remarks)
VALUES 
    (1, 'ORD-2024-0001', 1, '2024-01-05 10:30:00', '2024-01-12', 'Completed', 158000.00, 7900.00, 7505.00, 500.00, 157105.00, '銀行轉帳', 'Paid', '台北市信義區信義路五段7號', '台北市', 'Taiwan', '急件，請優先處理'),
    (2, 'ORD-2024-0002', 2, '2024-01-08 14:20:00', '2024-01-15', 'Completed', 285000.00, 14250.00, 13537.50, 800.00, 285087.50, '支票', 'Paid', '高雄市前鎮區中山二路123號', '高雄市', 'Taiwan', NULL),
    (3, 'ORD-2024-0003', 4, '2024-01-12 09:15:00', '2024-01-19', 'Shipping', 450000.00, 22500.00, 21375.00, 1000.00, 449875.00, '銀行轉帳', 'Paid', '新竹市東區光復路二段101號', '新竹市', 'Taiwan', '需要提供安裝服務'),
    (4, 'ORD-2024-0004', 3, '2024-01-15 16:45:00', '2024-01-22', 'Processing', 128000.00, 0.00, 6400.00, 600.00, 135000.00, '貨到付款', 'Unpaid', '台中市西屯區台灣大道三段99號', '台中市', 'Taiwan', NULL),
    (5, 'ORD-2024-0005', 6, '2024-01-18 11:30:00', '2024-01-25', 'Pending', 189000.00, 9450.00, 8977.50, 700.00, 189227.50, '信用卡', 'Processing', '台南市永康區中華路567號', '台南市', 'Taiwan', NULL),
    (6, 'ORD-2024-0006', 7, '2024-01-22 13:50:00', '2024-01-29', 'Completed', 256000.00, 12800.00, 12160.00, 900.00, 256260.00, '銀行轉帳', 'Paid', '基隆市仁愛區港西街9號', '基隆市', 'Taiwan', '港口交貨'),
    (7, 'ORD-2024-0007', 10, '2024-01-25 10:20:00', '2024-02-01', 'Processing', 178000.00, 0.00, 8900.00, 750.00, 187650.00, '支票', 'Unpaid', '屏東市廣東路777號', '屏東市', 'Taiwan', NULL),
    (8, 'ORD-2024-0008', 1, '2024-01-28 15:30:00', '2024-02-04', 'Pending', 320000.00, 16000.00, 15200.00, 1200.00, 320400.00, '銀行轉帳', 'Unpaid', '台北市信義區信義路五段7號', '台北市', 'Taiwan', '長期客戶優惠'),
    (9, 'ORD-2024-0009', 5, '2024-02-01 09:45:00', '2024-02-08', 'Processing', 145000.00, 7250.00, 6887.50, 650.00, 145287.50, '現金', 'Paid', '桃園市桃園區中正路888號', '桃園市', 'Taiwan', NULL),
    (10, 'ORD-2024-0010', 9, '2024-02-05 14:15:00', '2024-02-12', 'Pending', 95000.00, 0.00, 4750.00, 550.00, 100300.00, '信用卡', 'Processing', '花蓮市中華路321號', '花蓮市', 'Taiwan', '偏遠地區加收運費');
SET IDENTITY_INSERT Orders OFF;
GO

PRINT '✓ 已插入 10 筆訂單資料';
GO

-- 插入訂單明細資料
SET IDENTITY_INSERT OrderDetails ON;
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity, UnitPrice, Discount)
VALUES 
    -- Order 1
    (1, 1, 1, 10, 15800.00, 5.00),
    (2, 1, 2, 5, 8500.00, 5.00),
    -- Order 2
    (3, 2, 3, 10, 28000.00, 5.00),
    (4, 2, 7, 1, 45000.00, 5.00),
    -- Order 3
    (5, 3, 7, 10, 45000.00, 5.00),
    -- Order 4
    (6, 4, 4, 8, 12500.00, 0.00),
    (7, 4, 6, 3, 9800.00, 0.00),
    -- Order 5
    (8, 5, 9, 5, 35000.00, 5.00),
    (9, 5, 10, 2, 18500.00, 5.00),
    -- Order 6
    (10, 6, 15, 8, 25000.00, 5.00),
    (11, 6, 16, 2, 32000.00, 5.00),
    -- Order 7
    (12, 7, 5, 8, 18900.00, 0.00),
    (13, 7, 17, 4, 6500.00, 0.00),
    -- Order 8
    (14, 8, 3, 10, 28000.00, 5.00),
    (15, 8, 8, 2, 22000.00, 5.00),
    -- Order 9
    (16, 9, 11, 20, 5800.00, 5.00),
    (17, 9, 12, 10, 3200.00, 5.00),
    -- Order 10
    (18, 10, 13, 8, 11200.00, 0.00),
    (19, 10, 14, 5, 2800.00, 0.00);
SET IDENTITY_INSERT OrderDetails OFF;
GO

PRINT '✓ 已插入 19 筆訂單明細資料';
GO

-- =============================================
-- 建立檢視表 (Views)
-- =============================================

-- 訂單摘要檢視表
CREATE VIEW vw_OrderSummary AS
SELECT 
    o.OrderID,
    o.OrderNumber,
    o.OrderDate,
    c.CustomerName,
    c.City,
    o.OrderStatus,
    o.PaymentStatus,
    COUNT(od.OrderDetailID) AS TotalItems,
    SUM(od.Quantity) AS TotalQuantity,
    o.NetAmount,
    DATEDIFF(day, o.OrderDate, GETDATE()) AS DaysSinceOrder
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
LEFT JOIN OrderDetails od ON o.OrderID = od.OrderID
GROUP BY o.OrderID, o.OrderNumber, o.OrderDate, c.CustomerName, c.City, 
         o.OrderStatus, o.PaymentStatus, o.NetAmount;
GO

-- 產品庫存檢視表
CREATE VIEW vw_ProductInventory AS
SELECT 
    p.ProductID,
    p.ProductCode,
    p.ProductName,
    p.Category,
    p.UnitPrice,
    p.StockQuantity,
    p.ReorderLevel,
    CASE 
        WHEN p.StockQuantity <= p.ReorderLevel THEN '需補貨'
        WHEN p.StockQuantity <= p.ReorderLevel * 2 THEN '庫存偏低'
        ELSE '庫存正常'
    END AS StockStatus,
    p.StockQuantity * p.UnitPrice AS StockValue
FROM Products p;
GO

-- 客戶應收帳款檢視表
CREATE VIEW vw_CustomerBalance AS
SELECT 
    c.CustomerID,
    c.CustomerCode,
    c.CustomerName,
    c.CreditLimit,
    c.Balance AS CurrentBalance,
    c.CreditLimit - c.Balance AS AvailableCredit,
    CASE 
        WHEN c.Balance > c.CreditLimit THEN '超過額度'
        WHEN c.Balance > c.CreditLimit * 0.8 THEN '接近額度'
        ELSE '正常'
    END AS CreditStatus
FROM Customers c
WHERE c.IsActive = 1;
GO

PRINT '✓ 檢視表建立成功';
GO

-- =============================================
-- 建立預存程序 (Stored Procedures)
-- =============================================

-- 取得客戶訂單歷史
CREATE PROCEDURE sp_GetCustomerOrderHistory
    @CustomerID INT
AS
BEGIN
    SELECT 
        o.OrderNumber,
        o.OrderDate,
        o.OrderStatus,
        o.PaymentStatus,
        o.NetAmount,
        COUNT(od.OrderDetailID) AS ItemCount
    FROM Orders o
    LEFT JOIN OrderDetails od ON o.OrderID = od.OrderID
    WHERE o.CustomerID = @CustomerID
    GROUP BY o.OrderNumber, o.OrderDate, o.OrderStatus, o.PaymentStatus, o.NetAmount
    ORDER BY o.OrderDate DESC;
END;
GO

-- 更新產品庫存
CREATE PROCEDURE sp_UpdateProductStock
    @ProductID INT,
    @Quantity INT,
    @Operation CHAR(1) -- 'A' for Add, 'S' for Subtract
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        IF @Operation = 'A'
            UPDATE Products SET StockQuantity = StockQuantity + @Quantity WHERE ProductID = @ProductID;
        ELSE IF @Operation = 'S'
        BEGIN
            DECLARE @CurrentStock INT;
            SELECT @CurrentStock = StockQuantity FROM Products WHERE ProductID = @ProductID;
            
            IF @CurrentStock < @Quantity
            BEGIN
                RAISERROR('庫存不足', 16, 1);
                RETURN;
            END
            
            UPDATE Products SET StockQuantity = StockQuantity - @Quantity WHERE ProductID = @ProductID;
        END
        
        COMMIT TRANSACTION;
        PRINT '庫存更新成功';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

PRINT '✓ 預存程序建立成功';
GO

-- =============================================
-- 建立觸發器 (Triggers)
-- =============================================

-- 訂單修改時自動更新 ModifiedDate
CREATE TRIGGER trg_Orders_UpdateModifiedDate
ON Orders
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Orders 
    SET ModifiedDate = GETDATE()
    WHERE OrderID IN (SELECT OrderID FROM inserted);
END;
GO

-- 客戶修改時自動更新 ModifiedDate
CREATE TRIGGER trg_Customers_UpdateModifiedDate
ON Customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Customers 
    SET ModifiedDate = GETDATE()
    WHERE CustomerID IN (SELECT CustomerID FROM inserted);
END;
GO

-- 產品修改時自動更新 ModifiedDate
CREATE TRIGGER trg_Products_UpdateModifiedDate
ON Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Products 
    SET ModifiedDate = GETDATE()
    WHERE ProductID IN (SELECT ProductID FROM inserted);
END;
GO

PRINT '✓ 觸發器建立成功';
GO

-- =============================================
-- 資料統計
-- =============================================
PRINT '';
PRINT '========================================';
PRINT '資料庫建立完成！';
PRINT '========================================';
PRINT '';

SELECT 
    '統計資訊' AS Category,
    (SELECT COUNT(*) FROM Customers) AS Customers,
    (SELECT COUNT(*) FROM Products) AS Products,
    (SELECT COUNT(*) FROM Orders) AS Orders,
    (SELECT COUNT(*) FROM OrderDetails) AS OrderDetails,
    (SELECT COUNT(*) FROM Employees) AS Employees;
GO

-- 顯示表格列表
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
GO

PRINT '';
PRINT '✓ 資料庫 TestSourceDB 建立完成';
PRINT '✓ 包含 5 個資料表、3 個檢視表、2 個預存程序、3 個觸發器';
PRINT '✓ 已插入測試資料：10 筆客戶、20 筆產品、10 筆訂單、19 筆訂單明細、10 筆員工';
PRINT '';
PRINT '下一步：';
PRINT '1. 檢查資料庫連線是否正常';
PRINT '2. 執行 SELECT * FROM vw_OrderSummary 查看訂單摘要';
PRINT '3. 準備開始資料庫轉移作業';
PRINT '';
