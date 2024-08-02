import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib

# Thông tin kết nối SQL Server
sqlserver_user = 'it'
sqlserver_password = 'Famima@123'
sqlserver_host = '192.168.10.16'
sqlserver_instance = 'POS_SQLSERVER'
sqlserver_db = 'DSMART90'

# Tạo chuỗi kết nối với URL encoding
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sqlserver_host}\\{sqlserver_instance};"
    f"DATABASE={sqlserver_db};"
    f"UID={sqlserver_user};"
    f"PWD={sqlserver_password};"
    "TrustServerCertificate=yes"
)

connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

# Tạo engine kết nối với SQL Server
engine = create_engine(connection_string)
print("Kết nối thành công đến SQL Server")

# Tính toán ngày hiện tại trừ 1 ngày
yesterday = datetime.now() - timedelta(days=1)
query_date_str = yesterday.strftime("%Y-%m-%d")

# Truy vấn SQL với CTE
query = f"""
WITH JQKA AS
(
    SELECT 
        Trans_Date,
        Stk_ID - 1 AS Store,
        Goods_Grp_ID AS Category,
        a.Goods_ID AS 'Item cd',
        Full_Name,
        SUM(CASE WHEN StkTrCls_ID = 3032 THEN (SKUBase_Qty) ELSE -1 * (SKUBase_Qty) END) AS Qty,
        SUM(CASE WHEN StkTrCls_ID = 3032 THEN (Sales_Amt + Direct_Disc_Amt) ELSE -1 * (Sales_Amt + Direct_Disc_Amt) END) AS Sales
    FROM 
        STr_SaleDtl a
    JOIN 
        Goods b ON a.Goods_ID = b.Goods_ID
    WHERE 
        Trans_Date = '{query_date_str}'  
        AND StkTrCls_ID IN (3032, 3036) 
        AND a.Disabled = 0
    GROUP BY 
        Trans_Date, 
        Stk_ID, 
        Goods_Grp_ID, 
        a.Goods_ID, 
        Full_Name
)
SELECT 
    Trans_Date,
    Store,
    Category,
    DENSE_RANK() OVER (PARTITION BY Category, Store ORDER BY Qty DESC) AS Rank,
    [Item cd],
    Qty,
    Sales 
FROM 
    JQKA
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Định dạng tên file
excel_filename = f'save_rank_{query_date_str}.xlsx'

# Xuất dữ liệu ra file Excel
df_data.to_excel(excel_filename, index=False, engine='openpyxl')
print(f"Dữ liệu đã được xuất ra file Excel: {excel_filename}")
