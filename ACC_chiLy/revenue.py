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

# Tính toán ngày mồng 1 tháng trước và ngày mồng 1 tháng này
# Tính toán ngày đầu và cuối của tháng trước
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

# Chuyển đổi ngày sang định dạng chuỗi
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")

# Truy vấn SQL
query = f"""
WITH STr_SaleDtl_CTE AS (
    SELECT 
        Stk_ID AS Stk_ID,
        DATENAME(MONTH, (ssd.Trans_Date)) AS 'Month Name',
        COUNT(DISTINCT(DAY(ssd.Trans_Date))) AS 'Num OP days',
        COUNT(DISTINCT(CASE WHEN ssd.StkTrCls_ID = 3032 THEN (ssd.Trans_No) END)) AS 'Num Cus',
        (SUM(CASE WHEN ssd.StkTrCls_ID = 3032 THEN (Sales_Amt + Direct_Disc_Amt) 
                  WHEN ssd.StkTrCls_ID = 3036 THEN -1 * (Sales_Amt + Direct_Disc_Amt) ELSE 0 END)) / 
        COUNT(DISTINCT(DAY(ssd.Trans_Date))) AS 'ADS Before dis',
        SUM(CASE WHEN ssd.StkTrCls_ID = 3032 THEN (Sales_Amt + Direct_Disc_Amt) 
                 WHEN ssd.StkTrCls_ID = 3036 THEN -1 * (Sales_Amt + Direct_Disc_Amt) ELSE 0 END) AS 'Sale Before dis',
        SUM(CASE WHEN ssd.StkTrCls_ID = 3032 THEN VAT_Amt 
                 WHEN ssd.StkTrCls_ID = 3036 THEN -1 * (VAT_Amt) ELSE 0 END) AS VAT,
        SUM(CASE WHEN ssd.StkTrCls_ID = 3032 THEN Direct_Disc_Amt 
                 WHEN ssd.StkTrCls_ID = 3036 THEN -1 * (Direct_Disc_Amt) ELSE 0 END) AS Dis_Direct,
        SUM(CASE WHEN ssd.StkTrCls_ID = 3032 THEN (Sales_Amt) 
                 WHEN ssd.StkTrCls_ID = 3036 THEN -1 * (Sales_Amt) ELSE 0 END) AS Sales_Amt
    FROM STr_SaleDtl ssd 
    WHERE ssd.Trans_Date >= '{start_date_str}' AND ssd.Trans_Date < '{end_date_str}' AND ssd.Disabled = 0
    GROUP BY Stk_ID, DATENAME(MONTH, (ssd.Trans_Date))
),
Payment_CTE AS (
    SELECT DISTINCT(spm.Trans_No),
           spm.Pmt_ID,
           spm.Payment_Amt,
           Stk_ID
    FROM STr_Payment spm 
    INNER JOIN STr_SaleDtl ssd ON spm.Trans_No = ssd.Trans_No
    WHERE spm.Trans_Date >= '{start_date_str}' AND ssd.Trans_Date < '{end_date_str}' 
          AND spm.Disabled = 0 AND Pmt_ID IN (4)
),
Payment1_CTE(Stk_ID, ttPayment_Amt) AS (
    SELECT Stk_ID, SUM(Payment_Amt) 
    FROM Payment_CTE
    GROUP BY Stk_ID
)
SELECT 
    ssd.Stk_ID - 1 AS store,
    [Month Name],
    [Num OP days],
    [Num Cus],
    [ADS Before dis],
    [Sale Before dis],
    VAT,
    Dis_Direct,
    ttPayment_Amt AS Dis_Ecoupon,
    Sales_Amt - ttPayment_Amt AS 'Sale of Mer (After Dis)',
    Dis_Direct + ttPayment_Amt AS 'no_name'
FROM STr_SaleDtl_CTE ssd 
INNER JOIN Payment1_CTE pmt1 ON ssd.Stk_ID = pmt1.Stk_ID
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
excel_filename = f'data_revenue_{start_date_str}_to_{end_date_str}.xlsx'

# Xuất dữ liệu ra file Excel
df_data.to_excel(excel_filename, index=False, engine='openpyxl')

print(f"Xuất dữ liệu thành công ra file {excel_filename}")
