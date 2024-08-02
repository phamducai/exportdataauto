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

# Tính toán ngày đầu tiên của tháng trước và ngày đầu tiên của tháng hiện tại
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)

# Chuyển đổi ngày sang định dạng chuỗi
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = first_day_of_current_month.strftime("%Y-%m-%d")

# Truy vấn SQL
query = f"""
SELECT
  Trans_Date,
  Stk_ID - 1 AS Store_ID,
  Goods_ID,
  Sales_Amt,
  (Sales_Amt + VAT_Amt) AS Amount,
  SUM(SKUBase_Qty) AS Total_Qty
FROM
  STr_SaleDtl
WHERE
  Trans_Date >= '{start_date_str}'
  AND Trans_Date < '{end_date_str}'
  AND Goods_ID IN (147661, 147503, 219116, 219375, 219611)
GROUP BY
  Trans_Date,
  Stk_ID,
  Goods_ID,
  Sales_Amt,
  VAT_Amt;
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
csv_filename = f'Sales_Report_{start_date_str}_to_{end_date_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
