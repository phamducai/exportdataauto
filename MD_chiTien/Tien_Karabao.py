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
  STr_SaleDtl.Trans_Date AS [Date],
  STr_SaleDtl.Stk_ID -1 AS Store,
  STr_SaleDtl.Goods_ID,
  Goods.Short_Name,
  Goods.Goods_Code AS Barcode,
  SUM ( STr_SaleDtl.SKUBase_Qty ) AS qty,
  SUM ( STr_SaleDtl.Sales_Amt ) AS Sales,
  SUM ( STr_SaleDtl.VAT_Amt ) AS VAT,
  SUM ( STr_SaleDtl.Sales_Amt + STr_SaleDtl.VAT_Amt ) AS Amount
FROM
  STr_SaleDtl
  INNER JOIN Goods ON STr_SaleDtl.Goods_ID = Goods.Goods_ID 
WHERE
  STr_SaleDtl.Trans_Date >= '{start_date_str}'
  AND STr_SaleDtl.Trans_Date < '{end_date_str}'
  AND STr_SaleDtl.Goods_ID = 224195
GROUP BY
  STr_SaleDtl.Trans_Date,
  STr_SaleDtl.Stk_ID,
  STr_SaleDtl.Goods_ID,
  Goods.Short_Name,
  Goods.Goods_Code
ORDER BY
  STr_SaleDtl.Trans_Date,
  STr_SaleDtl.Stk_ID   
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
csv_filename = f'Karabao_{start_date_str}_to_{end_date_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
