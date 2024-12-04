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

# Tính toán ngày đầu tiên và ngày cuối cùng của tháng trước
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = last_day_of_last_month.replace(day=1)

# Convert dates to string format
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")


# Truy vấn SQL
query = f"""
SELECT
  STr_SaleDtl.Trans_Date AS DATE,
  STr_SaleDtl.Stk_ID - 1 AS Store,
  Goods.Goods_ID AS Goods_ID,
  Goods.Short_Name AS Short_Name,
  Goods.Goods_Code AS Barcode,
  SUM ( STr_SaleDtl.SKUBase_Qty ) AS Qty,
  SUM ( STr_SaleDtl.Sales_Amt ) AS Sales,
  SUM ( STr_SaleDtl.VAT_Amt ) AS VAT,
  SUM ( STr_SaleDtl.VAT_Amt + STr_SaleDtl.Sales_Amt ) AS After_promotion,
  SUM ( STr_SaleDtl.Direct_Disc_Amt ) AS Discount,
  SUM ( STr_SaleDtl.VAT_Amt + STr_SaleDtl.Sales_Amt+ STr_SaleDtl.Direct_Disc_Amt ) AS Before_promotion 
FROM
  STr_SaleDtl
  JOIN Goods ON STr_SaleDtl.Goods_ID = Goods.Goods_ID 
WHERE
  Goods.Goods_ID IN (
    223032,
    162141,
    223031,
    223169,
    223177,
    223302,
    166615,
    224018,
    223179,
    221760,
    224009,
    222162,
    221364,
    221360,
    223028,
    221355,
    221361,
    223033,
    221757,
    162603,
    223171,
    223318,
    223178,
    166110,
    223320,
    223029,
    233069,
    221048,
    165109,
    222166,
    221356,
    221349,
    221216,
    223336,
    162604,
    167200,
    221346,
    221761,
    162602,
    221365,
    224189,
    221354,
    166601,
    223006,
    233068,
    223027,
    223321,
    223176,
    223157,
    224190,
    223314
  ) 
  AND STr_SaleDtl.Trans_Date >= '{start_date_str}' 
  AND STr_SaleDtl.Trans_Date <=  '{end_date_str}' 
GROUP BY
  STr_SaleDtl.Trans_Date,
  STr_SaleDtl.Stk_ID ,
  Goods.Goods_ID ,
  Goods.Short_Name ,
  Goods.Goods_Code
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
excel_filename = f'pepsi_{start_date_str}_to_{end_date_str}.xlsx'

# Xuất dữ liệu ra file Excel
df_data.to_excel(excel_filename, index=False, engine='openpyxl')

print("Xuất dữ liệu thành công từ SQL Server")
