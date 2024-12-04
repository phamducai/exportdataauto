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

# Tính toán ngày bắt đầu và ngày kết thúc theo yêu cầu
today = datetime.now()
end_date = today - timedelta(days=1)  # Ngày hôm nay trừ đi 4 ngày
start_date = today - timedelta(days=28)  # Ngày hôm nay trừ đi 32 ngày
end_date_label = today - timedelta(days=1)

# Chuyển đổi ngày sang định dạng chuỗi
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
end_date_label_str = end_date_label.strftime("%Y-%m-%d")

# Truy vấn SQL
query = f"""
SELECT
  Trans_Date,
  Stk_ID - 1 AS Store_ID,
  Goods_ID,
  Sales_Amt,
  ( Sales_Amt + VAT_Amt ) AS Amount,
  SUM ( SKUBase_Qty ) AS Total_Qty 
FROM
  STr_SaleDtl 
WHERE
  Trans_Date >= '{start_date_str}' 
  AND Trans_Date <= '{end_date_str}' 
  AND Goods_ID IN (
    209304,
    209305,
    209306,
    211006,
    211007,
    211057,
    211058,
    221172,
    221174,
    221176,
    221178,
    221211,
    221212,
    221352,
    221353,
    254354,
    149996,
    241203,
    241204,
    241205,
    241206,
    241207,
    244915,
    244916,
    244923,
    244924,
    244925,
    245049,
    245050,
    245051,
    245116,
    245131,
    246472,
    246478,
    250494,
    251111,
    251164,
    251222,
    251225,
    252524,
    251098,
    251312,
    252552,
    251352,
    252060,
    252419,
    252464,
    252268,
    252283,
    252284,
    252418,
    252435,
    252525,
    252551,
    253188,
    253224,
    253225,
    253294,
    253369,
    220460,
    220461,
    220463,
    220497,
    220498,
    222469,
    241206,
    241207 
  ) 
GROUP BY
  Trans_Date,
  Stk_ID,
  Goods_ID,
  Sales_Amt,
  VAT_Amt
"""

# Đọc dữ liệu từ SQL Server
print("Bắt đầu đọc dữ liệu từ SQL Server...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
csv_filename = f'Fc_Report_{start_date_str}_to_{end_date_label_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
