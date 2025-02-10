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
try:
    engine = create_engine(connection_string)
    print("✅ Kết nối thành công đến SQL Server")
except Exception as e:
    print(f"❌ Lỗi kết nối SQL Server: {e}")
    exit()

# Tính toán khoảng thời gian từ thứ Hai tuần trước đến hết Chủ Nhật tuần trước
today = datetime.now() - timedelta(weeks=1)  
start_of_this_week = today - timedelta(days=today.weekday())  # Tìm thứ Hai của tuần hiện tại
start_of_last_week = start_of_this_week - timedelta(weeks=1)  # Tìm thứ Hai của tuần trước
end_of_last_week = start_of_this_week - timedelta(days=1)  # Chủ Nhật tuần trước

# Chuyển thành chuỗi "YYYY-MM-DD"
start_date = start_of_last_week.strftime("%Y-%m-%d")
end_date = end_of_last_week.strftime("%Y-%m-%d")

# Truy vấn SQL với biến ngày được truyền bằng `params`
query = """
SELECT
  b.Stk_ID - 1 AS StoreID,
  STK_Name AS StoreName,
  '"' + a.Trans_No AS Trans_No,
  CONVERT(datetime, LEFT(EfTran_Date, 12), 101) AS DATE,
  DATEPART(HOUR, CONVERT(TIME, EfTran_Date, 108)) AS HOUR,
  DATEPART(WEEK, EfTran_Date) AS Week,
  b.Goods_ID,
  Short_Name,
  Goods_Grp_ID,
  GGrp_Name,
  SUM(SKUBase_Qty) AS Qty,
  SUM(Sales_Amt + VAT_Amt) AS Amount,
  SUM(Direct_Disc_Amt) AS Discount,
  Request_Data AS Provider,
  Response_Data AS 'Ma van don'
FROM
  STr_Payment a
  JOIN STr_SaleDtl b ON a.Trans_No = b.Trans_No
  JOIN Stock c ON b.Stk_ID = c.Node_ID
  JOIN Goods d ON b.Goods_ID = d.Goods_ID
  JOIN GoodsGrp e ON d.Goods_Grp_ID = e.GGrp_ID 
WHERE
  a.Trans_No IN (
    SELECT Trans_No 
    FROM STr_SaleDtl 
    WHERE
      CONVERT(datetime, LEFT(EfTran_Date, 12), 101) BETWEEN ? AND ?
      AND Disabled = 0 
  ) 
  AND Pmt_ID = 12 
  AND Request_Data <> 'BTF' 
GROUP BY
  b.Stk_ID - 1,
  STK_Name,
  a.Trans_No,
  CONVERT(datetime, LEFT(EfTran_Date, 12), 101),
  DATEPART(HOUR, CONVERT(TIME, EfTran_Date, 108)),
  DATEPART(WEEK, EfTran_Date),
  b.Goods_ID,
  Short_Name,
  Goods_Grp_ID,
  GGrp_Name,
  Request_Data,
  Response_Data 
ORDER BY
  b.Stk_ID - 1,
  STK_Name,
  a.Trans_No,
  CONVERT(datetime, LEFT(EfTran_Date, 12), 101),
  DATEPART(HOUR, CONVERT(TIME, EfTran_Date, 108)),
  DATEPART(WEEK, EfTran_Date),
  b.Goods_ID,
  Short_Name,
  Goods_Grp_ID,
  GGrp_Name,
  Request_Data,
  Response_Data
"""

# ✅ **Sửa lỗi bằng cách truyền `params` đúng cách**
try:
    df_data = pd.read_sql(query, engine, params=(start_date, end_date))  # Không cần `[(start_date, end_date)]`
    print("✅ Đọc dữ liệu thành công từ SQL Server")
except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {e}")
    exit()

# Kiểm tra dữ liệu trước khi xuất Excel
if df_data.empty:
    print("⚠ Không có dữ liệu để xuất.")
else:
    excel_filename = f'delivery_{start_date}_to_{end_date}.xlsx'
    df_data.to_excel(excel_filename, index=False, engine='openpyxl')
    print(f"✅ Dữ liệu đã được xuất ra file: {excel_filename}")
