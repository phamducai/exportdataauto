import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib.parse

# Thông tin kết nối MySQL
mysql_user = 'IT'
mysql_password = 'Simple123'
mysql_host = '192.168.10.35'
mysql_db = 'vfm_db'
mysql_port = 3306

# Mã hóa mật khẩu
encoded_password = urllib.parse.quote_plus(mysql_password)

# Tạo chuỗi kết nối với pymysql
connection_string = f"mysql+pymysql://{mysql_user}:{encoded_password}@{mysql_host}:{mysql_port}/{mysql_db}"

# Tạo engine kết nối với MySQL
engine = create_engine(connection_string, echo=True)
print("Kết nối thành công đến MySQL")

# Tính toán ngày đầu tiên của tháng trước và ngày hiện tại
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
  a.tran_date,
  a.ef_date,
  a.trnsaction_no,
  a.id,
  b.PMT_code,
  CASE
    WHEN b.P_OrderID LIKE '25%%' THEN 'IPOINT_COUPON'
    WHEN b.P_OrderID LIKE '98%%' THEN 'PAYOO_COUPON'
    ELSE 'UNKNOWN'
  END AS CouponType,
  b.P_OrderID,
  SUM(a.sales_price + a.vat_amt) AS SalesSum,
  b.Amount1 AS CouponValues,
  (SUM(a.sales_price + a.vat_amt) - b.Amount1) AS Diff
FROM
  sle_trn a
  INNER JOIN (
    SELECT
      tran_date,
      tran_num,
      tran_code,
      PMT_code,
      P_OrderID,
      SUM(amount) AS Amount1
    FROM (
      SELECT DISTINCT
        tran_date,
        tran_num,
        tran_code,
        PMT_code,
        P_OrderID,
        amount
      FROM
        ten_det
      WHERE
        tran_date BETWEEN DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%%Y-%%m-01') - INTERVAL 1 DAY AND CURDATE() + INTERVAL 1 DAY
        AND PMT_code IN ('ICOUP', 'ECOUP', 'VOUCH')
    ) AS distinct_tendet
    GROUP BY
      tran_date,
      tran_num,
      tran_code,
      PMT_code,
      P_OrderID
  ) b ON a.trnsaction_no = b.tran_num
WHERE
  a.ef_date BETWEEN '{start_date_str}' AND '{end_date_str}'
GROUP BY
  a.tran_date,
  a.ef_date,
  a.trnsaction_no,
  a.id,
  b.PMT_code,
  b.P_OrderID,
  b.Amount1;
"""

# Đọc dữ liệu từ MySQL
print("Bắt đầu đọc dữ liệu từ MySQL...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ MySQL")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
csv_filename = f'voucher_{start_date_str}_to_{end_date_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
