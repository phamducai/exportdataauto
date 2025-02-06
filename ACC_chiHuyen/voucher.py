import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
from dateutil.relativedelta import relativedelta

# Thông tin kết nối MySQL
mysql_user = 'AIIT'
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

# Tính toán ngày mồng 1 của tháng trước và ngày mồng 2 của tháng này

today = datetime.now() - relativedelta(months=0)

# Ngày mùng 1 của tháng hiện tại
first_day_of_current_month = today.replace(day=1)

# Ngày mùng 1 của tháng trước
first_day_of_last_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

# Ngày mùng 2 của tháng hiện tại
second_day_of_current_month = first_day_of_current_month + timedelta(days=1)

# Chuyển đổi ngày sang định dạng chuỗi
first_day_of_last_month_str = first_day_of_last_month.strftime("%Y-%m-%d")
first_day_of_current_month_str = first_day_of_current_month.strftime("%Y-%m-%d")
second_day_of_current_month_str = second_day_of_current_month.strftime("%Y-%m-%d")

# Truy vấn SQL mới sử dụng parameterized query
query = text(f"""
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
        tran_date BETWEEN :first_day_of_last_month AND :second_day_of_current_month
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
  a.ef_date >= :first_day_of_last_month AND a.ef_date < :first_day_of_current_month
GROUP BY
  a.tran_date,
  a.ef_date,
  a.trnsaction_no,
  a.id,
  b.PMT_code,
  b.P_OrderID,
  b.Amount1;
""")

# Đọc dữ liệu từ MySQL với các tham số được truyền vào truy vấn
print("Bắt đầu đọc dữ liệu từ MySQL...")
df_data = pd.read_sql(query, engine, params={
    'first_day_of_last_month': first_day_of_last_month_str,
    'second_day_of_current_month': second_day_of_current_month_str,
    'first_day_of_current_month': first_day_of_current_month_str
})
print("Đọc dữ liệu thành công từ MySQL")

# Hiển thị dữ liệu
print(df_data)

# Định dạng tên file
csv_filename = f'voucher_{first_day_of_last_month_str}_to_{first_day_of_current_month_str}.xlsx'

# Xuất dữ liệu ra file Excel
df_data.to_excel(csv_filename, index=False)
print(f"Dữ liệu đã được xuất ra file Excel: {csv_filename}")
