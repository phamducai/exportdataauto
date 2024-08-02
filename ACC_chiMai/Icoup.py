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

# Lấy ngày mồng 1 của tháng trước và ngày mồng 1 của tháng hiện tại
today = datetime.now()
first_day_of_current_month = datetime(today.year, today.month, 1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = datetime(last_day_of_last_month.year, last_day_of_last_month.month, 1)

# Lưu trữ ngày mồng 1 của tháng hiện tại và ngày cuối cùng của tháng trước
first_day_of_current_month_str = first_day_of_current_month.strftime("%Y-%m-%d")
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")

# Truy vấn SQL
new_query = f"""
SELECT
  a.tran_date AS WorkingDate,
  b.ActualDate AS ef_date,
  b.trnsaction_no,
  a.id,
  a.PMT_code,
  b.TotalAmount AS SALESUM,
  a.amount AS CouponValues,
  b.TotalAmount - a.amount AS Diff,
  a.P_OrderID
FROM
  ten_det a
  INNER JOIN (
    SELECT
      tran_date AS WorkingDate,
      ef_date AS ActualDate,
      id,
      trnsaction_no,
      card_id AS Member_No,
      SUM(sales_price + vat_amt) AS TotalAmount
    FROM
      sle_trn
    WHERE
      tran_date >= '{start_date_str}'
      AND tran_date < '{first_day_of_current_month_str}'
    GROUP BY
      tran_date,
      ef_date,
      id,
      trnsaction_no,
      card_id
  ) b ON a.tran_num = b.trnsaction_no
WHERE
  a.PMT_code IN ('ICOUP','BTF','VOUCH','ECOUP')
"""

# Đọc dữ liệu từ MySQL với các trường được chỉ định lưu ở dạng text
print("Bắt đầu đọc dữ liệu từ MySQL...")
df_new_data = pd.read_sql(new_query, engine)
print("Đọc dữ liệu thành công từ MySQL")

# Thêm dấu nháy đơn trước các giá trị trong cột 'trnsaction_no' và 'P_OrderID'
file_name = f'Icoup__{start_date_str}_to_{end_date_str}.xlsx'


# Xuất toàn bộ dữ liệu ra file Excel với một sheet duy nhất
df_new_data.to_excel(file_name, index=False)
# Định dạng tên file
# Xuất dữ liệu ra file CSV
print(f'Dữ liệu đã được xuất ra file CSV: {df_new_data}')
