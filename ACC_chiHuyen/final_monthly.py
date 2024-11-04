import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib.parse

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

# Tính toán ngày đầu tiên của tháng trước và ngày mồng 2 của tháng này
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)
second_day_of_current_month = today.replace(day=2)

# Chuyển đổi ngày sang định dạng chuỗi
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = second_day_of_current_month.strftime("%Y-%m-%d")

# Truy vấn SQL mới
query_additional = f"""
SELECT tran_date, tran_time, ef_date, trnsaction_no, id, goods_id, sales_price, Barcode, itemname, Services_name
FROM sle_trn a JOIN payoo ON Barcode=Services_code
WHERE ef_date BETWEEN '{start_date_str}' AND '{end_date_str}' AND trans_code=23
ORDER BY tran_date, ef_date, trnsaction_no
"""

# Đọc dữ liệu từ MySQL cho truy vấn mới
print("Bắt đầu đọc dữ liệu từ MySQL cho truy vấn mới...")
df_data_additional = pd.read_sql(query_additional, engine)
print("Đọc dữ liệu thành công từ MySQL cho truy vấn mới")

# Hiển thị dữ liệu
print(df_data_additional)

# Định dạng tên file
csv_filename = f'final_monthly_{start_date_str}_to_{end_date_str}.xlsx'

# Xuất dữ liệu ra file excel
df_data_additional.to_excel(csv_filename, index=False)
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
