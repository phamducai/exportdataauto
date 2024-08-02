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

# Truy vấn SQL
query = """
SELECT 
    tran_date, 
    tran_time, 
    ef_date, 
    trnsaction_no, 
    trans_code, 
    id, 
    goods_id, 
    Category, 
    Vendor_Code, 
    qty, 
    (sales_price + vat_amt) AS Amount,
    discount,
    Retail_Price,
    CAST(card_id AS CHAR) AS card_id,  -- Chuyển đổi card_id thành chuỗi
    itemname 
FROM
    sle_trn a
    JOIN itm_mas ON a.goods_id = Item_Code 
WHERE
    ef_date >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH,'%%Y-%%m-25')
    AND ef_date < DATE_FORMAT(CURDATE() ,'%%Y-%%m-01')
    AND Vendor_Code IN (30421, 30384, 30541, 30547, 30557)
ORDER BY
     ef_date
"""

# Đọc dữ liệu từ MySQL
print("Bắt đầu đọc dữ liệu từ MySQL...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ MySQL")

# Hiển thị dữ liệu
print(df_data)

# Tính toán ngày 26 của tháng trước và ngày đầu tiên của tháng này
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
previous_month = first_day_of_current_month - timedelta(days=1)
twenty_sixth_last_month = previous_month.replace(day=25)

# Định dạng tên file
start_date_str = twenty_sixth_last_month.strftime("%Y-%m-%d")
end_date_str = first_day_of_current_month.strftime("%Y-%m-%d")
csv_filename = f'du_lieu_ban_hang_{start_date_str}_to_{end_date_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
