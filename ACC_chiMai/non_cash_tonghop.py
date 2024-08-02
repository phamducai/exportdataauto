import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib.parse

# Thông tin kết nối đến cơ sở dữ liệu MySQL
user = 'IT'
password = 'Simple123'
host = '192.168.10.35'
database = 'vfm_db'
port = 3306

# Mã hóa mật khẩu và tạo chuỗi kết nối
encoded_password = urllib.parse.quote_plus(password)
connection_str = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"

# Khởi tạo engine để kết nối với MySQL
engine = create_engine(connection_str, echo=True)
print("Đã kết nối thành công với MySQL")

# Tính toán ngày mùng 1 của tháng trước và ngày cuối cùng của tháng trước
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
last_day_of_previous_month_date = first_day_of_current_month - timedelta(days=1)
first_day_of_previous_month = last_day_of_previous_month_date.replace(day=1)

# Chuyển đổi các ngày thành chuỗi để sử dụng trong truy vấn SQL
start_date_str = first_day_of_previous_month.strftime('%Y-%m-%d')
end_date_str = last_day_of_previous_month_date.strftime('%Y-%m-%d')

# Truy vấn SQL để lấy dữ liệu
sql_query = f"""
SELECT
	tran_date AS WorkingDate,
	ActualDate,
	a.id,
	PMT_code,
	SUM(amount) AS amt1,
	(
		CASE
			WHEN PMT_code = "ATM" THEN '131013' 
			WHEN PMT_code = "JCB" THEN '131013' 
			WHEN PMT_code = "CARDS" THEN '131013' 
			WHEN PMT_code = "MASTE" THEN '131013' 
			WHEN PMT_code = "MOMO" THEN '131031' 
			WHEN PMT_code = "MOCA" THEN '131034' 
			WHEN PMT_code = "SHPEE" THEN '131036' 
			WHEN PMT_code = "ZALOPAY" THEN '131035' 
			WHEN PMT_code = "ZALO" THEN '131035' 
			WHEN PMT_code = "VISA" THEN '131013' 
			WHEN PMT_code = "QRC" THEN '131032' 
			WHEN PMT_code = "VNPay" THEN '131032' 
			WHEN PMT_code = "OTHER" THEN '131032' 
			WHEN PMT_code = "OWNCP" THEN '131018' 
			WHEN PMT_code = "ECOUP" THEN '131018' 
			WHEN PMT_code = "ICOUP" THEN '131018' 
			WHEN PMT_code = "GVC" THEN '131017' 
			WHEN PMT_code = "EGVC" THEN '131017' 
			WHEN PMT_code = "VOUCH" THEN '131017' 
			WHEN PMT_code = "BTF" THEN '131017' 
			WHEN PMT_code = "POINT" THEN '131019' 
			ELSE '131013' 
		END 
	) AS ACCOUNT
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
			AND tran_date <= '{end_date_str}'
		GROUP BY
			tran_date,
			ef_date,
			id,
			trnsaction_no,
			card_id 
	) b ON tran_num = trnsaction_no 
WHERE
	PMT_code NOT IN ('CASH', 'DEPOS') 
GROUP BY
	tran_date,
	ActualDate,
	a.id,
	PMT_code
"""

# Đọc dữ liệu từ cơ sở dữ liệu MySQL và lưu vào DataFrame
print("Đang lấy dữ liệu từ cơ sở dữ liệu...")
data_frame = pd.read_sql(sql_query, engine)
print("Dữ liệu đã được lấy thành công")

# Định dạng tên file Excel để lưu kết quả
file_name = f'noncash_chitiet_{start_date_str}_to_{end_date_str}.xlsx'

# Giới hạn số hàng tối đa của một sheet
max_rows_per_sheet = 1048000

# Xuất dữ liệu ra file Excel với nhiều sheet nếu cần
with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
    for idx in range(0, len(data_frame), max_rows_per_sheet):
        chunk = data_frame.iloc[idx:idx + max_rows_per_sheet]
        sheet_name = f'Sheet_{idx // max_rows_per_sheet + 1}'
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"Dữ liệu đã được lưu thành công vào file: {file_name}")
