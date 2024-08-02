import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
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

# Lấy ngày mồng 1 của tháng trước và ngày mồng 2 của tháng này
today = datetime.now()
first_day_of_last_month = datetime(today.year, today.month - 1, 1)
second_day_of_this_month = datetime(today.year, today.month, 2)

# Định dạng tên file
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = second_day_of_this_month.strftime("%Y-%m-%d")

# Truy vấn SQL
query = f"""
SELECT
    a.tran_num,
    a.tran_date AS WorkingDate,
    b.ActualDate,
    a.id,
    a.PMT_code,
    SUM(a.amount) AS amt1,
    CASE
        WHEN b.Member_No <> 'F' THEN CONCAT('000', b.Member_No) ELSE b.Member_No
    END AS Member_No,
    CASE
        WHEN a.PMT_code = 'ATM' THEN '131013'
        WHEN a.PMT_code = 'JCB' THEN '131013'
        WHEN a.PMT_code = 'CARDS' THEN '131013'
        WHEN a.PMT_code = 'MASTE' THEN '131013'
        WHEN a.PMT_code = 'MOMO' THEN '131031'
        WHEN a.PMT_code = 'MOCA' THEN '131034'
        WHEN a.PMT_code = 'SHPEE' THEN '131036'
        WHEN a.PMT_code = 'ZALOPAY' THEN '131035'
        WHEN a.PMT_code = 'ZALO' THEN '131035'
        WHEN a.PMT_code = 'VISA' THEN '131013'
        WHEN a.PMT_code = 'QRC' THEN '131032'
        WHEN a.PMT_code = 'VNPay' THEN '131032'
        WHEN a.PMT_code = 'OTHER' THEN '131032'
        WHEN a.PMT_code = 'OWNCP' THEN '131018'
        WHEN a.PMT_code = 'ECOUP' THEN '131018'
        WHEN a.PMT_code = 'ICOUP' THEN '131018'
        WHEN a.PMT_code = 'GVC' THEN '131017'
        WHEN a.PMT_code = 'EGVC' THEN '131017'
        WHEN a.PMT_code = 'VOUCH' THEN '131017'
        WHEN a.PMT_code = 'BTF' THEN '131017'
        WHEN a.PMT_code = 'POINT' THEN '131019'
        ELSE '131013'
    END AS ACCOUNT,
    a.P_tran_date,
    a.P_tran_time,
    a.P_tran_ID,
    a.P_OrderID
FROM
    (SELECT tran_num, tran_date, id, PMT_code, amount, P_tran_date, P_tran_time, P_tran_ID, P_OrderID FROM ten_det WHERE PMT_code NOT IN ('CASH', 'DEPOS')) a
    INNER JOIN (
        SELECT
            tran_date AS WorkingDate,
            ef_date AS ActualDate,
            id,
            trnsaction_no,
            card_id AS Member_No,
            SUM(sales_price + vat_amt) AS TotalAmount
        FROM sle_trn
        WHERE
            ef_date BETWEEN '{start_date_str}' AND '{end_date_str}'
        GROUP BY
            tran_date,
            ef_date,
            id,
            trnsaction_no,
            card_id
    ) b ON a.tran_num = b.trnsaction_no
GROUP BY
    a.tran_num,
    a.tran_date,
    b.ActualDate,
    a.id,
    a.PMT_code,
    Member_No,
    a.P_tran_date,
    a.P_tran_time,
    a.P_tran_ID,
    a.P_OrderID
"""

# Đọc dữ liệu từ MySQL
print("Bắt đầu đọc dữ liệu từ MySQL...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ MySQL")

# Định dạng tên file
csv_filename = f'noncash_{start_date_str}_to_{end_date_str}.csv'

# Xuất dữ liệu ra file CSV
df_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file CSV: {csv_filename}")
