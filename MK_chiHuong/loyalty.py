import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Thông tin kết nối Oracle
oracle_user = 'FMV_HQ'
oracle_password = 'Ha63EA8ppPi'
oracle_host = '192.168.120.1'
oracle_port = 1521
oracle_sid = 'pdb'

# Tạo chuỗi kết nối Oracle với SQLAlchemy
dsn = f"oracle+cx_oracle://{oracle_user}:{oracle_password}@{oracle_host}:{oracle_port}/?service_name={oracle_sid}"
engine = create_engine(dsn)

print("Kết nối thành công đến Oracle Database")

# Tính toán ngày hiện tại và ngày 8 ngày trước
today = datetime.now()
# last_month = today - relativedelta(months=1)

# # Set the dates with the adjusted month

# Tính toán ngày hiện tại và ngày 16 của tháng trước
# start_date_str = last_month.replace(day=1).strftime("%Y-%m-%d")  # First day of the last month
# end_date_str = last_month.replace(day=15).strftime("%Y-%m-%d")   # 15th day of the last month
# end_date_str_1 = last_month.replace(day=16).strftime("%Y-%m-%d") 

# Tính toán ngày hiện tại và ngày 16 của tháng này
start_date_str = today.replace(day=1).strftime("%Y-%m-%d")
end_date_str = today.replace(day=15).strftime("%Y-%m-%d")
end_date_str_1 = today.replace(day=16).strftime("%Y-%m-%d")
# Truy vấn SQL
query = f"""
SELECT 
  to_char(
    NVL(
      mc.MEMBER_NYU_DAY, mc.CARD_ISSUED_DAY
    ), 
    'DD'
  ) AS "Ngày đăng ký", 
  to_char(
    NVL(
      mc.MEMBER_NYU_DAY, mc.CARD_ISSUED_DAY
    ), 
    'MONTH'
  ) AS "Tháng đăng ký", 
  mc.MEMBER_NAME AS "Họ Tên", 
  CASE mc.K_SEX WHEN 1 THEN 'Male' WHEN 2 THEN 'Female' ELSE 'Others' END AS "Giới tính", 
  mc.TEL AS "Số điện thoại", 
  mc.CARD_NO AS "Member ID", 
  CASE mc.THIS_RANK WHEN 'C' THEN 'GOLD' WHEN 'D' THEN 'SILVER' ELSE 'MEMBER' END AS "Rank", 
  to_char(rpm.HASEI_TIME, 'MM') AS "Tháng giao dịch", 
  to_char(rpm.HASEI_TIME, 'DD') AS "Ngày giao dịch", 
  to_char(rpm.HASEI_TIME, 'HH24:MI:SS') AS "Thời gian giao dịch", 
  to_char(rpm.CONO) || to_char(rpm.HASEI_TIME, 'HH24:MI:SS') AS BILL_NO, 
  rpm.C_TANPIN AS "Items Code", 
  rpm.C_TANPIN AS "Items ID", 
  rpm.C_CLASS AS "Categories ID", 
  rpm.QT AS "Số Lượng", 
  rpm.UNIT_PRICE AS "Giá 1 sản phẩm", 
  rpm.TORI_KIN AS "tổng giá 1 sp", 
  CASE WHEN weu.DISCOUNT_VALUE > 0 THEN weu.DISCOUNT_VALUE ELSE 0 END AS "Giá trị ECP sử dụng", 
  rpm.C_TENPO AS "Cửa hàng áp dụng", 
  FMV_POINT.GET_TUMITATE_POINT (mc.CARD_NO) AS "Số điểm còn lại" 
FROM 
  FMV_POINT.R_POINT_MEISAI rpm 
  INNER JOIN FMV_POINT.M_CUSTOMER mc ON mc.CARD_NO = rpm.CARD_NO 
  INNER JOIN FMV_POINT.R_POINT rp ON rp.HASEI_TIME = rpm.HASEI_TIME 
  AND rp.C_KIGYO = rpm.C_KIGYO 
  AND rp.C_TENPO = rpm.C_TENPO 
  AND rp.TMNO = rpm.TMNO 
  AND rp.CONO = rpm.CONO 
  AND rp.CARD_NO = rpm.CARD_NO 
  LEFT JOIN FMV_POINT.W_E_COUPON_USED weu ON weu.HASEI_TIME = rpm.HASEI_TIME 
  AND weu.C_KIGYO = rpm.C_KIGYO 
  AND weu.C_TENPO = rpm.C_TENPO 
  AND weu.TMNO = rpm.TMNO 
  AND weu.CONO = rpm.CONO 
  AND weu.CARD_NO = rpm.CARD_NO 
WHERE 
  rpm.HASEI_TIME BETWEEN TO_DATE('{start_date_str}', 'YYYY-MM-DD') AND TO_DATE('{end_date_str_1}', 'YYYY-MM-DD')
"""

# Đọc dữ liệu từ Oracle
print("Bắt đầu đọc dữ liệu từ Oracle...")
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ Oracle")
df_data.rename(columns={'bill_no': 'BILL_NO'}, inplace=True)

# Định dạng tên file
excel_filename = f'iPoint_Data_{start_date_str}_to_{end_date_str}.xlsx'

# Xuất dữ liệu ra file Excel
df_data.to_excel(excel_filename, index=False, engine='openpyxl')
print(f"Xuất dữ liệu thành công ra file {excel_filename}")
