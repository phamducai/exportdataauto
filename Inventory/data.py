import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import time
import openpyxl

# Thông tin kết nối MariaDB local
db_user = 'root'
db_password = 'Simple123'
db_host = 'localhost'
db_name = 'tonkho'
db_port = 3306

# Mã hóa mật khẩu
encoded_password = urllib.parse.quote_plus(db_password)

# Tạo chuỗi kết nối với charset utf8mb4
connection_string = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"

# Tạo engine kết nối với MariaDB
engine = create_engine(connection_string, echo=True)
print("Kết nối thành công đến MariaDB local")

# Truy vấn để lấy danh sách các cửa hàng
store_query = text("SELECT store_code, store_name FROM store")
stores = pd.read_sql(store_query, engine)

# Vòng lặp qua từng cửa hàng
for index, store in stores.iterrows():
    store_code = store['store_code']
    store_name = store['store_name']
    
    # Truy vấn SQL cho từng cửa hàng
    query = text("""
    SELECT 
        :store_code AS `Mã Cửa Hàng`,
        :store_name AS `Tên cửa hàng`,
        products.product_code AS `Mã sản phẩm`, 
        " " AS `Tên Sản Phẩm`,
        products.unit_code AS `Đơn vị lưu kho`,
        CASE 
            WHEN item_report.`end Qty` IS NULL THEN 1
            WHEN item_report.`end Qty` <= 0 THEN 1
            ELSE CAST(REPLACE(item_report.`end Qty`, ',', '') AS DECIMAL(10,2))
        END AS `Số Lượng tồn kho`,
        0 AS `Hàng đi đường`,  
        CASE 
            WHEN item_report.`End Unit Cost` IS NULL THEN 1
            WHEN item_report.`End Unit Cost` = 0 THEN 1
            ELSE CAST(REPLACE(item_report.`End Unit Cost`, ',', '') AS DECIMAL(10,2))
        END AS `Giá Vốn`
    FROM
        products 
    LEFT JOIN 
        (SELECT * FROM item_report WHERE SUBSTRING_INDEX(`Store`, ':', 1) = :store_code) AS item_report
    ON 
        products.product_code = item_report.`Item Code`
    """)

    # Đọc dữ liệu từ MariaDB
    print(f"Bắt đầu đọc dữ liệu cho cửa hàng {store_code}...")
    df_data = pd.read_sql(query, engine, params={'store_code': store_code, 'store_name': store_name})
    print(f"Đọc dữ liệu thành công cho cửa hàng {store_code}")

    # Định dạng tên file
    excel_filename = f'tonkho_export_{store_code}.xlsx'

    # Xuất dữ liệu ra file Excel
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        df_data.to_excel(writer, index=False, sheet_name='Tồn kho')
    print(f"Dữ liệu đã được xuất ra file Excel: {excel_filename}")

    # Nghỉ 0.1 giây trước khi xử lý cửa hàng tiếp theo
    time.sleep(0.1)

print("Hoàn thành xuất dữ liệu cho tất cả các cửa hàng.")