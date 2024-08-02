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
engine = create_engine(connection_string)
print("Kết nối thành công đến SQL Server")

# Xác định ngày hôm nay và 6 ngày trước
today = datetime.now().strftime("%Y-%m-%d")
old_date = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")

# Truy vấn SQL
query = f"""
SELECT 
    CONVERT(DATE, LEFT(EfTran_Date, 12), 101) AS DATE,
    Stk_ID - 1 AS store,
    dbo.DsTransNoFSPTransNo(a.trans_no) AS Trans_no,
    SUM(Payment_Amt) AS Amount,
    CASE
        WHEN ISJSON(Response_Data) = 1 THEN JSON_VALUE(Response_Data, '$.ResData.TrID') 
        ELSE NULL 
    END AS ItemName,
    CASE
        WHEN ISJSON(Response_Data) = 1 THEN JSON_VALUE(Response_Data, '$.ResData.OrNo') 
        ELSE NULL 
    END AS PayooTrans,
    CASE
        WHEN ISJSON(Response_Data) = 1 THEN JSON_VALUE(Response_Data, '$.ResData.PrId') 
        ELSE NULL 
    END AS Service,
    CASE
        WHEN ISJSON(Response_Data) = 0 AND CHARINDEX('-', Response_Data) > 0 THEN Response_Data 
        ELSE NULL 
    END AS Barcode,
    CASE
        WHEN ISJSON(Response_Data) = 1 THEN JSON_VALUE(Response_Data, '$.ResData.TrAmt') 
        ELSE NULL 
    END AS PaypooAtm
FROM
    STr_Payment a
    INNER JOIN STr_SaleDtl b ON a.Trans_No = b.Trans_No 
WHERE
    a.Trans_No IN (
        SELECT Trans_No 
        FROM STr_Hdr 
        WHERE StkTrCls_ID = 3033 
          AND Disabled = 0 
          AND EfTran_Date >= '{old_date}'
          AND EfTran_Date < '{today}'
    ) 
GROUP BY
    a.trans_no,
    EfTran_Date,
    Response_Data,
    Stk_ID 
ORDER BY
    EfTran_Date,
    a.Trans_No,
    Stk_ID ASC;
"""

# Đọc dữ liệu từ SQL Server
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Xuất dữ liệu ra file Excel
excel_filename = f'draft_{old_date}_to_{today}.csv'
df_data.to_csv(excel_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file Excel: {excel_filename}")
