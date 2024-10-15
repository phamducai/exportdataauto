import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib
import time
import openpyxl

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

# Xác định ngày đầu tiên của tháng trước và tháng này
today = datetime.now()
first_day_this_month = today.replace(day=1)
last_day_previous_month = first_day_this_month - timedelta(days=1)
first_day_previous_month = last_day_previous_month.replace(day=1)

start_date = first_day_previous_month
end_date = today

# Truy vấn SQL
query_template = """
SELECT
  dbo.DsTransNoFSPTransNo ( a.Trans_No ) AS tran_num,
  a.Trans_Date AS WorkingDate,
  CONVERT ( datetime, LEFT ( EfTran_Date, 12 ), 101 ) AS ActualDate,
  ExpStkClust_ID - 1 AS ID,
  SUM ( Payment_Amt ) AS Amt1,
  Member_No AS 'MemberID',
  131013 AS Account,
CASE
    WHEN ISJSON ( Response_Data ) = 1 THEN
    JSON_VALUE ( Response_Data, '$.ResData.ApCode' ) ELSE NULL 
  END AS 'P_tran_ID',
CASE
    WHEN ISJSON ( Response_Data ) = 1 THEN
    JSON_VALUE ( Response_Data, '$.ResData.TrDa' ) ELSE NULL 
  END AS 'P_tran_date',
CASE
    
    WHEN ISJSON ( Response_Data ) = 1 THEN
  CASE
      
      WHEN JSON_VALUE ( Response_Data, '$.ResData.DeID' ) IS NOT NULL 
      AND JSON_VALUE ( Response_Data, '$.ResData.TrID' ) IS NOT NULL THEN
        CONCAT ( JSON_VALUE ( Response_Data, '$.ResData.DeID' ), JSON_VALUE ( Response_Data, '$.ResData.TrID' ) ) ELSE JSON_VALUE ( Response_Data, '$.ResData.CaNum' ) 
      END ELSE NULL 
    END AS 'P_Order_ID',
  CASE
      WHEN ISJSON ( Response_Data ) = 1 THEN
    CASE
        WHEN LEFT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 1 ) = '3' THEN
        CONCAT ( RIGHT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4 ), '_JCB' ) 
        WHEN LEFT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 1 ) = '4' THEN
        CONCAT ( RIGHT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4 ), '_VISA' ) 
        WHEN LEFT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 1 ) = '5' THEN
        CONCAT ( RIGHT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4 ), '_MASTE' ) 
        WHEN LEFT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 1 ) = '6' THEN
        CONCAT ( RIGHT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4 ), '_UNION' ) 
        WHEN LEFT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 1 ) = '9' THEN
        CONCAT ( RIGHT ( JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4 ), '_ATM' )
        ELSE CONCAT(RIGHT(JSON_VALUE ( Response_Data, '$.ResData.CaNum' ), 4), '_CARD')
    END ELSE NULL 
    END AS 'PMT_ID',
a.Trans_No as Trans_No
FROM
  STr_Payment a
  JOIN STr_Hdr b ON a.Trans_No= b.Trans_No 
WHERE 
  CAST(b.EfTran_Date AS DATE) = :query_date
  AND Pmt_ID = 3 
GROUP BY
  a.Trans_No,
  a.Trans_Date,
  EfTran_Date,
  ExpStkClust_ID,
  Member_No,
  Request_Data,
  Response_Data
"""

def execute_query_with_retry(query, params, max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            return pd.read_sql(text(query), engine, params=params)
        except Exception as e:
            if "deadlock" in str(e).lower() and attempt < max_retries - 1:
                print(f"Deadlock detected. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise

df_list = []
current_date = start_date

while current_date < end_date:
    query_params = {'query_date': current_date.strftime("%Y-%m-%d")}
    
    print(f"Đang truy vấn dữ liệu cho ngày {query_params['query_date']}")
    
    try:
        day_df = execute_query_with_retry(query_template, query_params)
        df_list.append(day_df)
        print(f"Truy vấn thành công cho ngày {query_params['query_date']}")
    except Exception as e:
        print(f"Lỗi khi truy vấn ngày {query_params['query_date']}: {str(e)}")
    
    current_date += timedelta(days=1)
    time.sleep(1)  # Tạm dừng 1 giây giữa các truy vấn để giảm tải cho server

# Kết hợp tất cả các DataFrame
df_data = pd.concat(df_list, ignore_index=True)
df_data['Trans_No'] = "'" + df_data['Trans_No'].astype(str)

print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Đặt tên file dựa trên tháng trước
previous_month = (today.replace(day=1) - timedelta(days=1)).month
excel_filename = f'Card_T{previous_month}.xlsx'

with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    df_data.to_excel(writer, index=False, sheet_name='Sheet1')
    
    # Định dạng cột Trans_No là text
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    column_letter = openpyxl.utils.get_column_letter(df_data.columns.get_loc('Trans_No'))
    for cell in worksheet[column_letter][1:]:  # Bỏ qua hàng tiêu đề
        cell.number_format = '@'

print(f"Dữ liệu đã được xuất ra file Excel: {excel_filename}")