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

# Tính toán ngày đầu tiên của tháng trước và ngày đầu tiên của tháng hiện tại
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

# Chuyển đổi ngày sang định dạng chuỗi
last_day_of_last_month_str = last_day_of_last_month.strftime("%Y-%m-%d")
print("Kết nối thành công đến SQL Server")
# Chuyển đổi ngày sang định dạng chuỗi
start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = first_day_of_current_month.strftime("%Y-%m-%d")
print({start_date_str},{end_date_str})
# Truy vấn SQL
query = f"""
WITH jqka AS (
    SELECT 
    CASE 
        WHEN Goods_Grp_ID IN (3002,3001,3199,3109,3499,3105,3003,3201,3202,3004,3099,3009,3401,3101,3299,3209,3104,3102) THEN 'ALCOHOL'
        WHEN Goods_Grp_ID IN (7301,7309,7302,7901,7206,7209,8103,7109,7102,7101,7202,7201) THEN 'BOOK,MAGAZINE'
        WHEN Goods_Grp_ID IN (602,1001,1002,1007) THEN 'BREAD'
        WHEN Goods_Grp_ID IN (2203,2107,2102,2304,2399,2303,2309,2301,2204,2299,2209,2202,2004,2201,2106,2402,2401,2109,2199) THEN 'CHILLED DRINK'
        WHEN Goods_Grp_ID IN (5305,5008,1088,5101,1102,5105,5303,5102,5103,10,1906,5001,5007,5004,5003,5099,5009,5002) THEN 'CONFECTIONARY'
        WHEN Goods_Grp_ID IN (407,408,409,403,499,404,4005,305,4010,2002,2001,4603,1501,1599,1509,8503,8502,8501,8509,4601,701,702,703,704,799) THEN 'DAILY FOOD'
        WHEN Goods_Grp_ID IN (4202,1499,1409,903,902,1403,1404,5304,904,999,905) THEN 'DESSERT'
        WHEN Goods_Grp_ID IN (107,4007,2105,157,2302,2499,2409,2404,2403,2009,2003,2005,2104,4004,2103) THEN 'DRINK'
        WHEN Goods_Grp_ID IN (699,603,604,1099,5199,5109,5302,1003,101,103,102,401,4604,1505,5399,5309,5301,1503,5201,4599,4501,154,153,5005,1504) THEN 'FAST FOOD'
        WHEN Goods_Grp_ID IN (106,1901,1903,1904,1907,1902,1909,1905,1999,156) THEN 'ICE CREAM'
        WHEN Goods_Grp_ID IN (109,405,406,4299,4209,159) THEN 'LUNCHI BOX'
        WHEN Goods_Grp_ID IN (1601,402,1305,4003,9504) THEN 'NON FOOD'
        WHEN Goods_Grp_ID IN (599,4502,4409,503,504,4403,501,502,4401,505,506,4402,507,508) THEN 'NOODLE'
        WHEN Goods_Grp_ID IN (201,202,203,299,207,208,209,210,211,204,205,206) THEN 'OMUSUBI'
        WHEN Goods_Grp_ID IN (7604,7203,8102,7903,8205,4006,3103,8101,129,108,2101,4002,4001,8709,1399,1309,1699,1609,8204,8203,7504,7503,7502,7501,7509,8703,8008,8005,8006,1,8603,7606,7601,8609,8402,8209,8206,8002,8303,8309,8003,8001,8011,7801,7609,7909,7803,7904,8009,7902,8704,8401,8806,8403,4109,7704,7605,7802,7806,8901,7805,8104,8301,7409,7402,7401,4201,7809,7403,8007,99,179,158,199,7706,8201,8302,7701,7702,7703,3301,3399,3309,8202,7905,7807,4011,7505,7602,8601,8902,7705,8602,7709,8808,8801,8809,8702,8804,8807,1706,1705,7804,8701,8109,8010,8409,8803,8802,7603,8004) THEN 'OTHERS'
        WHEN Goods_Grp_ID IN (8805,1913,1301,1502,149,4399,4301,4302,4309,4101,4103,4199,4102,1602,1302,4104,1911,4106,152,151,1603,1401,1402,5306,4204,1702,212,4699,4609,1704,1303,4008,1919,1912,4203,4105,4009,4099,1703,1799,1709,1304,4602,1701,5307) THEN 'PROCESS FOOD'
        WHEN Goods_Grp_ID IN (899,801,802,803,804) THEN 'SALAD'
        WHEN Goods_Grp_ID IN (601) THEN 'SANDWICH'
        WHEN Goods_Grp_ID IN (105,5006,5203,155,5202,5299,5209) THEN 'SIDE DISH, SNACK DISH'
        WHEN Goods_Grp_ID IN (901,1306,1004,5104,1005,1006,1101,1199,104) THEN 'SOFT CAKE'
        WHEN Goods_Grp_ID IN (304,399,301,302,303,306) THEN 'SUSHI'
        WHEN Goods_Grp_ID IN (6009,6002,6001) THEN 'TOBACCO'
        ELSE 'UNKNOWN' 
    END AS BigCategory,
    SUM(CASE 
        WHEN StkTrCls_ID = 3032 THEN 1 * Sales_Amt 
        WHEN StkTrCls_ID = 3032 THEN -1 * Sales_Amt 
        ELSE 0 
    END) AS 'TotalSales'
    FROM STr_SaleDtl a 
    JOIN Goods b ON a.goods_id = b.Goods_ID
    WHERE trans_date >= '{start_date_str}' AND trans_date < '{end_date_str}'
    GROUP BY Goods_Grp_ID
)
SELECT BigCategory, SUM(TotalSales) AS TotalSales 
FROM jqka
GROUP BY BigCategory
"""

# Đọc dữ liệu từ SQL Server
df_data = pd.read_sql(query, engine)
print("Đọc dữ liệu thành công từ SQL Server")

# Hiển thị dữ liệu
print(df_data)

# Xuất dữ liệu ra file Excel
excel_filename = f'Nganh_Hang_VFC_{start_date_str}_to_{last_day_of_last_month_str}.csv'

df_data.to_csv(excel_filename, index=False, encoding='utf-8-sig')
print(f"Dữ liệu đã được xuất ra file Excel: {excel_filename}")
