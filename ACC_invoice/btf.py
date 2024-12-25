import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib

# SQL Server connection details
sqlserver_user = 'it'
sqlserver_password = 'Famima@123'
sqlserver_host = '192.168.10.16'
sqlserver_instance = 'POS_SQLSERVER'
sqlserver_db = 'DSMART90'

# SQL Server connection string with URL encoding
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sqlserver_host}\\{sqlserver_instance};"
    f"DATABASE={sqlserver_db};"
    f"UID={sqlserver_user};"
    f"PWD={sqlserver_password};"
    "TrustServerCertificate=yes"
)

connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

# Create SQL Server engine
engine = create_engine(connection_string)
print("Successfully connected to SQL Server")

# Calculate yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# SQL query using yesterday's date
query = f"""
SELECT
  a.Trans_Date,
  ExpStkClust_ID - 1 AS Store,
  Request_Data,
  Response_Data,
  Random_Code,
  SUM(Payment_Amt) AS Amount 
FROM
  STr_Payment a
  JOIN str_hdr b ON a.Trans_No = b.Trans_No 
WHERE
  a.Trans_Date = '{yesterday}'  -- Use yesterday's date
  AND pmt_id = 12 
  AND a.Disabled = 0 
GROUP BY
  a.Trans_Date,
  ExpStkClust_ID,
  Random_Code,
  Request_Data,
  Response_Data
"""

# Read data from SQL Server
df_data = pd.read_sql(query, engine)
print("Data successfully read from SQL Server")

# Display the data
print(df_data)
# Export data to Excel
excel_filename = f'BTF_{yesterday}.xlsx'
df_data.to_excel(excel_filename, index=False)
print(f"Data has been exported to the Excel file: {excel_filename}")

