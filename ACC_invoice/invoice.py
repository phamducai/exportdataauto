import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# MySQL connection details
mysql_user = 'IT'
mysql_password = 'Simple123'
mysql_host = '192.168.10.35'
mysql_db = 'vfm_db'
mysql_port = 3306

# Encode the MySQL password
encoded_password = urllib.parse.quote_plus(mysql_password)

# MySQL connection string
mysql_connection_string = f"mysql+pymysql://{mysql_user}:{encoded_password}@{mysql_host}:{mysql_port}/{mysql_db}"
mysql_engine = create_engine(mysql_connection_string, echo=True)

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
sqlserver_connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
sqlserver_engine = create_engine(sqlserver_connection_string, echo=True)
days_to_subtract = 0
current_date =(datetime.now() - timedelta(days=days_to_subtract)).strftime("%Y-%m-%d") # Format: YYYY-MM-DD for SQL queries
current_date_formatted = (datetime.now() - timedelta(days=days_to_subtract)).strftime("%Y/%m/%d")  # Format: YYYY/MM/DD for the stored procedure
current_invoice_date = (datetime.now() - timedelta(days=days_to_subtract)).strftime("%Y%m%d")  # Format: YYYYMMDD for the stored procedure
# Function to fetch data from MySQL
def fetch_from_mysql():
    query = f"""
    SELECT tran_date as date, id, ROUND( SUM( CASE WHEN trans_code = 21 THEN sales_price ELSE -1 * (sales_price) END ), 0 ) AS sales 
    FROM sle_trn 
    WHERE tran_date = '{current_date}'
    AND trans_code IN (21, 22) 
    GROUP BY tran_date, id
    """
    with mysql_engine.connect() as conn:
        df_data = pd.read_sql(query, conn)
    return df_data

def fetch_from_sqlserver():
    query = f"""
    SELECT Trans_Date as date, Stk_ID - 1 AS id, 
    ROUND(SUM(CASE WHEN StkTrCls_ID = 3032 THEN (Sales_Amt) WHEN StkTrCls_ID = 3036 THEN -1 * (Sales_Amt) ELSE 0 END), 0) AS sales
    FROM STr_SaleDtl 
    WHERE Disabled = 0 
    AND Trans_Date = '{current_date}' 
    GROUP BY Trans_Date, Stk_ID 
    ORDER BY Trans_Date, Stk_ID ASC
    """
    with sqlserver_engine.connect() as conn:
        df_data = pd.read_sql(query, conn)
    return df_data
# Function to execute stored procedure on SQL Server
def execute_stored_procedure():
    with sqlserver_engine.connect() as conn:
        stored_procedure = text("EXEC CreateEInv_Clone :date1, :date2")
        result = conn.execute(stored_procedure, {'date1': current_date_formatted, 'date2': current_date_formatted})
        df_data = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df_data

# Strategy interface for processing data
class DataProcessingStrategy:
    def process(self, arr1, arr2, arr3, differences):
        raise NotImplementedError

class FindDifferencesStrategy(DataProcessingStrategy):
    def process(self, arr1, arr2, arr3, differences):
        base_map = {item['id']: item['sales'] for item in arr1}
        differences_map = {}

        # Process arr3 first (higher priority)
        for item in arr3:
            if item['id'] in base_map:
                difference = base_map[item['id']] - item['sales']
                if difference > 5 or difference < -5:
                    differences_map[item['id']] = {
                        'date': item['date'],
                        'id': item['id'],
                        'sales': item['sales'],
                        'salesDifference': difference
                    }

        # Process arr2 if not already in differences_map
        for item in arr2:
            if item['id'] in base_map and item['id'] not in differences_map:
                difference = base_map[item['id']] - item['sales']
                if difference > 5 or difference < -5:
                    differences_map[item['id']] = {
                        'date': item['date'],
                        'id': item['id'],
                        'sales': item['sales'],
                        'salesDifference': difference
                    }

        return list(differences_map.values())
class CreateFilesStrategy(DataProcessingStrategy):
    def process(self, arr1, arr2, arr3, differences):
        self.export_stored_procedure_data()
        print('File created successfully.')

    def export_stored_procedure_data(self):
        with sqlserver_engine.connect() as conn:
            result = conn.execute(text("EXEC CreateEInv :date1, :date2"), {'date1': current_date_formatted, 'date2': current_date_formatted})
            df_data = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # Apply trimming and export to CSV in a single chain
            filename = f'{current_invoice_date}_eInvoice.csv'
            df_data.applymap(lambda x: x.strip() if isinstance(x, str) else x) \
                   .to_csv(filename, index=False, header=False, sep=',', encoding='utf-8')

            print(f"Data exported to file: {filename}")

class SendEmailStrategy(DataProcessingStrategy):
    def __init__(self, emails):
        self.emails = emails

    def process(self, arr1, arr2, arr3, differences):
        # Determine the type of email based on the data
        issue_type = 'differences' if len(arr1) == len(arr2) == len(arr3) else 'missing'

        if not differences and issue_type == 'differences':
            print('No discrepancies found. Email not sent.')
            return

        body = self._construct_email_body(differences, issue_type)
        self._send_email(body)

    def _construct_email_body(self, differences, issue_type):
        if issue_type == 'differences':
            discrepancy_list = "\n".join(
                [f"Store ID: {item['id']}, Date: {item['date']}, Sales Difference: {item['salesDifference']}"
                 for item in differences]
            )
            body = f"""
            <p>Dear Team,</p>
            <p>The following discrepancies were found in the sales data:</p>
            <pre>{discrepancy_list}</pre>
            <p>Please review and take necessary actions.</p>
            """
        elif issue_type == 'missing':
            missing_list = "\n".join(
                [f"Store ID: {item['id']}" for item in differences]
            )
            body = f"""
            <p>Dear An Sàn,</p>
            <p>Cửa hàng trong mysql thiếu:</p>
            <pre>{missing_list}</pre>
            <p>cứu em.</p>
            <p>Ái</p>
            <p>Thanks,</p>
            """
        return body

    def _send_email(self, body):
        smtp_server = 'mail.famima.vn'
        smtp_port = 465
        smtp_user = 'no-reply@famima.vn'
        smtp_pass = 'JQKA@2222'

        subject = 'Discrepancy Report for Sales Data'
        message = MIMEMultipart()
        message['From'] = smtp_user
        message['To'] = ', '.join(self.emails)
        message['Subject'] = subject
        message.attach(MIMEText(body, 'html'))

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, self.emails, message.as_string())
            print('Email sent successfully.')

# Context class for using strategies
class DataProcessor:
    def __init__(self, strategy: DataProcessingStrategy):
        self._strategy = strategy

    def process_data(self, arr1, arr2, arr3, differences):
        # Pass all required parameters to the strategy's process method
        self._strategy.process(arr1, arr2, arr3, differences)

# Function to find differences or missing IDs
def find_different_sales_or_missing_ids(arr1, arr2, arr3):
    if len(arr1) == len(arr2) == len(arr3):
        # Lengths are equal; find differences
        strategy = FindDifferencesStrategy()
        differences = strategy.process(arr1, arr2, arr3, [])
        return 'differences', differences
    else:
        # Lengths are not equal; find missing IDs
         arr1_ids = {item['id'] for item in arr1}
         missing_ids = [{'id': item['id'], 'sales': item['sales']} for item in arr2 if item['id'] not in arr1_ids]
         print(missing_ids, "missing")
         return 'missing', missing_ids
# Main async function to fetch data concurrently using ThreadPoolExecutor
async def main():
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        # Fetch data concurrently from MySQL, SQL Server, and Stored Procedure
        df_dataArr1, df_dataArr2, df_dataArr3 = await asyncio.gather(
            loop.run_in_executor(executor, fetch_from_mysql),
            loop.run_in_executor(executor, fetch_from_sqlserver),
            loop.run_in_executor(executor, execute_stored_procedure)
        )

    # Convert dataframes to lists of dictionaries
    arr1 = df_dataArr1.to_dict(orient='records')
    arr2 = df_dataArr2.to_dict(orient='records')
    arr3 = df_dataArr3.to_dict(orient='records')

    # Find differences or missing IDs
    issue_type, differences = find_different_sales_or_missing_ids(arr1, arr2, arr3)

    # Case 1: arr1 == arr2 == arr3
    if issue_type == 'differences' and len(differences) == 0:
        # No differences, export data
        processor = DataProcessor(CreateFilesStrategy())
    elif issue_type == 'differences' and len(differences) > 0:
        # Differences found, send an email about discrepancies
        email_recipients = ['ai.pd@famima.vn', 'an.np@famima.vn']
        processor = DataProcessor(SendEmailStrategy(emails=email_recipients))
    else:  # Case 2: arr1 != arr2 != arr3
        # Missing IDs found, send an email about missing data
        print(len(arr1) ,len(arr2) , len(arr3))
        email_recipients = ['an.np@famima.vn', 'ai.pd@famima.vn']
        processor = DataProcessor(SendEmailStrategy(emails=email_recipients))

    # Process data based on the selected strategy
    processor.process_data(arr1, arr2, arr3, differences)

# Run the main function
if __name__ == '__main__':
    asyncio.run(main())
