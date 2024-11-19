import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
def fetch_sales_data():
    # SQL Server connection details
    sqlserver_user = 'it'
    sqlserver_password = 'Famima@123'
    sqlserver_host = '192.168.10.16'
    sqlserver_instance = 'POS_SQLSERVER'
    sqlserver_db = 'DSMART90'

    # Get current date and first day of month
    current_date = datetime.now()
    first_day = current_date.replace(day=1).strftime("%Y-%m-%d")
    current_date_str = current_date.strftime("%Y-%m-%d")
    
    # Calculate number of days
    days_in_period = (current_date - current_date.replace(day=1)).days + 1

    # Daily query
    daily_query = f"""
    SELECT
        Stk_ID - 1 AS Store,
        SUM(Sales_Amt + Direct_Disc_Amt) AS Sale,
        COUNT(DISTINCT Trans_No) AS TransCount 
    FROM
        STr_SaleDtl 
    WHERE
        Trans_Date = '{current_date_str}'
        AND StkTrCls_ID = 3032 
    GROUP BY
        Stk_ID 
    ORDER BY
        Stk_ID
    """
    # Average query
    avg_query = f"""
    SELECT
        Stk_ID - 1 AS Store,
        ROUND(SUM(Sales_Amt + Direct_Disc_Amt) / {days_in_period}, 0) AS Sale,
        ROUND(COUNT(DISTINCT Trans_No) / {days_in_period}, 0) AS TransCount
    FROM
        STr_SaleDtl 
    WHERE
        Trans_Date >= '{first_day}'
        AND Trans_Date <= '{current_date_str}'
        AND StkTrCls_ID = 3032 
    GROUP BY
        Stk_ID 
    ORDER BY
        Stk_ID
    """

    # Create connection and fetch data
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sqlserver_host}\\{sqlserver_instance};"
        f"DATABASE={sqlserver_db};"
        f"UID={sqlserver_user};"
        f"PWD={sqlserver_password};"
        "TrustServerCertificate=yes"
    )
    sqlserver_connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    sqlserver_engine = create_engine(sqlserver_connection_string)

    with sqlserver_engine.connect() as conn:
        daily_df = pd.read_sql(daily_query, conn)
        avg_df = pd.read_sql(avg_query, conn)
        print(f" {daily_df}")

    return daily_df, avg_df

def send_email(daily_df, avg_df, recipients=['sales_data@famima.vn','n-kikunaga@famima.vn','t-tsuji204@family.co.jp','y-hirao@family.co.jp','a-kirimura@family.co.jp','a-okada759@family.co.jp','s-odake855@family.co.jp','t-suzuki984@family.co.jp','b-gen@family.co.jp','ai.pd@famima.vn','truong.tk@famima.vn','hien.dtn@famima.vn']):
    try:
        smtp_server = 'mail.famima.vn'
        smtp_port = 465
        smtp_user = 'no-reply@famima.vn'
        smtp_pass = 'JQKA@2222'
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        # Calculate totals for daily
        daily_total_sales = float(daily_df['Sale'].sum())
        daily_total_customers = int(daily_df['TransCount'].sum())

        # Calculate store average
        store_count = len(avg_df)
        total_sales = int(avg_df['Sale'].sum())  
        total_customers = int(avg_df['TransCount'].sum())
        store_avg_sales = total_sales / store_count
        store_avg_customers = total_customers / store_count
        
        # Format table with both daily and average data
        html_table = """
        <pre style="font-family: Consolas, monospace;">
============== DAILY BUSINESS REPORT ==============
ISSUED : {date}
CUSTOMER NAME: VIETNAM_FAMILYMART

SALES DATE : {date}
CURRENCY : VND

================================================
== DAILY SALES
================================================

STORE           SALES           NUMBER OF
ID             (VND)          CUSTOMERS
--------- ---------------- ------------
{daily_data}
---------------- ------------ ---------
TOTAL     {daily_total_sales:>15,.0f} {daily_total_customers:>12,.0f}

================================================
== MONTHLY AVERAGE
================================================

STORE           SALES           NUMBER OF
ID             (VND)          CUSTOMERS
--------- ---------------- ------------
{avg_data}
---------------- ------------ ------------
AVERAGE   {store_avg_sales:>15,.0f} {store_avg_customers:>12,.0f}
        </pre>
        """.format(
            date=current_date,
            daily_data=''.join(
                f"{int(row['Store']):05d}      {float(row['Sale']):>13,.0f} {int(row['TransCount']):>12,.0f}\n" 
                for _, row in daily_df.iterrows()
            ).rstrip(),
            daily_total_sales=daily_total_sales,
            daily_total_customers=daily_total_customers,
            avg_data=''.join(
                f"{int(row['Store']):05d}      {float(row['Sale']):>13,.0f} {float(row['TransCount']):>12,.0f}\n" 
                for _, row in avg_df.iterrows()
            ).rstrip(),
            store_avg_sales=store_avg_sales,
            store_avg_customers=store_avg_customers
        )
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <p style="font-weight: bold;">This email is automatically sent from the FMV system.</p>
            {html_table}
            <p>DO NOT REPLY TO THIS MAIL.</p>
        </body>
        </html>
        """
        
        message = MIMEMultipart()
        message['From'] = smtp_user
        message['To'] = ', '.join(recipients)
        message['Subject'] = f'VN_BUSINESS_REPORT'
        message.attach(MIMEText(body, 'html'))
        
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, message.as_string())
            print('Email sent successfully.')
            
    except Exception as e:
        print(f"Error sending email: {str(e)}")

def main():
    try:
        # Fetch both daily and average sales data
        daily_data, avg_data = fetch_sales_data()
        
        # Check if data exists
        if not daily_data.empty and not avg_data.empty:
            # Send email with both datasets
            send_email(daily_data, avg_data)
        else:
            print("No sales data found for the specified date")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()