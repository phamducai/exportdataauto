import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib

# Lấy ngày hiện tại
current_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
from_date = f"{current_date}T00:00:00.000"
to_date = f"{current_date}T23:59:59.999"

# Thông tin kết nối SQL Server
SQL_CONFIG = {
    "user": "it",
    "password": "Famima@123",
    "host": "192.168.10.16",
    "instance": "POS_SQLSERVER",
    "database": "DSMART90"
}

# Thông tin Telegram
BOT_TOKEN = "7988421115:AAHU4RGoDNrrGaU4t_9HkLbW59P78X_Qhm8"
GROUP_CHAT_ID = "-4718435048"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

API_URL = "https://fds-portal.rox.vn/rpt/api/app/sale-invoice/sale-invoice"

# Kết nối SQL Server
def connect_to_sql():
    try:
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_CONFIG['host']}\\{SQL_CONFIG['instance']};"
            f"DATABASE={SQL_CONFIG['database']};"
            f"UID={SQL_CONFIG['user']};"
            f"PWD={SQL_CONFIG['password']};"
            "TrustServerCertificate=yes"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_string)
        print("Kết nối thành công đến SQL Server")
        return engine
    except Exception as e:
        print(f"Lỗi kết nối SQL Server: {e}")
        raise

# Lấy dữ liệu từ SQL Server
def get_sql_data(engine):
    query = """
    SELECT STK_Code 
    FROM Stock 
    WHERE Disabled = 0 AND STK_Code NOT IN (00000,00290,00291,00292,00293,00203,00202)
    """
    try:
        df = pd.read_sql(query, engine)
        return df['STK_Code'].astype(int).tolist()
    except Exception as e:
        print(f"Lỗi khi truy vấn SQL Server: {e}")
        raise

# Gọi API lấy dữ liệu
def get_api_data():
    api_body = {
        "skipCount": 0,
        "maxResultCount": 200,
        "fromDate": from_date,
        "toDate": to_date
    }
    try:
        response = requests.post(API_URL, json=api_body)
        if response.status_code == 200:
            data = response.json()
            if 'items' in data['data']:
                 return [int(item['storeCode']) for item in data['data']['items'] if 'storeCode' in item]
            else:
                print("API response không chứa 'items'")
                return []
        else:
            print(f"Lỗi gọi API: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Lỗi khi gọi API: {e}")
        return []

# So sánh dữ liệu
def compare_data(sql_data, api_data):
    return list(set(sql_data) - set(api_data))

# Chia nhỏ tin nhắn
def split_into_batches(items, batch_size=20):
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

# Gửi thông báo qua Telegram
def send_telegram_message(messages):
    try:
        for message in messages:
            response = requests.post(TELEGRAM_URL, data={"chat_id": GROUP_CHAT_ID, "text": message})
            if response.status_code == 200:
                print("Tin nhắn đã được gửi thành công!")
            else:
                print(f"Lỗi gửi tin nhắn Telegram: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Lỗi khi gửi tin nhắn Telegram: {e}")

# Main function
def main():
    # Kết nối SQL Server và lấy dữ liệu
    engine = connect_to_sql()
    sql_data = get_sql_data(engine)

    # Gọi API lấy dữ liệu
    api_data = get_api_data()
    print(api_data)
    # So sánh dữ liệu
    missed_stores = compare_data(sql_data, api_data)

    # Tạo thông báo
    if missed_stores:
        batches = split_into_batches(missed_stores, batch_size=50)
        messages = []
        for batch in batches:
            message = (
                f"(Tổng cộng: {len(missed_stores)}) cho ngày {current_date}\n\n"
            )
            for store in batch:
                message += f"🏬 Store ID: {store}\n"
                message += f"☣️ Status: Chưa kết ca\n\n"
            messages.append(message)
        send_telegram_message(messages)
    else:
        send_telegram_message([f"Không có cửa hàng nào chưa kết ca cho ngày {current_date}!"])

# Chạy chương trình
if __name__ == "__main__":
    main()
