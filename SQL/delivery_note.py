import aiohttp
import asyncio
import os
import pandas as pd
from datetime import datetime
import numpy as np

# Bearer Token
BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg4NkE3REIwQjU1OThEMTNEMEY2MzE0RjUzQjI3RDlEIiwidHlwIjoiYXQrand0In0.eyJuYmYiOjE3Mzc3ODUyMDQsImV4cCI6MTczNzg3MTYwNCwiaXNzIjoiaHR0cHM6Ly9mZHMtYXV0aC5yb3gudm4iLCJhdWQiOlsiUG9ydGFsIiwiRklMRSIsIklOVEciLCJJTlYiLCJNQUlMIiwiTURNIiwiTk9USSIsIlBPTSIsIlJQVCJdLCJjbGllbnRfaWQiOiJGRFNfV0VCIiwic3ViIjoiZDBlMTczNTAtNDE5MC00MjcxLTg3OGEtM2I4Y2VkYWMyOGQ1IiwiYXV0aF90aW1lIjoxNzM3Nzg1MjA0LCJpZHAiOiJsb2NhbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IsOBSSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3N1cm5hbWUiOiJQSOG6oE0gxJDhu6hDIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRjAwMDA0NDIiLCJnaXZlbl9uYW1lIjoiw4FJIiwiZmFtaWx5X25hbWUiOiJQSOG6oE0gxJDhu6hDIiwicm9sZSI6WyJhZG1pbiIsIklUIl0sInBob25lX251bWJlciI6IjEyMzQ1Njc4OTAiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOiJGYWxzZSIsImVtYWlsIjoiYWkucGRAZmFtaW1hLnZuIiwiZW1haWxfdmVyaWZpZWQiOiJGYWxzZSIsIm5hbWUiOiJGMDAwMDQ0MiIsInNpZCI6IkU2MDVCQTJFRDcwODM4RjcwMThFQ0Y5RkNFOEZGMTYwIiwiaWF0IjoxNzM3Nzg1MjA0LCJzY29wZSI6WyJvcGVuaWQiLCJQb3J0YWwiLCJGSUxFIiwiSU5URyIsIklOViIsIk1BSUwiLCJNRE0iLCJOT1RJIiwiUE9NIiwiUlBUIl0sImFtciI6WyJwd2QiXX0.gO6-qCcNmt0Ubxdm41e6XYaE9O94Ki9wZ5Lm5sJKNUzbXiy5-kZCchcoSpKrVrCygX56YGBb0h-k8pHSWeb9XNpXfdL-uqstPfO_twye9Wmiudwhh0T3XfifKJ9DMNdWyHpnxAsxgCUmrH5bmoe6LJ2rHb36sZz6oq6dx-K3JQBbhSWAVUGwnDzSKILCoZ3a8Yj6Io8HmsHbYmgFFpyaShT8hAgIh0nQWRYrMafW9VQjZfflYkfoYIz2dCrJQ67ov6c9fkVTwe-VrdcLrZkD1-I9OaPsb8TywiFJI0u2cF0ncCX1Iioc3ybOKQbzKoUpjCL2nP490yQXBIrEA6F7_Q"
ISSUES_DATE_TIME = "2025-01-06T13:09:42.000Z" 
# Đệ quy chuyển đổi numpy.int64 hoặc numpy.float64 trong danh sách object
def convert_numpy_to_python(obj):
    if isinstance(obj, list):
        return [convert_numpy_to_python(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_numpy_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, (pd.Int64Dtype, np.int64, np.float64)):
        return int(obj) if isinstance(obj, (np.int64, pd.Int64Dtype)) else float(obj)
    return obj

# Chuyển đổi dữ liệu từ API sang dữ liệu GoodsIssueDetail
def convert_data_good_issue_detail(api_data, quantity):
    # Lấy ngày giờ hiện tại và định dạng thành ISO 8601
    current_datetime = datetime.now().isoformat()

    # Chuyển đổi dữ liệu
    converted_data = {
        "beginningInventory": api_data.get("beginningInventory"),
        "createDateTime": current_datetime,
        "goodsIssueDetailExpiryDates": [],  # Mảng rỗng
        "inventoryCostPrice": api_data.get("inventoryCostPrice"),
        "inventoryFirstReceiptDateTime": api_data.get("inventoryFirstReceiptDateTime"),
        "inventoryId": api_data.get("inventoryId"),
        "inventoryManagement": api_data.get("inventoryManagement"),
        "inventoryUnitCode": api_data.get("inventoryUnitCode"),
        "inventoryUnitName": api_data.get("inventoryUnitName"),
        "isActive": api_data.get("isActive"),
        "isDefaultQuantity": True,  # Luôn luôn true
        "issuePrice": api_data.get("inventoryCostPrice"),
        "issueQuantity": quantity,  # Lấy từ file Excel
        "productBarcode": api_data.get("inventoryBarCode"),
        "productCode": api_data.get("code"),
        "productCostFormulaTypeCode": api_data.get("costFormula"),
        "productExpiryDateManagement": api_data.get("expiryDateManagement"),
        "productName": api_data.get("name"),
        "productNameEn": api_data.get("nameEn"),
        "productShortName": api_data.get("shortName"),
        "productUnitCode": api_data.get("inventoryUnitCode"),
        "productUnitName": api_data.get("inventoryUnitName")
    }

    return converted_data

def convert_issue_detail_mappings(good_issue_detail, api_data, total_quantity):
    # Lấy ngày giờ hiện tại và định dạng thành ISO 8601
    current_datetime = datetime.now().isoformat()
    
    # Kiểm tra hasAssociation
    has_association = api_data.get("parentProduct") is not None

    # Lấy detailRefQty từ productAssociationItems

    # Chuyển đổi dữ liệu cuối cùng
    final_data = {
        "beginningInventory": api_data.get("beginningInventory"),
        "createDateTime": current_datetime,
        "detailRefQty": "[]",  # Mảng rỗng
        "goodsIssueDetails": [good_issue_detail],  # Kết quả từ hàm convert_data_good_issue_detail
        "hasAssociation": has_association,
        "inventoryCostPrice": api_data.get("inventoryCostPrice"),
        "inventoryFirstReceiptDateTime": api_data.get("inventoryFirstReceiptDateTime"),
        "inventoryId": api_data.get("inventoryId"),
        "inventoryManagement": api_data.get("inventoryManagement"),
        "inventoryUnitCode": api_data.get("inventoryUnitCode"),
        "inventoryUnitName": api_data.get("inventoryUnitName"),
        "isActive": api_data.get("isActive"),
        "isDefaultQuantity": True,  # Luôn luôn true
        "issuePrice": api_data.get("inventoryCostPrice"),
        "issueQuantity": total_quantity,  # Tổng số lượng lấy từ file Excel
        "productBarcode": api_data.get("inventoryBarCode"),
        "productCode": api_data.get("code"),
        "productCostFormulaTypeCode": api_data.get("costFormula"),
        "productExpiryDateManagement": api_data.get("expiryDateManagement"),
        "productName": api_data.get("name"),
        "productNameEn": api_data.get("nameEn"),
        "productShortName": api_data.get("shortName"),
        "productUnitCode": api_data.get("inventoryUnitCode"),
        "productUnitName": api_data.get("inventoryUnitName"),
    }

    return final_data



# Tự động tìm file Excel trong thư mục
def find_excel_file(folder_path):
    for file in os.listdir(folder_path):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            return os.path.join(folder_path, file)
    return None

# Đọc dữ liệu từ file Excel
def read_excel_data(file_path):
    df = pd.read_excel(file_path)
    df = df.dropna()  # Loại bỏ các dòng trống
    return df

# Hàm gọi API (tích hợp mã hiện tại)
async def fetch_inventory_transfer(session, store_code, item_code):
    url = f"https://fds-portal.rox.vn/inv/api/app/product/search-by-code-or-barcode?value={item_code}&type=GoodsIssue&storeCode={store_code}"
    async with session.get(url) as response:
        if response.status == 200:
            json_response = await response.json()
            print(json_response)
            data = json_response.get('data')
            if data:
                print(data)
                return data  # Chỉ trả về dữ liệu trong trường 'data'
            else:
                print(f"Dữ liệu rỗng hoặc không tìm thấy trường 'data' cho CH={store_code}, Item={item_code}")
                return None
        else:
            print(f"Lỗi khi gọi API cho CH={store_code}, Item={item_code}, Status={response.status}")
            print(await response.text())
            return None
async def post_goods_issue(session, store_code, result_goods_issue_detail, result_goods_issue_detail_mappings):
    body = {
        "description": f"Goods issue for store {store_code}",
        "goodsIssueDetailMappings": result_goods_issue_detail_mappings,
        "goodsIssueDetails": result_goods_issue_detail,
        "issuesDateTime": ISSUES_DATE_TIME,
        "refCode": None,
        "refId": None,
        "storeCode": store_code,
        "storeName": "",
        "type": "GOODS_ISSUE_TYPE.OTHER"
    }

    post_url = "https://fds-portal.rox.vn/inv/api/app/goods-issue/add"

    try:
        # Send POST request
        async with session.post(post_url, json=convert_numpy_to_python(body)) as response:
            # Check if the response is successful
            if response.status == 200:
                print(f"API POST succeeded for storeCode={store_code}")
                return await response.json()
            else:
                # Handle errors when the response status is not 200
                error_message = await response.text()
                print(f"Error during API POST. Status={response.status}. Response: {error_message}")
                return None
    except aiohttp.ClientConnectionError as e:
        print(f"Connection error during API POST for storeCode={store_code}: {e}")
        return None
    except aiohttp.ClientResponseError as e:
        print(f"Response error from server during API POST for storeCode={store_code}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during API POST for storeCode={store_code}: {e}")
        return None

async def main():
    folder_path = r"D:\xuathang"  # Thư mục chứa file Excel
    file_path = find_excel_file(folder_path)
    
    if not file_path:
        print("Không tìm thấy file Excel trong thư mục!")
        return
    
    print(f"Đọc dữ liệu từ file: {file_path}")
    data = read_excel_data(file_path)

    async with aiohttp.ClientSession(headers={"Authorization": BEARER_TOKEN}) as session:
        # Lấy danh sách mã cửa hàng
        store_codes = data['CH'].unique()
        
        for store_code in store_codes:
            # Lọc danh sách sản phẩm thuộc cửa hàng
            items = data[data['CH'] == store_code]
            result_goods_issue_detail = []
            result_goods_issue_detail_mappings=[]
            store_code = str(store_code).zfill(5)

            # Tính tổng số lượng từ cột `qty` trong file Excel
            for _, row in items.iterrows():
                item_code = row['item']
                quantity = row['qty']  # Lấy số lượng từ file Excel cho từng sản phẩm
                print(f"Gọi API cho CH={store_code}, Mã sản phẩm={item_code}")
                api_data = await fetch_inventory_transfer(session, store_code, item_code)
                
                if api_data:
                    # Chuyển đổi dữ liệu và thêm vào danh sách kết quả
                    good_issue_detail = convert_data_good_issue_detail(api_data, quantity)
                    good_issue_detail_mappings = convert_issue_detail_mappings(good_issue_detail, api_data, quantity)

                    result_goods_issue_detail.append(good_issue_detail)
                    result_goods_issue_detail_mappings.append(good_issue_detail_mappings)

                    print(f"Dữ liệu đã chuyển đổi: {result_goods_issue_detail_mappings}")
            post_response = await post_goods_issue(
            session=session,
            store_code=store_code,
            result_goods_issue_detail=result_goods_issue_detail,
            result_goods_issue_detail_mappings=result_goods_issue_detail_mappings
            )
            if post_response:
                print(f"Phản hồi từ API POST cho cửa hàng {store_code}: {post_response}")
            else:
                print(f"API POST thất bại cho cửa hàng {store_code}.")

# Thực thi chương trình
asyncio.run(main())