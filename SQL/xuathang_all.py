import aiohttp
import asyncio

# Bearer Token
BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg4NkE3REIwQjU1OThEMTNEMEY2MzE0RjUzQjI3RDlEIiwidHlwIjoiYXQrand0In0.eyJuYmYiOjE3Mzg4OTM0MjUsImV4cCI6MTczODk3OTgyNSwiaXNzIjoiaHR0cHM6Ly9mZHMtYXV0aC5yb3gudm4iLCJhdWQiOlsiUG9ydGFsIiwiRklMRSIsIklOVEciLCJJTlYiLCJNQUlMIiwiTURNIiwiTk9USSIsIlBPTSIsIlJQVCJdLCJjbGllbnRfaWQiOiJGRFNfV0VCIiwic3ViIjoiZDBlMTczNTAtNDE5MC00MjcxLTg3OGEtM2I4Y2VkYWMyOGQ1IiwiYXV0aF90aW1lIjoxNzM4ODkzNDI1LCJpZHAiOiJsb2NhbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IsOBSSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3N1cm5hbWUiOiJQSOG6oE0gxJDhu6hDIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRjAwMDA0NDIiLCJnaXZlbl9uYW1lIjoiw4FJIiwiZmFtaWx5X25hbWUiOiJQSOG6oE0gxJDhu6hDIiwicm9sZSI6WyJhZG1pbiIsIklUIl0sInBob25lX251bWJlciI6IjEyMzQ1Njc4OTAiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOiJGYWxzZSIsImVtYWlsIjoiYWkucGRAZmFtaW1hLnZuIiwiZW1haWxfdmVyaWZpZWQiOiJGYWxzZSIsIm5hbWUiOiJGMDAwMDQ0MiIsInNpZCI6IjJDRjVDMkJEMzQyODRGNEVCOUREQjA1QjAxRDY5RjRCIiwiaWF0IjoxNzM4ODkzNDI1LCJzY29wZSI6WyJvcGVuaWQiLCJQb3J0YWwiLCJGSUxFIiwiSU5URyIsIklOViIsIk1BSUwiLCJNRE0iLCJOT1RJIiwiUE9NIiwiUlBUIl0sImFtciI6WyJwd2QiXX0.RA_8kdiUw6g-IYqJC7U2OdFdRuQAQ52ep2PeLB0RgJpE_JWsUioQlZ4eQkKkWCrNsyCzoVJ1Kw5Q6Et6HkqbXXmEhAWei2bFJBtIp2qZMHDKlae4UZ4zA-dB9kD4bX0fqihNtwaK7DKhwW2B3zI1WrYiSdWpGt0_hou_ftMimJBLdhHXE-DD1Kisi2nupFKODl6MAMinxRloP2qr_tPxDIxRj4ET8M60efhMkSNCKMPP2Cfq-CAQ2zJmdylcz2KsNah4eTq7VValu7q1maT09tdSlmGVFcD3ux7juKTcYGkSz9aQMECwQYB9_37lDSSpJr8Ov1uX7OSLwSlOAMVs3g"

# Cấu hình thông tin mặc định
FROM_DATE = "2024-12-01T00:00:00.000Z"
TO_DATE = "2024-12-31T23:59:59.999Z"

# Lấy danh sách cửa hàng
async def fetch_store_codes(session):
    url = "https://fds-portal.rox.vn/mdm/api/app/store?skipCount=0&maxResultCount=200"

    async with session.get(url) as response:
        if response.status == 200:
            result = await response.json()
            data = result.get("data", {})
            items = data.get("items", [])
            return [item.get("code") for item in items if item.get("code")]
        else:
            print(f"Failed to fetch store codes. Status code: {response.status}")
            print(await response.text())
            return []

# Lấy danh sách phiếu xuất hàng của một cửa hàng
async def fetch_inventory_transfer(session, store_code):
    url = "https://fds-portal.rox.vn/inv/api/app/inventory-transfer/get-list"
    payload = {
        "filterText": "",
        "fromDate": FROM_DATE,
        "isExportStore": True,
        "maxResultCount": 300,
        "skipCount": 0,
        "sorting": "CreationTime desc",
        "statusCode": "INVENTORY_TRANSFER_STATUS.APPROVED",
        "storeCode": [store_code],
        "toDate": TO_DATE,
        "type": ""
    }

    async with session.post(url, json=payload) as response:
        if response.status == 200:
            result = await response.json()
            data = result.get("data", {})
            items = data.get("items", [])
            return [{"code": item.get("code"), "id": item.get("id")} for item in items]
        else:
            print(f"Failed to fetch inventory transfer data for store {store_code}. Status code: {response.status}")
            print(await response.text())
            return []

# Gọi API tạo phiếu xuất hàng từ phiếu chuyển hàng
async def call_create_goods_issue(session, inventory_transfer_id):
    url = f"https://fds-portal.rox.vn/inv/api/app/goods-issue/create-by-inventory-transfer?inventoryTransferId={inventory_transfer_id}"

    async with session.get(url) as response:
        if response.status == 200:
            try:
                result = await response.json()
                if result is None:
                    print(f"No data returned for inventoryTransferId={inventory_transfer_id}")
                    return []
                goods_issue_details = result.get("data", {}).get("goodsIssueDetails", [])
                return goods_issue_details
            except Exception as e:
                print(f"Failed to parse JSON for inventoryTransferId={inventory_transfer_id}. Error: {e}")
                print(await response.text())  # Log raw response for debugging
                return []
        else:
            print(f"Failed to call API for inventoryTransferId={inventory_transfer_id}. Status code: {response.status}")
            print(await response.text())  # Log response for debugging
            return []

# Gọi API thêm phiếu xuất hàng
async def call_add_goods_issue(session, payload):
    url = "https://fds-portal.rox.vn/inv/api/app/goods-issue/add"

    async with session.post(url, json=payload) as response:
        if response.status == 200:
            result = await response.json()
            print(f"Response from goods issue add API: {result}")
            return result
        else:
            print(f"Failed to call add goods issue API. Status code: {response.status}")
            print(await response.text())
            return None

# Lấy thông tin chi tiết phiếu xuất hàng từ phiếu chuyển hàng
async def fetch_specific_inventory_transfer(session, specific_id):
    url = f"https://fds-portal.rox.vn/inv/api/app/inventory-transfer/{specific_id}"

    async with session.get(url) as response:
        if response.status == 200:
            try:
                result = await response.json()
                print(f"Data for inventory transfer ID {specific_id}: {result}")
                return result
            except Exception as e:
                print(f"Failed to parse JSON for specific inventory transfer ID {specific_id}. Error: {e}")
                print(await response.text())
                return None
        else:
            print(f"Failed to fetch specific inventory transfer data. Status code: {response.status}")
            print(await response.text())
            return None

# Hàm chính để xử lý tất cả cửa hàng
async def main():
    async with aiohttp.ClientSession(headers={"Authorization": BEARER_TOKEN}) as session:
        # Lấy danh sách cửa hàng
        store_codes = await fetch_store_codes(session)
        if not store_codes:
            print("No store codes found. Exiting...")
            return

        print(f"Processing {len(store_codes)} stores...")

        # Lặp qua từng cửa hàng để xử lý phiếu xuất hàng
        for store_code in store_codes:
            print(f"Processing store code: {store_code}")

            # Lấy danh sách phiếu xuất hàng của cửa hàng
            codes_and_ids = await fetch_inventory_transfer(session, store_code)

            for item in codes_and_ids:
                inventory_transfer_id = item["id"]
                ref_code = item["code"]

                # Lấy chi tiết phiếu xuất hàng
                goods_issue_details = await call_create_goods_issue(session, inventory_transfer_id)

                if goods_issue_details:  # Nếu có dữ liệu, tiếp tục tạo phiếu xuất
                    payload = {
                        "issuesDateTime": None,
                        "refCode": ref_code,
                        "refId": inventory_transfer_id,
                        "storeName": "DUMMY TRANSFER",
                        "storeCode": store_code,
                        "type": "GOODS_ISSUE_TYPE.INVENTORY_TRANSFER",
                        "description": "",
                        "goodsIssueDetails": goods_issue_details
                    }
                    print(f"Adding goodsIssueDetails for inventoryTransferId={ref_code}")
                    await call_add_goods_issue(session, payload)
                    print(f"Successfully added goodsIssueDetails for inventoryTransferId={ref_code}")
                else:
                    print(f"No goodsIssueDetails for inventoryTransferId={inventory_transfer_id}")

# Thực thi
asyncio.run(main())
