import aiohttp
import asyncio

# Bearer Token
BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg4NkE3REIwQjU1OThEMTNEMEY2MzE0RjUzQjI3RDlEIiwidHlwIjoiYXQrand0In0.eyJuYmYiOjE3MzY5MTY1NzAsImV4cCI6MTczNzAwMjk3MCwiaXNzIjoiaHR0cHM6Ly9mZHMtYXV0aC5yb3gudm4iLCJhdWQiOlsiUG9ydGFsIiwiRklMRSIsIklOVEciLCJJTlYiLCJNQUlMIiwiTURNIiwiTk9USSIsIlBPTSIsIlJQVCJdLCJjbGllbnRfaWQiOiJGRFNfV0VCIiwic3ViIjoiZDBlMTczNTAtNDE5MC00MjcxLTg3OGEtM2I4Y2VkYWMyOGQ1IiwiYXV0aF90aW1lIjoxNzM2OTE2NTY5LCJpZHAiOiJsb2NhbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IsOBSSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3N1cm5hbWUiOiJQSOG6oE0gxJDhu6hDIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRjAwMDA0NDIiLCJnaXZlbl9uYW1lIjoiw4FJIiwiZmFtaWx5X25hbWUiOiJQSOG6oE0gxJDhu6hDIiwicm9sZSI6WyJhZG1pbiIsIklUIl0sInBob25lX251bWJlciI6IjEyMzQ1Njc4OTAiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOiJGYWxzZSIsImVtYWlsIjoiYWkucGRAZmFtaW1hLnZuIiwiZW1haWxfdmVyaWZpZWQiOiJGYWxzZSIsIm5hbWUiOiJGMDAwMDQ0MiIsInNpZCI6IkMzM0Y1QzZCOTg3QzNCNjQzNTA2NEE0QjYxRUEzQTUyIiwiaWF0IjoxNzM2OTE2NTcwLCJzY29wZSI6WyJvcGVuaWQiLCJQb3J0YWwiLCJGSUxFIiwiSU5URyIsIklOViIsIk1BSUwiLCJNRE0iLCJOT1RJIiwiUE9NIiwiUlBUIl0sImFtciI6WyJwd2QiXX0.Fwz4xOGSBDZ0CQwztjr0EHtnMBA-Mkq0hF072G5q9NhoTFw6GfroUStZRX0v3jU4MF5-_j1rlDaqXpP7dsYm5P1NeEsni9wg2ZpRaWEVVKIytrwFKp9EWnA-RxOdGlkrsgm4k7MzrguWKooMUVfDaEQulLJLmeEBlP_Rn2uoMmnKMvqCT3hhja1TPoFeLRIstoH4XQlfrJMTswp9seN5okrW-MWFZ0Mp-6TmfPQsnSvExd-RxjRTpztipp2iQGwjaC3nnVACLLdxX98UG2BiJRY_FmkoVAcf2YTa59STfrP2NVjn_x6QamO0WM0NUdQacWzzfCjk0HNVb_pNMJHbjg"

# Cấu hình thông tin mặc định
STORE_NAME = "DUMMY TRANFER"
STORE_CODE = "00179"  # Biến này có thể thay đổi

async def fetch_inventory_transfer(session):
    # URL API
    url = "https://fds-portal.rox.vn/inv/api/app/inventory-transfer/get-list"

    # Dữ liệu body
    payload = {
        "filterText": "",
        "fromDate": "2025-01-01T00:00:00.000Z",
        "isExportStore": False,
        "maxResultCount": 1,
        "skipCount": 0,
        "sorting": "CreationTime desc",
        "statusCode": "INVENTORY_TRANSFER_STATUS.GOODS_ISSUED",
        "storeCode": [STORE_CODE],
        "toDate": "2025-01-30T23:59:59.999Z",
        "type": ""
    }

    async with session.post(url, json=payload) as response:
        if response.status == 200:
            result = await response.json()
            data = result.get("data", {})
            items = data.get("items", [])
            return [{"code": item.get("code"), "id": item.get("id")} for item in items]
        else:
            print(f"Failed to fetch inventory transfer data. Status code: {response.status}")
            print(await response.text())
            return []

async def gr_detail_from_transfer(session, inventory_transfer_id):
    # URL API
    url = f"https://fds-portal.rox.vn/inv/api/app/goods-receipt/gr-detail-from-transfer/{inventory_transfer_id}"

    async with session.get(url) as response:
        if response.status == 200:
            try:
                result = await response.json()  # Parse JSON
                if result is None:
                    print(f"No data returned for inventoryTransferId={inventory_transfer_id}")
                    return {}

                data = result.get("data", {})
                goodsReceiptDetail = data.get("goodsReceiptDetail", [])
                goodsIssueId = data.get("goodsIssueId", None)
                goodsIssueCode = data.get("goodsIssueCode", None)

                # Trả về dictionary với 3 trường đồng cấp
                return {
                    "goodsIssueId": goodsIssueId,
                    "goodsIssueCode": goodsIssueCode,
                    "goodsReceiptDetail": goodsReceiptDetail,
                }
            except Exception as e:
                print(f"Failed to parse JSON for inventoryTransferId={inventory_transfer_id}. Error: {e}")
                print(await response.text())  # Log raw response for debugging
                return {}
        else:
            print(f"Failed to call API for inventoryTransferId={inventory_transfer_id}. Status code: {response.status}")
            print(await response.text())  # Log response for debugging
            return {}


async def call_add_goods_issue(session, payload):
    # URL API
    url = "https://fds-portal.rox.vn/inv/api/app/goods-receipt"

    async with session.post(url, json=payload) as response:
        if response.status == 200:
            result = await response.json()
            print(f"Response from goods issue add API: {result}")
            return result
        else:
            print(f"Failed to call add goods issue API. Status code: {response.status}")
            print(await response.text())
            return None



async def main():
    async with aiohttp.ClientSession(headers={"Authorization": BEARER_TOKEN}) as session:
        codes_and_ids = await fetch_inventory_transfer(session)

        for item in codes_and_ids:
            inventory_transfer_id = item["id"]
            ref_code = item["code"]

            # Gọi API để lấy goodsIssueId, goodsIssueCode, và goodsReceiptDetail
            transfer_details = await gr_detail_from_transfer(session, inventory_transfer_id)

            if transfer_details:  # Chỉ tiếp tục nếu có dữ liệu
                payload = {
                    "issuesDateTime": None,
                    "refCode": ref_code,
                    "refId": inventory_transfer_id,
                    "storeName": STORE_NAME,
                    "storeCode": STORE_CODE,
                    "type": "GOODS_RECEIPT_TYPE.INVENTORY_TRANSFER",
                    "refTypeCode": "INVENTORY_TRANSFER_TYPE.FROM_STORE_TO_STORE",
                    "description": "",
                    "goodsReceiptDetail": transfer_details["goodsReceiptDetail"],
                    "goodsIssueCode": transfer_details["goodsIssueCode"],
                    "goodsIssueId": transfer_details["goodsIssueId"],
                    "receiptDateTime": None,
                    "typeName": "Nhập hàng theo yêu cầu chuyển hàng"
                }
                print(f"Adding goodsIssueDetails for inventoryTransferId={transfer_details}")
                await call_add_goods_issue(session, payload)
                print(f"Successfully added goodsIssueDetails for inventoryTransferId={ref_code}")
            else:
                print(f"No goodsIssueDetails for inventoryTransferId={inventory_transfer_id}")



# Thực thi
asyncio.run(main())
