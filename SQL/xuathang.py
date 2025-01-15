import aiohttp
import asyncio

# Bearer Token
BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg4NkE3REIwQjU1OThEMTNEMEY2MzE0RjUzQjI3RDlEIiwidHlwIjoiYXQrand0In0.eyJuYmYiOjE3MzY4MjE3NzUsImV4cCI6MTczNjkwODE3NSwiaXNzIjoiaHR0cHM6Ly9mZHMtYXV0aC5yb3gudm4iLCJhdWQiOlsiUG9ydGFsIiwiRklMRSIsIklOVEciLCJJTlYiLCJNQUlMIiwiTURNIiwiTk9USSIsIlBPTSIsIlJQVCJdLCJjbGllbnRfaWQiOiJGRFNfV0VCIiwic3ViIjoiZDBlMTczNTAtNDE5MC00MjcxLTg3OGEtM2I4Y2VkYWMyOGQ1IiwiYXV0aF90aW1lIjoxNzM2ODIxNzc0LCJpZHAiOiJsb2NhbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IsOBSSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3N1cm5hbWUiOiJQSOG6oE0gxJDhu6hDIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRjAwMDA0NDIiLCJnaXZlbl9uYW1lIjoiw4FJIiwiZmFtaWx5X25hbWUiOiJQSOG6oE0gxJDhu6hDIiwicm9sZSI6WyJhZG1pbiIsIklUIl0sInBob25lX251bWJlciI6IjEyMzQ1Njc4OTAiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOiJGYWxzZSIsImVtYWlsIjoiYWkucGRAZmFtaW1hLnZuIiwiZW1haWxfdmVyaWZpZWQiOiJGYWxzZSIsIm5hbWUiOiJGMDAwMDQ0MiIsInNpZCI6IjdEQzYyNkYyMTE1NkNGNkZGMkYyQUNDQUI5OEEzQTE4IiwiaWF0IjoxNzM2ODIxNzc1LCJzY29wZSI6WyJvcGVuaWQiLCJQb3J0YWwiLCJGSUxFIiwiSU5URyIsIklOViIsIk1BSUwiLCJNRE0iLCJOT1RJIiwiUE9NIiwiUlBUIl0sImFtciI6WyJwd2QiXX0.NFQUdmsr7qp43BxiG7rSCMLCWHMYs49LBuWsjxQH-q7sOC2wXIxMD_uafV6Xm622VhUFhBuR4QFKzDv4PBLpG4x28eGBQF705mRxepZQXH6So5hzsNXmtyETfXg47r2ck_n6Vb2yi8WX6QOrXPYVtRpP12rncb0feBwTFkCulu9W35q1VktJ5nkJrsbU97gfx75Dur0Lg1RDlxYTKHQttAazm85fHL8qY_FC16YwlHUrvyf9BPG40Y0XR3rTVGuSBJdG9Mbmb6E6HCjMqKGMHMDJ2QNCZawlg1zKvl4yDIXfJjqYaoWMP_t5bMQdNulx7Bc7zGPhf5rEir8dFRp0wA"

# Cấu hình thông tin mặc định
STORE_NAME = "DUMMY TRANFER"
STORE_CODE = "80002"  # Biến này có thể thay đổi

async def fetch_inventory_transfer(session):
    # URL API
    url = "https://fds-portal.rox.vn/inv/api/app/inventory-transfer/get-list"

    # Dữ liệu body
    payload = {
        "filterText": "",
        "fromDate": "2025-01-01T00:00:00.000Z",
        "isExportStore": True,
        "maxResultCount": 1,
        "skipCount": 0,
        "sorting": "CreationTime desc",
        "statusCode": "INVENTORY_TRANSFER_STATUS.APPROVED",
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

async def call_create_goods_issue(session, inventory_transfer_id):
    # URL API
    url = f"https://fds-portal.rox.vn/inv/api/app/goods-issue/create-by-inventory-transfer?inventoryTransferId={inventory_transfer_id}"

    async with session.get(url) as response:
        if response.status == 200:
            try:
                result = await response.json()  # Parse JSON
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


async def call_add_goods_issue(session, payload):
    # URL API
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

async def fetch_specific_inventory_transfer(session, specific_id):
    # URL API
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

async def main():
    async with aiohttp.ClientSession(headers={"Authorization": BEARER_TOKEN}) as session:
        codes_and_ids = await fetch_inventory_transfer(session)

        for item in codes_and_ids:
            inventory_transfer_id = item["id"]
            ref_code = item["code"]

            # Gọi API để lấy goodsIssueDetails
            goods_issue_details = await call_create_goods_issue(session, inventory_transfer_id)

            if goods_issue_details:  # Chỉ tiếp tục nếu có dữ liệu
                payload = {
                    "issuesDateTime": None,
                    "refCode": ref_code,
                    "refId": inventory_transfer_id,
                    "storeName": STORE_NAME,
                    "storeCode": STORE_CODE,
                    "type": "GOODS_ISSUE_TYPE.INVENTORY_TRANSFER",
                    "description": "",
                    "goodsIssueDetails": goods_issue_details
                }
                print(f"Adding goodsIssueDetails for inventoryTransferId={ref_code}")
                await call_add_goods_issue(session, payload)
                await fetch_specific_inventory_transfer(session, inventory_transfer_id)
                print(f"Successfully added goodsIssueDetails for inventoryTransferId={ref_code}")
            else:
                print(f"No goodsIssueDetails for inventoryTransferId={inventory_transfer_id}")


# Thực thi
asyncio.run(main())
