import aiohttp
import asyncio

# Bearer Token
BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg4NkE3REIwQjU1OThEMTNEMEY2MzE0RjUzQjI3RDlEIiwidHlwIjoiYXQrand0In0.eyJuYmYiOjE3MzcwODM1NzcsImV4cCI6MTczNzE2OTk3NywiaXNzIjoiaHR0cHM6Ly9mZHMtYXV0aC5yb3gudm4iLCJhdWQiOlsiUG9ydGFsIiwiRklMRSIsIklOVEciLCJJTlYiLCJNQUlMIiwiTURNIiwiTk9USSIsIlBPTSIsIlJQVCJdLCJjbGllbnRfaWQiOiJGRFNfV0VCIiwic3ViIjoiZDBlMTczNTAtNDE5MC00MjcxLTg3OGEtM2I4Y2VkYWMyOGQ1IiwiYXV0aF90aW1lIjoxNzM3MDgzNTc3LCJpZHAiOiJsb2NhbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IsOBSSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3N1cm5hbWUiOiJQSOG6oE0gxJDhu6hDIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRjAwMDA0NDIiLCJnaXZlbl9uYW1lIjoiw4FJIiwiZmFtaWx5X25hbWUiOiJQSOG6oE0gxJDhu6hDIiwicm9sZSI6WyJhZG1pbiIsIklUIl0sInBob25lX251bWJlciI6IjEyMzQ1Njc4OTAiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOiJGYWxzZSIsImVtYWlsIjoiYWkucGRAZmFtaW1hLnZuIiwiZW1haWxfdmVyaWZpZWQiOiJGYWxzZSIsIm5hbWUiOiJGMDAwMDQ0MiIsInNpZCI6IjU3MzkyMTBBRTAxMEQzMDQxNzlENTc4MEQxRkIxQzI1IiwiaWF0IjoxNzM3MDgzNTc3LCJzY29wZSI6WyJvcGVuaWQiLCJQb3J0YWwiLCJGSUxFIiwiSU5URyIsIklOViIsIk1BSUwiLCJNRE0iLCJOT1RJIiwiUE9NIiwiUlBUIl0sImFtciI6WyJwd2QiXX0.mmyE66dbSYcmxJ-KAKluYl87g0pWOeTaaQKRRrFLGxreHrJ0YsJNyce7H9-rYsSo4j9B9vt-onO3GGjZ0dyg-wZWR3AFNkOpNDwxO9NNUUxM5K5h8B12ExTSo3IGGp0z2-UjQKaRVsG1SCgQU9zd7I21D4CTrV0noQX675EDdZRTbFbhYn0lsEJvzKpTyFm8qLFBWegTFT8hL1u2LYczJq7l_ZmUBayZNKxpRi0dyf5BvwmHyzb1Z2JbiOeXchnwhWCMbq4_HBQFrRF2f4iz_GqYBLypyMtaAHz1gBVJCI6UhwPxfbSLG0H31lPnzfPOqFEQpaSal0URjIdYb_n6_Q"
# Global date variables
FROM_DATE = "2025-01-01T00:00:00.000Z"
TO_DATE = "2025-01-31T23:59:59.999Z"
# Fetch store codes
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

# Fetch inventory transfer data
async def fetch_inventory_transfer(session, store_code):
    url = "https://fds-portal.rox.vn/inv/api/app/inventory-transfer/get-list"
    payload = {
        "filterText": "",
        "fromDate": FROM_DATE,
        "isExportStore": False,
        "maxResultCount": 50,
        "skipCount": 0,
        "sorting": "CreationTime desc",
        "statusCode": "INVENTORY_TRANSFER_STATUS.GOODS_ISSUED",
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

# Fetch goods receipt details
async def gr_detail_from_transfer(session, inventory_transfer_id):
    url = f"https://fds-portal.rox.vn/inv/api/app/goods-receipt/gr-detail-from-transfer/{inventory_transfer_id}"

    async with session.get(url) as response:
        if response.status == 200:
            try:
                result = await response.json()
                if result is None:
                    print(f"No data returned for inventoryTransferId={inventory_transfer_id}")
                    return {}

                data = result.get("data", {})
                return {
                    "goodsIssueId": data.get("goodsIssueId"),
                    "goodsIssueCode": data.get("goodsIssueCode"),
                    "goodsReceiptDetail": data.get("goodsReceiptDetail", [])
                }
            except Exception as e:
                print(f"Failed to parse JSON for inventoryTransferId={inventory_transfer_id}. Error: {e}")
                print(await response.text())
                return {}
        else:
            print(f"Failed to call API for inventoryTransferId={inventory_transfer_id}. Status code: {response.status}")
            print(await response.text())
            return {}

# Add goods issue
async def call_add_goods_issue(session, payload):
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

# Main logic
async def main():
    async with aiohttp.ClientSession(headers={"Authorization": BEARER_TOKEN}) as session:
        # Fetch all store codes
        store_codes = await fetch_store_codes(session)
        if not store_codes:
            print("No store codes found. Exiting...")
            return

        print(f"Processing {len(store_codes)} stores...")

        # Loop through each store code
        for store_code in store_codes:
            print(f"Processing store code: {store_code}")

            # Fetch inventory transfers for the store
            codes_and_ids = await fetch_inventory_transfer(session, store_code)

            for item in codes_and_ids:
                inventory_transfer_id = item["id"]
                ref_code = item["code"]

                # Fetch details for the inventory transfer
                transfer_details = await gr_detail_from_transfer(session, inventory_transfer_id)

                if transfer_details:  # Only proceed if data exists
                    payload = {
                        "issuesDateTime": None,
                        "refCode": ref_code,
                        "refId": inventory_transfer_id,
                        "storeName": "DUMMY TRANSFER",
                        "storeCode": store_code,
                        "type": "GOODS_RECEIPT_TYPE.INVENTORY_TRANSFER",
                        "refTypeCode": "INVENTORY_TRANSFER_TYPE.FROM_STORE_TO_STORE",
                        "description": "",
                        "goodsReceiptDetail": transfer_details["goodsReceiptDetail"],
                        "goodsIssueCode": transfer_details["goodsIssueCode"],
                        "goodsIssueId": transfer_details["goodsIssueId"],
                        "receiptDateTime": None,
                        "typeName": "Nhập hàng theo yêu cầu chuyển hàng"
                    }
                    print(f"Adding goods issue details for inventoryTransferId={ref_code}")
                    await call_add_goods_issue(session, payload)
                    print(f"Successfully added goods issue details for inventoryTransferId={ref_code}")
                else:
                    print(f"No goods issue details for inventoryTransferId={inventory_transfer_id}")

# Execute the script
asyncio.run(main())
