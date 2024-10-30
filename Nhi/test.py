import pandas as pd

# Đọc file CSV
file_path = 'C:\\Users\\phamd\\Desktop\\exportdataauto\\Nhi\\T5.csv'
df = pd.read_csv(file_path)

# Xử lý các cột int64
df['RETAIL PRICE'] = df['RETAIL PRICE'].str.replace(',', '').astype('int64')
df['RETAIL TOTAL'] = df['RETAIL TOTAL'].str.replace(',', '').astype('int64')

# Xử lý các cột float64 
df['COST PRICE'] = df['COST PRICE'].str.replace(',', '').astype('float64')
df['COST TOTAL'] = df['COST TOTAL'].str.replace(',', '').astype('float64')

# Lưu lại file CSV
df.to_csv(file_path, index=False)

# Kiểm tra lại định dạng
print("Kiểm tra định dạng:")
print(f"RETAIL PRICE: {df['RETAIL PRICE'].dtype}")
print(f"RETAIL TOTAL: {df['RETAIL TOTAL'].dtype}")
print(f"COST PRICE: {df['COST PRICE'].dtype}")
print(f"COST TOTAL: {df['COST TOTAL'].dtype}")