import pandas as pd

# Đọc file CSV của bạn
file_path = 'C:\\Users\\phamd\\Desktop\\exportdataauto\\Nhi\\T5.csv'
df = pd.read_csv(file_path)

# Hiển thị kiểu dữ liệu của từng cột
print("Kiểu dữ liệu của các cột:")
for column in df.columns:
    print(f"Cột '{column}': {df[column].dtype}")

# Nếu bạn muốn thông tin chi tiết hơn
print("\nThông tin chi tiết của DataFrame:")
print(df.info())