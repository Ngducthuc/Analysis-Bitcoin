import ccxt
import csv
import time

# Khởi tạo đối tượng Binance
exchange = ccxt.binance()

# Cài đặt các tham số
symbol = 'BTC/USDT'
timeframe = '1m'
limit = 1000  # Giới hạn số lượng nến mỗi lần gọi
start_time = '2023-11-06T00:00:00.000Z'  # Thời gian bắt đầu từ 2022-07-05T02:39:00.000Z
end_time = exchange.milliseconds()  # Thời gian kết thúc là thời gian hiện tại (milliseconds)

# Chuyển đổi thời gian bắt đầu từ định dạng ISO 8601 thành milliseconds
since = exchange.parse8601(start_time)

# Đường dẫn đến file CSV
file_path = 'bitcoin_data_8_months.csv'

# Kiểm tra nếu file CSV đã tồn tại để quyết định có cần ghi tiêu đề cột hay không
file_exists = False
try:
    with open(file_path, mode='r', newline='') as file:
        file_exists = True
except FileNotFoundError:
    file_exists = False
# Mở file CSV với chế độ append (ghi tiếp vào cuối file)
with open(file_path, mode='a', newline='') as file:
    writer = csv.writer(file)

    # Nếu file chưa tồn tại, ghi tiêu đề cột vào đầu file
    if not file_exists:
        writer.writerow(['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])

    # Lấy dữ liệu từ start_time đến hiện tại
    while since < end_time:
        # Lấy dữ liệu nến
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

        # Nếu không có dữ liệu trả về, dừng lại
        if not ohlcv:
            print("Không có dữ liệu trả về, dừng lại.")
            break

        # Ghi vào file CSV
        for o in ohlcv:
            timestamp = exchange.iso8601(o[0])
            open_price = o[1]
            high_price = o[2]
            low_price = o[3]
            close_price = o[4]
            volume = o[5]
            writer.writerow([timestamp, open_price, high_price, low_price, close_price, volume])

        # Cập nhật thời gian since cho lần gọi tiếp theo (thời gian của nến cuối cùng)
        since = ohlcv[-1][0] + 1  # Thêm 1ms để tránh lấy trùng dữ liệu của nến cuối cùng

        print(f"Đã lấy dữ liệu từ {timestamp}")

        time.sleep(1)  # Tạm dừng một chút giữa các lần gọi API để tránh bị lỗi từ server

print(f"Dữ liệu đã được ghi tiếp vào {file_path}")
