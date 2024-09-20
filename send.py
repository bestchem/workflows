from ftplib import FTP, error_perm
import datetime
import paho.mqtt.client as paho
import json
import io
import os
import time
import re
import threading

# Thông tin cấu hình cho các máy chủ
servers = [
    {
        "ftp_ip": "14.162.145.185",
        "ftp_usr": "testftp",
        "ftp_pwd": "mkc12345",
        "mqtt_client_id": "pgd83mm0yfbunrwi5bga",
        "mqtt_username": "6ezx7s6pl7d7bdesiqf2",
        "mqtt_password": "r4s8j4swhnaolncmefki",
        "mqtt_broker": "localhost",
        "mqtt_port": 1883
    },
]

# Thời gian chờ trước khi khởi động lại (giây)
RESTART_DELAY = 60

# Số lần thử lại kết nối
MAX_RETRY = 5

# Tạo lock để đồng bộ hóa truy cập FTP
ftp_lock = threading.Lock()

def parse_listing(line):
    """Phân tích một dòng thông tin từ danh sách tệp để lấy ngày và tên tệp."""
    parts = line.split()
    if len(parts) < 4:
        return None, None
    date_str = f"{parts[0]} {parts[1]}"
    filename = " ".join(parts[3:])
    try:
        file_date = datetime.datetime.strptime(date_str, "%m-%d-%y %I:%M%p")
    except ValueError:
        return None, None
    return file_date, filename

def get_txt_files(ftp_client, path, start_date, end_date):
    """Lấy danh sách các tệp .txt trong một thư mục nhất định dựa trên ngày tháng."""
    files = []
    try:
        ftp_client.cwd(path)
        lines = []
        ftp_client.retrlines('LIST', lines.append)
        for line in lines:
            file_date, filename = parse_listing(line)
            if (filename and filename.endswith('.txt') and
                file_date and start_date <= file_date <= end_date):
                files.append((file_date, filename))
    except Exception as e:
        print(f"Failed to access directory {path}: {e}")
    return sorted(files)

def write_log(log_file, date, processed_files):
    """Ghi thông tin tệp đã xử lý vào tệp log."""
    try:
        with open(log_file, 'a') as log:
            for filename in processed_files:
                log.write(f"{date},{filename}\n")
    except IOError as e:
        print(f"Failed to write to log file: {e}")

def read_log(log_file):
    """Đọc thông tin tệp đã xử lý từ tệp log và trả về tệp đã xử lý gần nhất."""
    processed_files_by_date = {}
    latest_file = None
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as log:
                for line in log:
                    line = line.strip()
                    if line:
                        date, filename = line.split(',', 1)
                        if date not in processed_files_by_date:
                            processed_files_by_date[date] = set()
                        processed_files_by_date[date].add(filename)

                        # Cập nhật tệp đã xử lý gần nhất
                        if latest_file is None or date > latest_file[0]:
                            latest_file = (date, filename)
                        elif date == latest_file[0] and filename > latest_file[1]:
                            latest_file = (date, filename)
        except IOError as e:
            print(f"Failed to read log file: {e}")
    return processed_files_by_date, latest_file

def on_connect(client, userdata, flags, rc, properties=None):
    """Xử lý sự kiện khi kết nối thành công đến MQTT broker."""
    print("Connected with result code " + str(rc))

def on_publish(client, userdata, mid):
    """Xử lý sự kiện khi một thông điệp được xuất bản thành công."""
    print("Message Published: ", mid)

def download_file(ftp_client, filename):
    """Tải xuống một tệp từ máy chủ FTP và trả về nội dung dưới dạng BytesIO."""
    file_buffer = io.BytesIO()
    try:
        ftp_client.retrbinary(f'RETR {filename}', file_buffer.write)
        file_buffer.seek(0)
        return file_buffer
    except error_perm as e:
        print(f"File not found on FTP server: {filename} - {e}")
    except Exception as e:
        print(f"Error downloading file {filename}: {e}")
    return None

def publish_data(client, topic, data):
    """Gửi dữ liệu đến một topic trên MQTT broker."""
    try:
        result = client.publish(topic, json.dumps(data))
        if result.rc == paho.MQTT_ERR_SUCCESS:
            return True
        else:
            print(f"Publish failed with error code: {result.rc}")
    except Exception as e:
        print(f"Failed to publish data: {e}")
    return False

def reconnect_ftp(ftp_ip, ftp_usr, ftp_pwd):
    """Kết nối đến máy chủ FTP, thử lại nếu không thành công."""
    for attempt in range(MAX_RETRY):
        try:
            ftp_client = FTP(ftp_ip, timeout=60)
            ftp_client.login(user=ftp_usr, passwd=ftp_pwd)
            print("Connected to FTP server")
            return ftp_client
        except Exception as e:
            print(f"Attempt {attempt + 1}/{MAX_RETRY}: Could not connect to FTP server: {e}")
            time.sleep(5)
    raise ConnectionError(f"Could not connect to FTP server after {MAX_RETRY} attempts.")

def process_file(filename, file_content):
    """Xử lý nội dung của tệp để trích xuất dữ liệu cần thiết."""
    data = {}
    match = re.search(r'_(\d{14})\.txt$', filename)
    if match:
        data["time"] = match.group(1)

    for line in file_content.splitlines():
        parts = line.strip().split("\t")
        if len(parts) >= 4:
            key = parts[0]
            try:
                value = float(parts[1])
                data[key] = value
            except ValueError:
                print(f"Warning: Invalid data format in file {filename}: {line}")

    return data

def process_files(ftp_client, client, log_file, start_date, end_date):
    """Xử lý các tệp đã xác định trong khoảng thời gian từ start_date đến end_date."""
    processed_files_by_date, _ = read_log(log_file)

    current_date = start_date
    while current_date <= end_date:
        base_path = f'/TestServ/NLaiChau/NT/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}'

        with ftp_lock:
            txt_files = get_txt_files(ftp_client, base_path, start_date, end_date)

        processed_files_today = processed_files_by_date.get(current_date.strftime("%Y-%m-%d"), set())

        for file_date, filename in txt_files:
            if filename in processed_files_today:
                continue

            print(f"Attempting to retrieve file: {filename}")
            file_buffer = download_file(ftp_client, filename)

            if file_buffer is None:
                continue

            file_content = file_buffer.getvalue().decode('utf-8')
            data = process_file(filename, file_content)

            print("Data to be sent:")
            print(json.dumps(data, indent=2))

            if publish_data(client, "devices", data):
                print("Data sent successfully.")
                write_log(log_file, current_date.strftime("%Y-%m-%d"), [filename])
            else:
                print("Failed to send data after multiple attempts.")

        current_date += datetime.timedelta(days=1)

def check_and_send_latest_files(ftp_client, client, log_file):
    """Kiểm tra và gửi các tệp mới nhất từ máy chủ FTP."""
    processed_files_by_date, _ = read_log(log_file)

    while True:
        current_date = datetime.datetime.now()
        base_path = f'/TestServ/NLaiChau/NT/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}'

        with ftp_lock:
            txt_files = get_txt_files(ftp_client, base_path, current_date, current_date)

        processed_files_today = processed_files_by_date.get(current_date.strftime("%Y-%m-%d"), set())

        for file_date, filename in txt_files:
            if filename in processed_files_today:
                continue

            print(f"Found new file: {filename}")
            file_buffer = download_file(ftp_client, filename)
            if file_buffer is None:
                continue

            file_content = file_buffer.getvalue().decode('utf-8')
            data = process_file(filename, file_content)

            print("Latest data to be sent:")
            print(json.dumps(data, indent=2))

            if publish_data(client, "devices", data):
                print("Latest data sent successfully.")
                write_log(log_file, current_date.strftime("%Y-%m-%d"), [filename])
            else:
                print("Failed to send latest data after multiple attempts.")

        time.sleep(60)  # Kiểm tra lại sau 60 giây

def main():
    """Hàm khởi động chương trình và xử lý kết nối đến FTP và MQTT."""
    log_file = 'upload_log.txt'

    while True:
        for server in servers:
            try:
                ftp_client = reconnect_ftp(server['ftp_ip'], server['ftp_usr'], server['ftp_pwd'])

                client = paho.Client(client_id=server['mqtt_client_id'], protocol=paho.MQTTv5)
                client.username_pw_set(server['mqtt_username'], server['mqtt_password'])
                client.on_connect = on_connect
                client.on_publish = on_publish
                client.connect(server['mqtt_broker'], server['mqtt_port'])
                client.loop_start()

                start_date = datetime.datetime(2024, 9, 1)  # Adjust the start date as needed
                end_date = datetime.datetime(2024, 12, 31)

                # Xử lý các tệp đã xác định
                process_files(ftp_client, client, log_file, start_date, end_date)

                # Tạo luồng kiểm tra và gửi các tệp mới nhất
                latest_data_thread = threading.Thread(target=check_and_send_latest_files, args=(ftp_client, client, log_file), daemon=True)
                latest_data_thread.start()

                # Chờ luồng kiểm tra
                latest_data_thread.join()

                client.loop_stop()
                ftp_client.quit()

            except (TimeoutError, ConnectionError) as e:
                print(f"Connection error: {e}. Restarting in {RESTART_DELAY} seconds...")
                time.sleep(RESTART_DELAY)
                break  # Khởi động lại từ đầu
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Restarting in {RESTART_DELAY} seconds...")
                time.sleep(RESTART_DELAY)
                break  # Khởi động lại từ đầu

if __name__ == "__main__":
    main()
