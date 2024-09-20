# from ftplib import FTP
# import datetime

# def parse_listing(line):
#     # Giả định định dạng danh sách là:
#     # MM-DD-YY  HH:MMAM/PM       <DIR>   hoặc
#     # MM-DD-YY  HH:MMAM/PM     <size> <filename>
#     parts = line.split()
#     if len(parts) < 4:
#         return None, None
#     date_str = f"{parts[0]} {parts[1]}"
#     filename = " ".join(parts[3:])
    
#     try:
#         file_date = datetime.datetime.strptime(date_str, "%m-%d-%y %I:%M%p")
#     except ValueError:
#         return None, None
    
#     return file_date, filename

# def get_files_in_date_range(ftp_client, start_date, end_date):
#     # Lấy danh sách thư mục
#     files = []
#     ftp_client.retrlines('LIST', files.append)
    
#     filtered_files = []
    
#     for line in files:
#         file_date, filename = parse_listing(line)
#         if file_date and start_date <= file_date <= end_date:
#             filtered_files.append(filename)
    
#     return filtered_files

# # Thông tin máy chủ FTP
# ftp_ip = "14.162.145.185"
# ftp_usr = "testftp"
# ftp_pwd = "mkc12345"

# # Xác định khoảng thời gian để lọc
# start_date = datetime.datetime(2024, 9, 16, 0, 0)
# end_date = datetime.datetime(2024, 9, 16, 23, 59)

# # Kết nối tới máy chủ FTP
# ftp_client = FTP(ftp_ip)
# ftp_client.login(user=ftp_usr, passwd=ftp_pwd)

# # Lấy các tệp trong khoảng thời gian đã chỉ định
# files_in_date_range = get_files_in_date_range(ftp_client, start_date, end_date)

# # In ra các tên tệp đã lọc
# for filename in files_in_date_range:
#     print(filename)

# # Đóng kết nối FTP
# ftp_client.quit()

from ftplib import FTP
import datetime

ftp_ip = "14.162.145.185"
ftp_usr = "testftp"
ftp_pwd = "mkc12345"

ftp_client = FTP(ftp_ip)
ftp_client.login(user=ftp_usr, passwd = ftp_pwd)

files = []
ftp_client.retrlines('LIST', files.append)


# ftp_client.welcome


ftp_client.pwd()

print(ftp_client.retrlines('LIST'))


