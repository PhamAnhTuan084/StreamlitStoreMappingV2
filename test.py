import streamlit as st
import pandas as pd
import warnings
import re
import numpy as np
import unidecode
import phonenumbers as pn
from rapidfuzz import fuzz
from geopy import distance
from geopy.distance import geodesic
from tqdm import tqdm
warnings.filterwarnings('ignore')

def read_file():
    Province = pd.read_excel("Province.xlsx")
    teleco1 = pd.read_excel("Teleco Master 202307.xlsx", sheet_name='Di động')
    teleco2= pd.read_excel("Teleco Master 202307.xlsx", sheet_name='Cố định')
    OptionalText = pd.read_excel("Op_Add.xlsx")
    text_remove = pd.read_excel("remove_list_hvn.xlsx")        
    text_remove_2 = pd.read_excel("remove_list_vigo.xlsx")
    remove_name = pd.read_excel("remove_listname_hvn.xlsx")
    remove_name_2 = pd.read_excel("remove_listname_vigo.xlsx")

    return Province, teleco1, teleco2, OptionalText, text_remove, text_remove_2, remove_name, remove_name_2

# Xem coi có nằm trong giới hạn vùng lãnh thổ của Việt Nam
def xet_latlng(HVN):
    min_lat, max_lat = 8.18, 23.39
    min_lon, max_lon = 102.14, 109.46

    # Lọc các dòng không nằm trong khu vực của Việt Nam
    filtered_HVN = HVN[
        ~((min_lat <= HVN['Latitude']) & (HVN['Latitude'] <= max_lat) &
        (min_lon <= HVN['Longitude']) & (HVN['Longitude'] <= max_lon))
    ]

    # Lấy ra những HVN mà OutletID không nằm trong filtered_HVN
    unfiltered_outlets = HVN[~HVN['OutletID'].isin(filtered_HVN['OutletID'])]

    return unfiltered_outlets

# Hàm để chuẩn hóa và xóa dấu cách dư thừa, xóa dấu diacritics
def normalize_and_remove_accents(df, columns):
    from unidecode import unidecode
    for col in columns:
        df[col] = df[col].apply(lambda x: unidecode(x).lower().strip() if pd.notna(x) else x)
    return df

# Xét phân cấp thánh phố, huyện, xã và thị trấn
def xet_phancap(hvn_df, province_df):
    hvn_df['WardName'].fillna('', inplace=True)
    hvn_df['WardName'].replace({None: ''}, inplace=True)
    hvn_df['WardName'].replace({'NULL': ''}, inplace=True)

    hvn_df['CustomerAddress'].replace({'NULL': ''}, inplace=True)
    hvn_df['CustomerAddress'].replace({None: ''}, inplace=True)
    hvn_df['CustomerAddress'].replace({'NULL': ''}, inplace=True)

    hvn_df['DistrictName'].replace({'NULL': ''}, inplace=True)
    hvn_df['DistrictName'].replace({None: ''}, inplace=True)
    hvn_df['DistrictName'].replace({'NULL': ''}, inplace=True)

    # Chuẩn hóa và xóa dấu cách dư thừa cho các cột cần thiết trong hvn_df
    hvn_df[['ProvinceName', 'DistrictName', 'WardName']] = normalize_and_remove_accents(hvn_df[['ProvinceName', 'DistrictName', 'WardName']], ['ProvinceName', 'DistrictName', 'WardName'])

    # Chuẩn hóa và xóa dấu cách dư thừa, xóa dấu diacritics cho các cột cần thiết trong province_df
    province_df[['Tỉnh Thành Phố', 'Quận Huyện', 'Phường Xã']] = normalize_and_remove_accents(province_df[['Tỉnh Thành Phố', 'Quận Huyện', 'Phường Xã']], ['Tỉnh Thành Phố', 'Quận Huyện', 'Phường Xã'])

    # Tạo list để lưu thông tin các hvn_outlet_id không khớp
    invalid_outlets_data = []

    # Tìm các HVN_OutletID có ProvinceName không nằm trong danh sách của Province
    for index, row in hvn_df.iterrows():
        hvn_outlet_id = row['OutletID']
        province_name = row['ProvinceName']
        district_name = row['DistrictName']
        ward_name = row['WardName']

        # Kiểm tra xem ProvinceName có trong danh sách của Province không
        if province_name in province_df['Tỉnh Thành Phố'].values:
            # Kiểm tra xem DistrictName có khớp với ['Quận Huyện'] tại ['Tỉnh Thành Phố'] không
            province_row = province_df[province_df['Tỉnh Thành Phố'] == province_name]
            if district_name not in province_row['Quận Huyện'].values:
                invalid_outlets_data.append(row.to_dict())
            else:
                ward_row = province_row[province_row['Quận Huyện'] == district_name]
                if ward_name not in ward_row['Phường Xã'].values:
                    invalid_outlets_data.append(row.to_dict())

        else:
            invalid_outlets_data.append(row.to_dict())

    # Tạo DataFrame từ list thông tin các hvn_outlet_id không khớp
    invalid_outlets_df = pd.DataFrame(invalid_outlets_data)

    return invalid_outlets_df

#clean phone numbers
def clean_phone_data(orig_phone):
    try:
        new_phone = pn.format_number(pn.parse(orig_phone, 'VN'), pn.PhoneNumberFormat.E164)
    except: # NumberParseException
        return 'nophonedata'
    return new_phone

# remove phone numbers beginning with 12345, 012345
def remove_invalid_phone(df_column):
    for num in df_column:
        for s_rm in ['12345', '012345']:
              if num.startswith(s_rm):
                  new_num = '0'
                  df_column = df_column.replace([num], new_num)
    return df_column

# Lấy ra những dòng có số đt và không có số đt
def xuly_phone(HVN, Vigo):
    HVN['Phone'] = HVN['Phone'].apply(lambda x: str(x) if type(x) is not str else x)
    Vigo['Phone'] = Vigo['Phone'].apply(lambda x: str(x) if type(x) is not str else x)

    HVN['Phone'] = remove_invalid_phone(HVN['Phone'])
    Vigo['Phone'] = remove_invalid_phone(Vigo['Phone'])

    HVN['Phone']= HVN['Phone'].apply(clean_phone_data)
    Vigo['Phone'] = Vigo['Phone'].apply(clean_phone_data)

    HVN['Phone'] = HVN['Phone'].apply(lambda x: x.replace(' ', ''))
    Vigo['Phone'] = Vigo['Phone'].apply(lambda x: x.replace(' ', ''))

    HVN_nophone = HVN[HVN['Phone'] == 'nophonedata'].copy()
    HVN_phone = HVN.loc[lambda df: ~df.OutletID.isin(HVN_nophone['OutletID'])]

    Vigo_nophone = Vigo[Vigo['Phone'] == 'nophonedata'].copy()
    Vigo_phone = Vigo.loc[lambda df: ~df.OutletID.isin(Vigo_nophone['OutletID'])]

    # Thay thế các số điện thoại bắt đầu bằng "+84" thành "0" trong cột 'Phone'
    HVN_phone['Phone'] = HVN_phone['Phone'].replace(to_replace=r'^\+84', value='0', regex=True)

    # Loại bỏ giá trị trùng lặp từ cột 'Phone'
    HVN_phone['Phone'] = HVN_phone['Phone'].drop_duplicates()
    HVN_phone_na = HVN_phone[HVN_phone['Phone'].isna()]
    HVN_phone_notna = HVN_phone.dropna(subset=['Phone'])

    Vigo_phone['Phone'] = Vigo_phone['Phone'].replace(to_replace=r'^\+84', value='0', regex=True)
    Vigo_phone['Phone'] = Vigo_phone['Phone'].drop_duplicates()
    Vigo_phone_na = Vigo_phone[Vigo_phone['Phone'].isna()]
    Vigo_phone_notna = Vigo_phone.dropna(subset=['Phone'])

    return HVN_nophone, Vigo_nophone, HVN_phone_na, HVN_phone_notna, Vigo_phone_na, Vigo_phone_notna

# Kiểm tra đầu số điền thoại có đúng các nhà mạng hay mã vùng mới nhất. Nếu còn dùng đầu số cũ thì thay đầu số mới
def check_dausomoi(HVN_phone_notna, teleco1):
    matched_rows = []
    not_matching_rows = []

    # Iterate through each row in HVN_phone_notna['Phone']
    for index, phone_number in HVN_phone_notna['Phone'].items():
        match_found = False

        # Check for match in Đầu Số Mới
        for teleco1_prefix_moi in teleco1['Đầu Số Mới']:
            prefix_length = len(str(teleco1_prefix_moi))
            prefix = str(phone_number)[:prefix_length]
            condition_moi = str(teleco1_prefix_moi).startswith(prefix)

            if condition_moi:
                match_found = True
                break

        # Check for match in Đầu Số Cũ if not found in Đầu Số Mới
        if not match_found:
            for teleco1_prefix_cu in teleco1['Đầu Số Cũ']:
                prefix_length_cu = len(str(teleco1_prefix_cu))
                prefix_cu = str(phone_number)[:prefix_length_cu]
                condition_cu = str(teleco1_prefix_cu).startswith(prefix_cu)

                if condition_cu:
                    match_found = True

                    # Update the phone number in HVN_phone_notna['Phone']
                    matching_row_cu = teleco1.loc[teleco1['Đầu Số Cũ'] == teleco1_prefix_cu, 'Đầu Số Mới']
                    new_prefix_cu = matching_row_cu.iloc[0]
                    new_phone_number_cu = new_prefix_cu + str(phone_number)[prefix_length_cu:]
                    HVN_phone_notna.at[index, 'Phone'] = new_phone_number_cu

                    break

        # Add the row to the appropriate list based on match_found
        if match_found:
            matched_rows.append(HVN_phone_notna.loc[index])
        else:
            not_matching_rows.append(HVN_phone_notna.loc[index])

    # Create the DataFrames
    matched_df = pd.DataFrame(matched_rows, columns=HVN_phone_notna.columns)
    not_matching_df = pd.DataFrame(not_matching_rows, columns=HVN_phone_notna.columns)
    
    return matched_df, not_matching_df

# Kiểm tra đầu số điền thoại có đúng các nhà mạng hay mã vùng mới nhất. Nếu còn dùng đầu số cũ thì thay đầu số mới
def check_mavungmoi(HVN_phone_notna_2, teleco2):
    # Clear any existing rows in matched_df_2 and not_matching_df_2
    matched_rows_2 = []
    not_matching_rows_2 = []

    # Iterate through each row in HVN_phone_notna_2['Phone']
    for index, phone_number in HVN_phone_notna_2['Phone'].items():
        match_found = False

        # Check for match in 'Mã vùng điện thoại mới\t'
        for teleco2_prefix_moi in teleco2['Mã vùng điện thoại mới\t']:
            prefix_length = len(str(teleco2_prefix_moi))
            prefix = str(phone_number)[:prefix_length]
            condition_moi = str(teleco2_prefix_moi).startswith(prefix)

            if condition_moi:
                match_found = True
                break

        # Check for match in 'Mã vùng điện thoại cũ\t' if not found in 'Mã vùng điện thoại mới\t'
        if not match_found:
            for teleco2_prefix_cu in teleco2['Mã vùng điện thoại cũ\t']:
                prefix_length_cu = len(str(teleco2_prefix_cu))
                prefix_cu = str(phone_number)[:prefix_length_cu]
                condition_cu = str(teleco2_prefix_cu).startswith(prefix_cu)

                if condition_cu:
                    match_found = True

                    # Update the phone number in HVN_phone_notna_2['Phone']
                    matching_row_cu = teleco2.loc[teleco2['Mã vùng điện thoại cũ\t'] == teleco2_prefix_cu, 'Mã vùng điện thoại mới\t']
                    new_prefix_cu = matching_row_cu.iloc[0]
                    new_phone_number_cu = new_prefix_cu + str(phone_number)[prefix_length_cu:]
                    HVN_phone_notna_2.at[index, 'Phone'] = new_phone_number_cu

                    break

        # Add the row to the appropriate list based on match_found
        if match_found:
            matched_rows_2.append(HVN_phone_notna_2.loc[index])
        else:
            not_matching_rows_2.append(HVN_phone_notna_2.loc[index])

    # Create the DataFrames
    matched_df_2 = pd.DataFrame(matched_rows_2, columns=HVN_phone_notna_2.columns)
    not_matching_df_2 = pd.DataFrame(not_matching_rows_2, columns=HVN_phone_notna_2.columns)

    return matched_df_2, not_matching_df_2

# Lọc tạo danh sách thỏa và không thỏa số điện thoại
def tao_danh_sach_thoa_khongthoa(teleco1, teleco2, HVN_phone_notna, Vigo_phone_notna, HVN_nophone, HVN_phone_na, Vigo_nophone, Vigo_phone_na):
    teleco1['Đầu Số Cũ'] = '0' + teleco1['Đầu Số Cũ'].astype(str)
    teleco1['Đầu Số Mới'] = '0' + teleco1['Đầu Số Mới'].astype(str)

    teleco2['Mã vùng điện thoại cũ\t'] = '0' + teleco2['Mã vùng điện thoại cũ\t'].astype(str)
    teleco2['Mã vùng điện thoại mới\t'] = '0' + teleco2['Mã vùng điện thoại mới\t'].astype(str)

    HVN_dausomoi, HVN_nodausomoi= check_dausomoi(HVN_phone_notna, teleco1)
    HVN_mavungmoi, HVN_nomavungmoi= check_mavungmoi(HVN_nodausomoi, teleco2)

    Vigo_dausomoi, Vigo_nodausomoi= check_dausomoi(Vigo_phone_notna, teleco1)
    Vigo_mavungmoi, Vigo_nomavungmoi= check_mavungmoi(Vigo_nodausomoi, teleco2)

    HVN_khongthoa = pd.concat([HVN_nophone, HVN_phone_na])
    HVN_khongthoa = pd.concat([HVN_khongthoa, HVN_nomavungmoi])

    Vigo_khongthoa = pd.concat([Vigo_nophone, Vigo_phone_na])
    Vigo_khongthoa = pd.concat([Vigo_khongthoa, Vigo_nomavungmoi])

    HVN_thoa = pd.concat([HVN_dausomoi, HVN_mavungmoi])
    Vigo_thoa = pd.concat([Vigo_dausomoi, Vigo_mavungmoi])

    return HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa

# Thay thế những từ trùng với thông tin trong cột Optional
def replace_optional_text(row, remove_name):
    outlet_name = str(row['Outlet_Name'])  # Convert to string

    if pd.isna(outlet_name):
        return np.nan  # Skip replacement for NaN values

    for index, remove_row in remove_name.iterrows():
        optional_text = str(remove_row['Optional'])  # Ensure string conversion
        replace_text = str(remove_row['Replace'])

        outlet_name = outlet_name.replace(optional_text, replace_text)

    return outlet_name

# Loại bỏ khoảng trắng thừa
def preprocess_address(address):
    from unidecode import unidecode
    
    address = re.sub(r'\s+', ' ', address).strip()
    return unidecode(address)

# Tạo cột clean_name
def xuly_hvnname(HVN, remove_name):
    HVN['Outlet_Name'] = HVN['OutletName'].str.lower()
    HVN['Outlet_Name'].fillna('', inplace=True)
    HVN['Outlet_Name'].replace({None: ''}, inplace=True)
    HVN['Outlet_Name'].replace({'NULL': ''}, inplace=True)

    remove_name['Replace'].fillna('', inplace=True)
    remove_name['Replace'].replace({None: ''}, inplace=True)
    remove_name['Replace'].replace({'NULL': ''}, inplace=True)

    # Convert the "Replace" column in remove_name to strings
    remove_name['Replace'] = remove_name['Replace'].astype(str)

    HVN['clean_Outlet_Name'] = HVN.apply(lambda row: replace_optional_text(row, remove_name), axis=1)
    HVN['clean_Outlet_Name'] = HVN['clean_Outlet_Name'].apply(lambda x: re.sub(r'\s+', ' ', x))

    return HVN

def round1(HVN_thoa, Vigo_thoa):
    if (len(HVN_thoa['Phone'].unique()) < len(Vigo_thoa['Phone'].unique())):
        phone_list = HVN_thoa['Phone'].unique().tolist() 
    else:
        phone_list = Vigo_thoa['Phone'].unique().tolist()
    
    phonenum_map = pd.DataFrame()
    for phone_num in tqdm(phone_list):
        Data_df_phone = HVN_thoa[HVN_thoa['Phone'] == phone_num]
        VIGO_df_phone = Vigo_thoa[Vigo_thoa['Phone'] == phone_num]
        
        Data_df_phone['key'] = 1
        VIGO_df_phone['key'] = 1
        df_merged_by_phone = pd.merge(Data_df_phone, VIGO_df_phone, on='key', suffixes=('_file1', '_file2'))
        del df_merged_by_phone['key']
        phonenum_map = pd.concat([phonenum_map, df_merged_by_phone])
    
    return phonenum_map

def is_valid_format(address):
    parts = address.split(', ')
    if len(parts) == 2:
        first_part = parts[0].split(' ')
        if len(first_part) >= 2 and first_part[1] == 'Ấp' and 'Thị Trấn' in parts[1]:
            return True
    return False

def is_valid_format_1(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b\d+[a-zA-Z]*\s*Ấp[^\d,]+\b')
    match = pattern.match(address)
    return bool(match and match.group(0) == address)

def is_valid_format_2(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b\d+\s*Kênh Xáng,\s*Ấp (\d+),\s*Xã (\D+)')
    match = pattern.match(address)
    return bool(match and match.group(1) and match.group(2))

def is_valid_format_3(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b30 Cầu Đường Bàng,\s*Xã (\D+)')
    match = pattern.match(address)
    return bool(match and match.group(1))

def is_valid_format_4(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b29 Thuận Hòa')
    return bool(re.match(pattern, address))

def is_valid_format_5(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b(\d+\s*Hòa Lạc C)\s*,\s*Xã (\D+)')
    return bool(re.match(pattern, address))

def is_valid_format_6(address):
    if pd.isna(address):
        return False
    pattern = re.compile(r'\b(\d+\s*Cây Khô Lớn)\s*,\s*Xã (\D+)')
    return bool(re.match(pattern, address))

def loc_hvn_r2(HVN_r2):
    HVN_r2['WardName'].fillna('', inplace=True)
    HVN_r2['WardName'].replace({None: ''}, inplace=True)
    HVN_r2['WardName'].replace({'NULL': ''}, inplace=True)

    HVN_r2['CustomerAddress'].fillna('', inplace=True)
    HVN_r2['CustomerAddress'].replace({None: ''}, inplace=True)
    HVN_r2['CustomerAddress'].replace({'NULL': ''}, inplace=True)
    HVN_r2['CustomerAddress'] = HVN_r2['CustomerAddress'].str.strip()

    HVN_r2['DistrictName'].fillna('', inplace=True)
    HVN_r2['DistrictName'].replace({None: ''}, inplace=True)
    HVN_r2['DistrictName'].replace({'NULL': ''}, inplace=True)

    HVN_r2['OutletName'].fillna('', inplace=True)
    HVN_r2['OutletName'].replace({None: ''}, inplace=True)
    HVN_r2['OutletName'].replace({'NULL': ''}, inplace=True)

    HVN_r2['CustomerAddress'].fillna('', inplace=True)
    HVN_r2['CustomerAddress'].replace({None: ''}, inplace=True)
    HVN_r2['CustomerAddress'].replace({'NULL': ''}, inplace=True)
    HVN_r2['CustomerAddress'] = HVN_r2['CustomerAddress'].str.strip()

    HVN_digit_mask = HVN_r2[HVN_r2['CustomerAddress'].str.match(r'^\d')]
    HVN_notdigit_mask = HVN_r2[~HVN_r2['CustomerAddress'].str.match(r'^\d')]

    word_list = [
    'Đường', 'đường', 'Đ\\.', 'd\\.', '827', 'Trần Thành Đại', 'N27', 'Huỳnh Văn Thanh', 'Võ Văn Thành', 'Nguyễn Hoà Luông', 'Mai Thị Non', 'Lương Văn Bang', 'Truong Binh - Phuoc Lâm', 'Lộ 837', 'Huỳnh Thị Mai', 'ĐƯỜNG 836','Tiên Đông Thượng', 'Lộ Tránh', 'Công Lý', 'Ban Cao', 'CaoVăn Lầu', 'Bạch Đằng', 'Lộ Thầy Cai', 'Bình An', 'Nguyễn Công Truc', 'Long Khốt', 'Duong', 'duong', 'Bà Chánh Thâu','Trần Ngọc Giải', 'Dương Văn Dương', 'Đ12', 'Lê Văn Sáu', 'Nguyễn Văn Tư', 'Lê Văn Tám', 'Đt', 'Nguyễn đình chiểu', 'Trương Văn Kỉnh', 'Tiền Phong', 'Tô Thị Huỳnh', 'Đặng Ngọc Sương', 'phan đình Phùng', 'Lê Văn Khuyên', 'Nguyễn Văn Tiếp', 'Nguyễn Văn Cương', 'Lê Văn Tường', 'Võ Văn Môn', 'Lê Lợi', 'Nguyễn Trãi', 'Hùng Vương', 'Nguyễn Thị Nhỏ', 'Nguyễn Thị Bảy', 'Nguyễn Chí Thanh', 'Thống Chế Sỹ', 'Phạm Văn Thành', 'Huynh Chau So', 'Huỳnh Châu Sổ', 'Nguyễn Đình Chiểu', 'Đỗ Tường Phong', 'Sơn Thông', 'Đỗ Trình Thoại', 'Nguyễn Thông', 'Lãnh Binh Thái', 'Phạm Văn Thành', 'Trần Công Oanh', 'Đồng Khởi', 'Châu Thị Kim', 'Lê Văn Tưởng', 'Phạm Ngũ Lão', 'Nguyễn Văn Trổi', 'Nguyễn Thái Bình', 'Hoàng Hoa Thám', 'Đặng Văn Truyện', 'Huỳnh Văn Đảnh', 'Nguyễnvăn Trưng', 'Vành Đai', 'Nguyen Thong', 'Phú Hoà', 'phan đình Phùng', 'Hoà Hảo', 'Tiền Phong', 'Nguyễn Thông', 'Nguyen Trung Truc', 'Trương Định', 'Nguyễn Thị Định', 'Nguyễn Văn Nhâm', 'QL', 'Ql', 'Ba Sa Gò Mối', 'Mỹ Thuận', 'Bùi Hữu Nghĩa', 'Châu Thị Kim', 'Cử Luyện', 'Nguyễn Huệ', 'Hoàng Hoa Thám', 'Nguyễn Văn Tư', 'Nguyễn Huệ', 'Đoàn Hoàng Minh', 'Phan Văn Mảng', 'Đồng Khởi', 'Nguyễn Văn Tuôi', 'Tán Kế', 'Luu Van Te', 'Châu Thị Kim', 'Trần Văn Đấu', 'Quách Văn Tuấn', 'Sương Nguyệt Ánh', 'Châu Văn Bảy', 'Nguyễn Trung Trực', 'Nguyễn Văn Cánh', 'nguyễn minh đường', 'Nguyễn Thị Hạnh', 'Đỗ Đình Thoại', 'Nguyễn Du', 'Châu Thị Kim', 'Trương Vĩnh Ký', 'Nguyen Thi Dinh', 'Hồ Văn Huê', 'Nguyễn Đáng', 'Vĩnh Phú', 'Châu Thị Kim', 'Đoàn Hoàng Minh', 'Huỳnh Việt Thanh', 'Nguyễn Hữu Thọ', 'Luong Van Chan', 'Phan Đình Phùng', 'Phạm Văn Ngô', 'Nguyen Thong', 'Đương 30/4', 'CMT8', 'cmt8', 'Huỳnh Tấn Phát', 'Hương Lộ', 'HL', 'Trần Văn Đạt', 'Quốc Lộ', 'Tỉnh lộ', 'Dt', 'DT', 'Nguyễn An Ninh', 'Lê Hồng Phong', 'Lộc Trung', 'Lê Minh Xuân', 'Mai Thị Tốt', 'Phạm Văn Ngô', 'TL', 'Lê Thị Trâm', 'Quoc Lo', 'Tỉnh Lộ', 'Nguyễn Thị Minh Khai', 'Phạm Văn Chiên', 'Võ Văn Nhơn', 'Lê Hữu Nghĩa', 'Phan Văn Lay', 'Châu Văn Giác', 'Nguyễn Huỳnh Đức', 'Phan Văn Mãng', 'Bùi Tấn', 'Lưu Nghiệp Anh', 'Lê Hồng Phong', 'Nguyễn Văn Siêu', 'Nguyễn Văn Quá', 'Vo Cong Ton', 'Thái Hữu Kiểm', 'Trần Minh Châu', 'Lý Thường Kiệt', 'Phạm Văn Ngũ', 'Trần Phong Sắc', 'Nguyễn Văn Kỉnh', '827D', 'Phan Văn Mãng', 'Nguyễn Cửu Vân', 'Bùi Thị Hồng', 'Trần Thế Sinh', 'Hoàng Anh', 'Huỳnh Văn Tạo', 'Nguyễn Văn Trung', 'Đỗ Tường Tự', 'Nguyễn Văn Trưng', 'Tl', 'ĐT', 'Trần Phú', 'Nguyễn Thị Diện', '19/5', 'Hl', 'Nguyễn Văn Tiến', 'Phan Van Lay', 'Nguyen Thi Minh Khai', 'Đỗ Tường Tự', 'Thủ Khoa Huân', 'Thanh Hà', 'Tân Long', 'Truong Bình', 'Huỳnh Thị Lung', ' Phan Thanh Giảng', ' Phan Thanh Giảnp', 'Đinh Viết Cừu', 'Võ Nguyên Giáp', 'Lộ Dừa', 'Truong Vinh Ky', 'Phan Văn Tình', 'Trịnh Quang Nghị', 'Nguyễn Minh Trung', 'Ca Văn Thỉnh', 'Bàu Sen', 'Chu Văn An', 'Trần Thị Non', 'Lê lợi', 'Võ Công Tồn', 'NguyễnTrung Trực', 'Phan Van Mang', 'Phan Văn Mảng', 'Phan Văn Mãng', 'Nguyễn Hòa Luông', 'Nguyễn Văn Trỗi', 'Võ Văn Kiệt', 'Huỳnh Văn Gấm', 'Thanh hà', 'Hòa Lạc C', 'phạm văn ngô', 'Phạm Văn Ngô', 'Phước Toàn', 'Vỏ Duy Tạo', 'Lảnh Binh Thái', 'Nguyen Cuu Van', 'Trần Phú', 'Cao Văn Lầu', 'Điện Biên Phủ', 'Bạch Đằng' 'Huỳnh Văn Thanh', 'Võ Văn Tần', 'Phan Văn Tình', 'Chu Van An', 'Thuận Hòa', 'Vũ Đình Liệu', 'Đồng Văn Dẫn', 'Mậu Thân', 'Cao Thị Mai', 'Nguyễn Văn Rành', 'Nguyễn Công Trung', 'Nguyễn Minh Trường', 'Nguyễn Quang Đại', 'Hai Bà Trưng', 'Võ Thị Sáu', 'Trần Quốc Tuấn', 'Lê Văn Kiệt', 'Nguyễn Văn Tạo', '30 Tháng 4', '3/2', 'Phan đình phùng', 'Thủ Khoa Huân', 'Phan Văn Tình', 'Hoàng Lam', 'Ngô Quyền', 'Nguyễn Thị Bẹ', 'Phan Văn Đạt', 'Nguyễn Minh Trường', 'Võ Công Tồn', 'Huỳnh Văn Gấm', 'Huỳnh Văn Lộng', 'Bình Hòa', 'Nguyen Huu Tho', 'Nguyễn Hữu Thọ', 'Võ Công Tồn', 'Trần Phong Sắc', 'Trần Phong Sẳ', 'Phạm Ngọc Tòng', 'Phan Văn Tình', 'Trần Hưng Đạo', 'Nguyễn Văn Rành', 'Nguyễn Văn Cảnh', 'Thủ Khoa Thừa', 'Lê Thị Điền', 'Rạch Tre', 'Trần Hưng Dạo', 'Võ Công Tồn', 'Võ Hồng Cúc', 'Lê Văn Kiệt', 'Phạm Văn Trạch', 'Lê Văn Tao', 'Nguyễn Thiện Thành', 'Huỳnh Hữu Thống', '2 tháng 9', 'Phan Châu Trinh', 'Hoàng Lam', 'Trần Văn Trà', 'NGUYỄN THỊ ÚT', 'Nguyễn Thị Út', 'Bình Trị 2', 'Lê Văn Trần', 'Trưng Nhị', 'Bình hòa', 'Nguyễn ĐìnhChiểu', 'Hương lộ', 'Nguyen Thi Bay', 'Nguyễn Thị Bảy', 'Đt 816', 'huỳnh văn đảnh', 'Huỳnh Văn Đảnh', 'Nguyễn văn tiếp', 'Nguyễn Văn Tiếp', 'Cao Thi Mai', 'Đt825', 'Đặng Văn Búp', '30 Thang 4', 'Nguyễn Bỉnh Khiêm', 'Đt 835B'
    ]

    pattern = '|'.join(word_list)

    df_filtered = HVN_digit_mask[HVN_digit_mask['CustomerAddress'].str.contains(pattern, regex=True)]
    df_notfiltered = HVN_digit_mask[~HVN_digit_mask['CustomerAddress'].str.contains(pattern, regex=True)]

    regex_pattern = r'\b\d+ Ấp [^\d]+, Xã \w+\b'

    ap_ten = df_filtered[df_filtered['CustomerAddress'].str.contains(regex_pattern, regex=True, case=False)]
    non_ap_ten = df_filtered.loc[~df_filtered['CustomerAddress'].str.contains(regex_pattern, regex=True, case=False)]
    so_ap = non_ap_ten[non_ap_ten['CustomerAddress'].str.match(r'^\d+ Ấp [^QLHLĐTHLDlt]+\b(?:(?!QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ|Đinh Viết Cừu|Nguyễn Thông).)*$')]
    noso_ap = non_ap_ten[~non_ap_ten['CustomerAddress'].str.match(r'^\d+ Ấp [^QLHLĐTHLDlt]+\b(?:(?!QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ|Đinh Viết Cừu|Nguyễn Thông).)*$')]
    ap = noso_ap[noso_ap['CustomerAddress'].str.match(r'^\d+ Ấp (?!.*\b(QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ)\b)[^\d]+(\s+\d+)?$')]
    no_ap = noso_ap[~noso_ap['CustomerAddress'].str.match(r'^\d+ Ấp (?!.*\b(QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ)\b)[^\d]+(\s+\d+)?$')]
    xa = no_ap[noso_ap['CustomerAddress'].str.match(r'^\d+ Xã (?!.*\b(QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ)\b)[^\d]+(\s+\d+)?$')]
    no_xa = no_ap[~no_ap['CustomerAddress'].str.match(r'^\d+ Xã (?!.*\b(QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ)\b)[^\d]+(\s+\d+)?$')]
    ap2 = no_xa[no_xa['CustomerAddress'].str.match(r'\d+/[^ ]+ Ấp [^,]+')]
    no_ap2 = no_xa[~no_xa['CustomerAddress'].str.match(r'\d+/[^ ]+ Ấp [^,]+')]
    xa2 = no_ap2[no_ap2['CustomerAddress'].str.match(r'\d+/[^ ]+ Xã [^,]+')]
    no_xa2 = no_ap2[~no_ap2['CustomerAddress'].str.match(r'\d+/[^ ]+ Xã [^,]+')]
    
    ap_thitran = no_xa2[no_xa2['CustomerAddress'].apply(is_valid_format)]
    noap_thitran = no_xa2[~no_xa2['CustomerAddress'].apply(is_valid_format)]

    ap_df = noap_thitran[noap_thitran['CustomerAddress'].apply(lambda x: is_valid_format_1(x) if not pd.isna(x) else False)]
    no_ap_df = noap_thitran[~noap_thitran['CustomerAddress'].apply(lambda x: is_valid_format_1(x) if not pd.isna(x) else False)]
    
    ap_df_2 = no_ap_df[no_ap_df['CustomerAddress'].str.match(r'^\d+ Ap [^QLHLĐTHLDlt]+\b(?:(?!QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ|Đinh Viết Cừu|Nguyễn Thông).)*$')]
    no_ap_df_2 = no_ap_df[~no_ap_df['CustomerAddress'].str.match(r'^\d+ Ap [^QLHLĐTHLDlt]+\b(?:(?!QL|HL|ĐT|Hương Lộ|Ql|Tl|TL|Dt|Quốc Lộ|Tỉnh Lộ|Đinh Viết Cừu|Nguyễn Thông).)*$')]
    
    kenhxang = no_ap_df_2[no_ap_df_2['CustomerAddress'].apply(lambda x: is_valid_format_2(x) if not pd.isna(x) else False)]
    no_kenhxang = no_ap_df_2[~no_ap_df_2['CustomerAddress'].apply(lambda x: is_valid_format_2(x) if not pd.isna(x) else False)]
    
    cauduongbang = no_kenhxang[no_kenhxang['CustomerAddress'].apply(lambda x: is_valid_format_3(x) if not pd.isna(x) else False)]
    no_cauduongbang = no_kenhxang[~no_kenhxang['CustomerAddress'].apply(lambda x: is_valid_format_3(x) if not pd.isna(x) else False)]
    
    thuanhoa = no_cauduongbang[no_cauduongbang['CustomerAddress'].apply(lambda x: is_valid_format_4(x) if not pd.isna(x) else False)]
    no_thuanhoa = no_cauduongbang[~no_cauduongbang['CustomerAddress'].apply(lambda x: is_valid_format_4(x) if not pd.isna(x) else False)]
    
    hoa_lac_c = no_thuanhoa[no_thuanhoa['CustomerAddress'].apply(lambda x: is_valid_format_5(x) if not pd.isna(x) else False)]
    no_hoa_lac_c = no_thuanhoa[~no_thuanhoa['CustomerAddress'].apply(lambda x: is_valid_format_5(x) if not pd.isna(x) else False)]
    
    pattern = re.compile(r'\b695/4 Bình Trị 2, Xã Thuận Mỹ\b')

    binhtri = no_hoa_lac_c[no_hoa_lac_c['CustomerAddress'].str.contains(pattern, na=False, regex=True)]
    no_binhtri = no_hoa_lac_c[~no_hoa_lac_c['CustomerAddress'].str.contains(pattern, na=False, regex=True)]

    caykho = no_binhtri[no_binhtri['CustomerAddress'].apply(lambda x: is_valid_format_6(x) if not pd.isna(x) else False)]
    no_caykho = no_binhtri[~no_binhtri['CustomerAddress'].apply(lambda x: is_valid_format_6(x) if not pd.isna(x) else False)]

    pattern = re.compile(r'\b(Bình An)\s*,\s*Xã (\S+)\b')

    binhan = no_caykho[no_caykho['CustomerAddress'].str.contains(pattern, na=False, regex=True)]
    no_binhan = no_caykho[~no_caykho['CustomerAddress'].str.contains(pattern, na=False, regex=True)]

    df_khongthoa = pd.concat([HVN_notdigit_mask, df_notfiltered])
    df_khongthoa = pd.concat([df_khongthoa, ap_ten])
    df_khongthoa = pd.concat([df_khongthoa, so_ap])
    df_khongthoa = pd.concat([df_khongthoa, ap])
    df_khongthoa = pd.concat([df_khongthoa, xa])
    df_khongthoa = pd.concat([df_khongthoa, ap2])
    df_khongthoa = pd.concat([df_khongthoa, xa2])
    df_khongthoa = pd.concat([df_khongthoa, ap_thitran])
    df_khongthoa = pd.concat([df_khongthoa, ap_df])
    df_khongthoa = pd.concat([df_khongthoa, ap_df_2])
    df_khongthoa = pd.concat([df_khongthoa, kenhxang])
    df_khongthoa = pd.concat([df_khongthoa, cauduongbang])
    df_khongthoa = pd.concat([df_khongthoa, thuanhoa])
    df_khongthoa = pd.concat([df_khongthoa, hoa_lac_c])
    df_khongthoa = pd.concat([df_khongthoa, binhtri])
    df_khongthoa = pd.concat([df_khongthoa, caykho])
    df_khongthoa = pd.concat([df_khongthoa, binhan])

    pattern = re.compile(r'\b19 Nguyễn Văn Nhân, Xã Thanh Phú\b')

    nguyennhan = df_khongthoa[df_khongthoa['CustomerAddress'].str.contains(pattern, na=False, regex=True)]
    no_nguyennhan = df_khongthoa[~df_khongthoa['CustomerAddress'].str.contains(pattern, na=False, regex=True)]

    df_thoa = pd.concat([no_binhan, nguyennhan])
    df_kthoa = no_nguyennhan.copy()

    return df_thoa, df_kthoa

def xuly_toadotrongaddress_vigo(Vigo_r2):
    contains_plus = Vigo_r2[Vigo_r2['CustomerAddress'].str.contains('\\+')]
    not_contains_plus = Vigo_r2[~Vigo_r2['CustomerAddress'].str.contains('\\+')]

    contains_plus['plus_word'] = contains_plus['CustomerAddress'].str.extractall(r'(\S+\+\S+)').groupby(level=0).agg(','.join)[0]
    contains_plus['CustomerAddress'] = contains_plus.apply(lambda row: row['CustomerAddress'].replace(row['plus_word'], ''), axis=1)

    contains_plus = contains_plus.drop('plus_word', axis=1)    

    vigo = pd.concat([contains_plus, not_contains_plus])

    return vigo

def convert_district(match):
    district_number = match.group(1)
    return f'phường {district_number}'

def has_street_name(address):
    street_name_pattern = r'\b(?:\w+\s*)?(\d+(?:\/\d+)?\s*[ABCD]?[^\d]*\s*\d*(?:\s*\d+(?:\/\d+)?)?\s*[ABCD]?[^\d]*(?:Đường|đường|Đ\\.|d\\.|Duong|duong|Đại Lộ Đồng khởi|lộ phước hiệp|đương lộ làng|phan đình Phùng|Trương Văn Kỉnh|Nguyễn đình chiểu|Phú Hoà|phan đình Phùng|Hoà Hảo|Tiền Phong|Lê Văn Tám|Nguyễn Bỉnh Khiêm|Tô Thị Huỳnh|Lê Văn Khuyên|Nguyễn Văn Tiếp|Nguyễn Văn Cương|Lê Văn Tường|Võ Văn Môn|Lê Lợi|Nguyễn Trãi|Hùng Vương|Nguyễn Thị Nhỏ|Nguyễn Thị Bảy|Nguyễn Chí Thanh|Thống Chế Sỹ|Phạm Văn Thành|Huynh Chau So|Huỳnh Châu Sổ|Nguyễn Đình Chiểu|Đỗ Tường Phong|Sơn Thông|Đỗ Trình Thoại|Nguyễn Thông|Lãnh Binh Thái|Phạm Văn Thành|Trần Công Oanh|Đồng Khởi|Châu Thị Kim|Lê Văn Tưởng|Phạm Ngũ Lão|Nguyễn Văn Trổi|Nguyễn Thái Bình|Hoàng Hoa Thám|Đặng Văn Truyện|Huỳnh Văn Đảnh|Nguyễnvăn Trưng|Vành Đai|Nguyen Thong|Nguyễn Thông|Nguyen Trung Truc|Trương Định|Nguyễn Thị Định|Nguyễn Văn Nhâm|QL|Ql|Ba Sa Gò Mối|Mỹ Thuận|Bùi Hữu Nghĩa|Châu Thị Kim|Cử Luyện|Nguyễn Huệ|Hoàng Hoa Thám|Nguyễn Văn Tư|Nguyễn Huệ|Đoàn Hoàng Minh|Phan Văn Mảng|Đồng Khởi|Nguyễn Văn Tuôi|Tán Kế|Châu Thị Kim|Trần Văn Đấu|Sương Nguyệt Ánh|Châu Văn Bảy|Nguyễn Trung Trực|Nguyễn Văn Cánh|nguyễn minh đường|Nguyễn Thị Hạnh|Đỗ Đình Thoại|Nguyễn Du|Châu Thị Kim|Trương Vĩnh Ký|Nguyen Thi Dinh|Hồ Văn Huê|Nguyễn Đáng|Vĩnh Phú|Châu Thị Kim|Đoàn Hoàng Minh|Huỳnh Việt Thanh|Nguyễn Hữu Thọ|Luong Van Chan|Phan Đình Phùng|Phạm Văn Ngô|Nguyen Thong|Đương 30/4|CMT8|cmt8|Huỳnh Tấn Phát|Hương Lộ|HL|Trần Văn Đạt|Quốc Lộ|Hương Lộ|Tỉnh lộ|Dt|DT|Nguyễn An Ninh|Lê Hồng Phong|Lộc Trung|Lê Minh Xuân|Mai Thị Tốt|Phạm Văn Ngô|TL|Lê Thị Trâm|Quoc Lo|Tỉnh Lộ|Nguyễn Thị Minh Khai|Phạm Văn Chiên|Võ Văn Nhơn|Lê Hữu Nghĩa|Phan Văn Lay|Châu Văn Giác|Nguyễn Huỳnh Đức|Phan Văn Mãng|Bùi Tấn|Lưu Nghiệp Anh|Lê Hồng Phong|Nguyễn Văn Siêu|Nguyễn Văn Quá|Vo Cong Ton|Thái Hữu Kiểm|Trần Minh Châu|Lý Thường Kiệt|Phạm Văn Ngũ|Trần Phong Sắc|Nguyễn Văn Kỉnh|Phan Văn Mãng|Nguyễn Cửu Vân|Bùi Thị Hồng|Trần Thế Sinh|Hoàng Anh|Huỳnh Văn Tạo|Nguyễn Văn Trung|Đỗ Tường Tự|Nguyễn Văn Trưng|Tl|ĐT|Trần Phú|Nguyễn Thị Diện|Nguyễn Văn Tiến|Phan Van Lay|Nguyen Thi Minh Khai|Đỗ Tường Tự|Thủ Khoa Huân|Thanh Hà|Tân Long|Truong Bình|Huỳnh Thị Lung| Phan Thanh Giảng| Phan Thanh Giảnp|Đinh Viết Cừu|Võ Nguyên Giáp|Lộ Dừa|Truong Vinh Ky|Phan Văn Tình|Trịnh Quang Nghị|Nguyễn Minh Trung|Ca Văn Thỉnh|Bàu Sen|Chu Văn An|Trần Thị Non|Lê lợi|Võ Công Tồn|NguyễnTrung Trực|Phan Van Mang|Phan Văn Mảng|Phan Văn Mãng|Nguyễn Hòa Luông|Nguyễn Văn Trỗi|Võ Văn Kiệt|Huỳnh Văn Gấm|Thanh hà|Hòa Lạc C|phạm văn ngô|Phạm Văn Ngô|Phước Toàn|Vỏ Duy Tạo|Lảnh Binh Thái|Nguyen Cuu Van|Trần Phú|Cao Văn Lầu|Điện Biên Phủ|Bạch Đằng|Phú Hòa|Huỳnh Văn Thanh|Võ Văn Tần|Phan Văn Tình|Chu Van An|Thuận Hòa|Vũ Đình Liệu|Đồng Văn Dẫn|Mậu Thân|Cao Thị Mai|Nguyễn Văn Rành|Nguyễn Công Trung|Nguyễn Minh Trường|Nguyễn Quang Đại|Hai Bà Trưng|Võ Thị Sáu|Trần Quốc Tuấn|Lê Văn Kiệt|Nguyễn Văn Tạo|30 Tháng 4|3/2|Phan đình phùng|Thủ Khoa Huân|Phan Văn Tình|Hoàng Lam|Ngô Quyền|Nguyễn Thị Bẹ|Phan Văn Đạt|Nguyễn Minh Trường|Võ Công Tồn|Huỳnh Văn Gấm|Huỳnh Văn Lộng|Bình Hòa|Nguyen Huu Tho|Nguyễn Hữu Thọ|Võ Công Tồn|Trần Phong Sắc|Trần Phong Sẳ|Phạm Ngọc Tòng|Phan Văn Tình|Trần Hưng Đạo|Nguyễn Văn Rành|Nguyễn Văn Cảnh|Thủ Khoa Thừa|Lê Thị Điền|Rạch Tre|Trần Hưng Dạo|Võ Công Tồn|Võ Hồng Cúc|Lê Văn Kiệt|Phạm Văn Trạch|Lê Văn Tao|Nguyễn Thiện Thành|Huỳnh Hữu Thống|2 tháng 9|Phan Châu Trinh|Hoàng Lam|Trần Văn Trà|NGUYỄN THỊ ÚT|Nguyễn Thị Út|Bình Trị 2|Lê Văn Trần|Trưng Nhị|Bình hòa|Nguyễn ĐìnhChiểu|Hương lộ|Nguyen Thi Bay|Nguyễn Thị Bảy|Đt 816|huỳnh văn đảnh|Huỳnh Văn Đảnh|Nguyễn văn tiếp|Nguyễn Văn Tiếp|Cao Thi Mai|Đt825|Đặng Văn Búp|30 Thang 4|Đt 835B)\s*\S*)\b'
    return bool(re.search(street_name_pattern, address))

def loc_vigo_r2(vigo_lower):
    columns_to_lowercase = ['CustomerAddress', 'WardName', 'DistrictName', 'ProvinceName']
    vigo_lower[columns_to_lowercase] = vigo_lower[columns_to_lowercase].apply(lambda x: x.astype(str))
    vigo_lower['CustomerAddress'] = vigo_lower['CustomerAddress'].apply(lambda x: ', '.join(dict.fromkeys(x.split(', '))))
    vigo_lower['CustomerAddress'] = vigo_lower['CustomerAddress'].apply(lambda x: re.sub(r'\bp(\d+)\b', convert_district, x))
    vigo_lower['CustomerAddress'] = vigo_lower['CustomerAddress'].apply(lambda x: ', '.join(dict.fromkeys(x.split(', '))))
    with_street_vigo_lower = vigo_lower[vigo_lower['CustomerAddress'].apply(has_street_name)]
    without_street_vigo_lower = vigo_lower[~vigo_lower['CustomerAddress'].apply(has_street_name)]
    contains_keywords = with_street_vigo_lower[with_street_vigo_lower['CustomerAddress'].str.contains('Trụ điện|Trụ|Khóm|tru điện|Tru dien|TĐ|chợ|Chợ|Ngã 4|Ngã 3|Ấp An vĩnh|Gần Khánh Uyên 1|Cột điện|Ấp 2|Cột|Hẻm|Kp2|Ấp 4|Ấp mới 2|ấp Bàu Sen|ấp Nô Công|Cộ|Apa An vĩnh 1', case=False, regex=True)]
    does_not_contain_keywords = with_street_vigo_lower[ ~with_street_vigo_lower['CustomerAddress'].str.contains('Trụ điện|Trụ|Khóm|tru điện|Tru dien|TĐ|chợ|Chợ|Ngã 4|Ngã 3|Ấp An vĩnh|Gần Khánh Uyên 1|Cột điện|Ấp 2|Cột|Hẻm|Kp2|Ấp 4|Ấp mới 2|ấp Bàu Sen|ấp Nô Công|Cộ|Apa An vĩnh 1', case=False, regex=True)]
    df_khongthoa = pd.concat([without_street_vigo_lower, contains_keywords])
    contains_keywords_2 = df_khongthoa[df_khongthoa['CustomerAddress'].str.contains('1404 Đong trị|191, tỉnh lộ 914|24a tấn đức', case=False, regex=True)]
    does_not_contain_keywords_2 = df_khongthoa[ ~df_khongthoa['CustomerAddress'].str.contains('1404 Đong trị|191, tỉnh lộ 914|24a tấn đức', case=False, regex=True)]
    df_thoa = does_not_contain_keywords.copy()
    df_thoa = pd.concat([df_thoa, contains_keywords_2])
    df_khongthoa = does_not_contain_keywords_2.copy()    
    df_khongthoa['CustomerAddress'] = df_khongthoa['CustomerAddress'].replace(to_replace=r'Unnamed', value='', regex=True)

    return df_thoa, df_khongthoa

def xuly_address_hvn(OptionalText, data, text_remove):
    OptionalText['Replace'].fillna('', inplace=True)
    OptionalText['Replace'].replace({None: ''}, inplace=True)
    OptionalText['Replace'].replace({'NULL': ''}, inplace=True)

    for index, row in OptionalText.iterrows():
        optional_text = row['Optional']
        replace_text = row['Replace']
        
        data['CustomerAddress'] = data['CustomerAddress'].str.replace(optional_text, replace_text)

    data['result'] = data['CustomerAddress'].apply(extract_location)

    text_remove['Replace'].fillna('', inplace=True)
    text_remove['Replace'].replace({None: ''}, inplace=True)
    text_remove['Replace'].replace({'NULL': ''}, inplace=True)\

    for index, row in text_remove.iterrows():
        optional_text = row['Text']
        replace_text = row['Replace']
        
        data['result'] = data['result'].str.replace(optional_text, replace_text)

    return data

def extract_location(text):
    match = re.search(r'(.+?(?:Xã|Phường|Thị Trấn)(?=\s|$))', text)
    
    if match:
        result = match.group(1)
    else:
        # Nếu không tìm thấy, lấy toàn bộ địa chỉ
        result = text
    
    return result.strip()

def xuly_address_Vigo(OptionalText, data, text_remove):
    OptionalText['Replace'].fillna('', inplace=True)
    OptionalText['Replace'].replace({None: ''}, inplace=True)
    OptionalText['Replace'].replace({'NULL': ''}, inplace=True)

    data['result'] = data['CustomerAddress'].apply(extract_location)

    text_remove['Replace'].fillna('', inplace=True)
    text_remove['Replace'].replace({None: ''}, inplace=True)
    text_remove['Replace'].replace({'NULL': ''}, inplace=True)

    for index, row in text_remove.iterrows():
        optional_text = row['Text']
        replace_text = row['Replace']
        
        # Thực hiện thay thế trong cột 'Address' của HVN
        data['result'] = data['result'].str.replace(optional_text, replace_text)   
    
    return data

def tao_address(data):
    data['WardName'].fillna('', inplace=True)
    data['WardName'].replace({None: ''}, inplace=True)
    data['WardName'].replace({'NULL': ''}, inplace=True)

    data['DistrictName'].fillna('', inplace=True)
    data['DistrictName'].replace({None: ''}, inplace=True)
    data['DistrictName'].replace({'NULL': ''}, inplace=True)

    data['Address'] = data['result'] + data['WardName'] + data['DistrictName'] + data['ProvinceName']

    Op = pd.read_excel("Op_Text_vigo.xlsx")

    Op['Replace'].fillna('', inplace=True)
    Op['Replace'].replace({None: ''}, inplace=True)
    Op['Replace'].replace({'NULL': ''}, inplace=True)

    for index, row in Op.iterrows():
        optional_text = row['Optional']
        replace_text = row['Replace']
        
        # Check if replace_text is a string
        if not isinstance(replace_text, str):
            # Convert replace_text to string or handle accordingly
            replace_text = str(replace_text)

        # Thực hiện thay thế trong cột 'Address' của HVN
        data['Address'] = data['Address'].str.replace(optional_text, replace_text)

    return data

def fuzzy_similarity(row):
    return fuzz.token_set_ratio(row['Address_file1'], row['Address_file2'])

def round2(data1, data2):
    data1['Address'] = data1['Address'].str.lower()
    data1['Address'] = data1['Address'].apply(lambda x: re.sub(r'[^a-z0-9\s/]', '', x))
    data1['Address'] = data1['Address'].apply(lambda x: re.sub(r'\s', '', x))
    data2['Address'] = data2['Address'].str.lower()
    data2['Address'] = data2['Address'].apply(lambda x: re.sub(r'[^a-z0-9\s/]', '', x))
    data2['Address'] = data2['Address'].apply(lambda x: re.sub(r'\s', '', x))
    data1['ProvinceName'] = data1['ProvinceName'].str.lower()
    data1['DistrictName'] = data1['DistrictName'].str.lower()
    data1['WardName'] = data1['WardName'].str.lower()
    data2['ProvinceName'] = data2['ProvinceName'].str.lower()
    data2['DistrictName'] = data2['DistrictName'].str.lower()
    data2['WardName'] = data2['WardName'].str.lower()

    result = pd.merge(data1, data2, left_on=['ProvinceName', 'DistrictName', 'WardName'],
                  right_on=['ProvinceName', 'DistrictName', 'WardName'], how='inner',
                  suffixes=('_file1', '_file2'), 
                  left_index=False, right_index=False)
    
    result['fuzzy_similarity'] = result.apply(fuzzy_similarity, axis=1)
    
    matching_rows_fuzzy = result[result['fuzzy_similarity'] == 100]

    return matching_rows_fuzzy

def get_geoScore(Data_geo, V_geo):
    geo_dist = (distance.great_circle(Data_geo, V_geo).meters)
    geo_score = 0
    if (geo_dist > 1000):
        geo_score = 0
    else:
        geo_score = 100 - (geo_dist / 1000 * 100)
    return geo_score

def calc_score_dist(df):
    HVN_geo = (df['Latitude_file1'], df['Longitude_file2'])
    Vigo_geo = (df['Latitude_file2'], df['Longitude_file2'])
    dist_score = get_geoScore(HVN_geo, Vigo_geo)
    return dist_score

def calc_score_name(df):
    return fuzz.ratio(df['clean_Outlet_Name_file1'], df['clean_Outlet_Name_file2'])

def round3(HVN_r3, Vigo_r3):
    from unidecode import unidecode
    HVN_r3['clean_Outlet_Name'] = HVN_r3['clean_Outlet_Name'].apply(lambda x: unidecode(x))
    HVN_r3['ProvinceName'] = HVN_r3['ProvinceName'].str.lower()
    HVN_r3['DistrictName'] = HVN_r3['DistrictName'].str.lower()
    HVN_r3['WardName'] = HVN_r3['WardName'].str.lower()
    Vigo_r3['clean_Outlet_Name'] = Vigo_r3['clean_Outlet_Name'].apply(lambda x: unidecode(x))
    Vigo_r3['ProvinceName'] = Vigo_r3['ProvinceName'].str.lower()
    Vigo_r3['DistrictName'] = Vigo_r3['DistrictName'].str.lower()
    Vigo_r3['WardName'] = Vigo_r3['WardName'].str.lower()
    
    result = pd.merge(HVN_r3, Vigo_r3, left_on=['ProvinceName', 'DistrictName', 'WardName'],
                    right_on=['ProvinceName', 'DistrictName', 'WardName'], how='inner',  suffixes=('_file1', '_file2'))
    
    result['Score_Distance'] = result.apply(calc_score_dist, axis=1)
    result['Score_Name'] = result.apply(calc_score_name, axis=1)
    location90storename100 = result.loc[(result['Score_Distance'] >= 90) & (result['Score_Name'] == 100)]
    
    return location90storename100

# Hàm tính khoảng cách giữa hai điểm dựa trên tọa độ Latitude và Longitude (theo mét)
def calculate_distance(point1, point2):
    # point1 và point2 là tuple (latitude, longitude)
    return geodesic(point1, point2).meters

def apply_filter(row):
    ward_name = row['ward']
    district_name = row['district']
    province_name = row['province']
    distance = row['distance']

    if 'Phường' in ward_name and 'Thành phố' in district_name and 'Tỉnh' in province_name:
        return distance <= 5
    elif 'Xã' in ward_name and 'Thành phố' in district_name and 'Tỉnh' in province_name:
        return distance <= 10
    elif 'Thị trấn' in ward_name and 'Thành phố' in district_name and 'Tỉnh' in province_name:
        return distance <= 15
    elif 'Phường' in ward_name and 'Huyện' in district_name and 'Tỉnh' in province_name:
        return distance <= 10
    elif 'Xã' in ward_name and 'Huyện' in district_name and 'Tỉnh' in province_name:
        return distance <= 15
    elif 'Thị trấn' in ward_name and 'Huyện' in district_name and 'Tỉnh' in province_name:
        return distance <= 20
    else:
        # Nếu không phải các trường hợp trên, không áp dụng lọc
        return True

def calc_score_name_2(df):
    return fuzz.token_set_ratio(df['clean_Outlet_Name_file1'], df['clean_Outlet_Name_file2'])

def round4(HVN_r4, Vigo_r4):
    from unidecode import unidecode

    HVN_r4['clean_Outlet_Name'] = HVN_r4['clean_Outlet_Name'].apply(lambda x: unidecode(x))
    Vigo_r4['clean_Outlet_Name'] = Vigo_r4['clean_Outlet_Name'].apply(lambda x: unidecode(x))
    merged_df = pd.merge(HVN_r4, Vigo_r4, on=['ProvinceName', 'DistrictName', 'WardName'], how='inner', suffixes=('_file1', '_file2'))
    
    # Thêm cột mới 'distance' vào DataFrame
    merged_df['distance'] = merged_df.apply(lambda row: calculate_distance((row['Latitude_file1'], row['Longitude_file1']),
                                                                 (row['Latitude_file2'], row['Longitude_file2'])), axis=1)
    
    merged_df['province'] = 'Tỉnh ' + merged_df['province']
    filtered_result = merged_df[merged_df.apply(apply_filter, axis=1)]
    
    filtered_result['Score_Name_2'] = filtered_result.apply(calc_score_name_2, axis=1)
    
    storename80 = filtered_result.loc[filtered_result['Score_Name_2'] >= 80]
    
    return storename80

def process_uploaded_files(uploaded_files):
    dataframes = {}
    HVN = None
    Vigo = None

    for idx, file in enumerate(uploaded_files):
        df = pd.read_excel(file)

        # Convert "Phone" column to string
        if 'Phone' in df.columns:
            df['Phone'] = df['Phone'].astype(str)

        # Get the filename without extension
        filename_without_extension = file.name.split('.')[0]

        # Assign dataframe to dictionary using filename as key
        dataframes[filename_without_extension] = df

        # Assign specific dataframes
        if idx == 0:
            HVN = df.copy()
        elif idx == 1:
            Vigo = df.copy()

    return dataframes, HVN, Vigo

def apply_round1(HVN, Vigo, teleco1, teleco2):
    # Xử lý phone
    HVN_nophone, Vigo_nophone, HVN_phone_na, HVN_phone_notna, Vigo_phone_na, Vigo_phone_notna = xuly_phone(HVN, Vigo)
    HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = tao_danh_sach_thoa_khongthoa(teleco1, teleco2, HVN_phone_notna, Vigo_phone_notna, HVN_nophone, HVN_phone_na, Vigo_nophone, Vigo_phone_na)
    phonenum_map = round1(HVN_thoa, Vigo_thoa)
    return phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa

def apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2):
    # Lấy danh sách HVN round2, danh sách thỏa gồm đầy đủ số nhà và tên đường và danh sách không thỏa do thiếu thông tin
    HVN_r2_thoa, HVN_r2_khonghtoa = loc_hvn_r2(HVN)
    # lấy danh sách Vigo round 2, danh sách thỏa gồm đầy đủ số nhà và tên đường và danh sách không thỏa do thiếu thông tin
    Vigo_r2 = xuly_toadotrongaddress_vigo(Vigo)
    vigo_r2_thoa, vigo_r2_khongthoa = loc_vigo_r2(Vigo_r2)
    df1 = xuly_address_hvn(OptionalText, HVN_r2_thoa, text_remove)
    df2 = xuly_address_Vigo(OptionalText, vigo_r2_thoa, text_remove_2)
    # Xử lý address cho hvn và vigo
    df1 = tao_address(df1)
    df2 = tao_address(df2)
    # Round 2: 100% address
    matching_addess = round2(df1, df2)
    return matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa

def main():
    st.title("Store Mapping With Custom Selection")

    # Upload files
    st.header("1. Upload Excel File(s)")
    uploaded_files = st.file_uploader("Upload Excel files", type=["xlsx"], accept_multiple_files=True)

    # Display files
    st.header("2. Display Uploaded File(s)")
    
    dataframes = {}
    HVN = None
    Vigo = None

    if uploaded_files:
        dataframes, HVN, Vigo = process_uploaded_files(uploaded_files)

        # Display information for HVN and Vigo
        if HVN is not None:
            st.subheader("Displaying HVN:")
            st.dataframe(HVN)

        if Vigo is not None:
            st.subheader("Displaying Vigo:")
            st.dataframe(Vigo)

    # Display Round table
    st.header("3. Round Table")

    # Create Round Table
    Round_table = pd.DataFrame({
        'Round': [1, 2, 3, 4],
        'Description': ['Mapping 100% phone', 'Mapping 100% address', 
                        'Mapping ward_district_province, mapping location score >= 90 và mapping 100% store name',
                        'Mapping ward_district_province,  mapping location 5-10-15-10-15-20 và mapping >= 80 store name'],
    })

    # Display the draggable Round Table
    selected_Round_indexes = st.multiselect("Select Round to Add to Flow", Round_table.index, format_func=lambda i: Round_table.loc[i, 'Description'], key='selected_Round')

    # Create Flow Table
    flow_table = pd.DataFrame(columns=['Round', 'Description'])

    # Update Flow Table based on the selected Round in Round Table
    if selected_Round_indexes:
        selected_Round = Round_table.loc[selected_Round_indexes]
        flow_table = pd.concat([flow_table, selected_Round])

    # Display the Flow Table
    st.header("4. Flow Table")
    st.table(flow_table)

    # Display the Flow Table
    st.header("5. Test")
    rounds = flow_table['Round'].tolist()
    
    if st.button("Apply"):        
        if rounds:
            # Read nhung file ho tro cleansing va check thong tin
            Province, teleco1, teleco2, OptionalText, text_remove, text_remove_2, remove_name, remove_name_2 = read_file()    
            
            if HVN is not None and Vigo is not None:      
                st.text("Đang xét điều kiện")
                HVN = xet_latlng(HVN)
                Vigo = xet_latlng(Vigo)

                test = xet_phancap(HVN, Province)
                test2 = xet_phancap(Vigo, Province)

                if rounds == [1]:
                    st.text("round hiện tại là [1]")
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)
                
                elif rounds == [2]:
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)
                
                elif rounds == [3]:
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)
                
                elif rounds == [4]:
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)
                
                elif  rounds == [1, 2]:
                    st.text("round hiện tại là [1, 2]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)
                
                elif rounds == [1, 3]:
                    st.text("round hiện tại là [1, 3]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)
                
                elif rounds == [1, 4]:
                    st.text("round hiện tại là [1, 4]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [2, 1]:
                    st.text("round hiện tại là [2, 1]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)         
                
                elif rounds == [2, 3]:
                    st.text("round hiện tại là [2, 3]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])
                    
                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)
                
                elif rounds == [2, 4]:
                    st.text("round hiện tại là [2, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [3, 1]:
                    st.text("round hiện tại là [3, 1]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [3, 2]:
                    st.text("round hiện tại là [3, 2]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [3, 4]:
                    st.text("round hiện tại là [3, 4]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                   
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [4, 1]:
                    st.text("round hiện tại là [4, 1]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)                    

                elif rounds == [4, 2]:
                    st.text("round hiện tại là [4, 2]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [4, 3]:
                    st.text("round hiện tại là [4, 3]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif  rounds == [1, 2, 3]:
                    st.text("round hiện tại là [1, 2, 3]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif  rounds == [1, 2, 4]:
                    st.text("round hiện tại là [1, 2, 4]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)                                                    

                elif  rounds == [1, 3, 2]:
                    st.text("round hiện tại là [1, 3, 2]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif  rounds == [1, 3, 4]:
                    st.text("round hiện tại là [1, 3, 4]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                  
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [1, 4, 2]:
                    st.text("round hiện tại là [1, 4, 2]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [1, 4, 3]:
                    st.text("round hiện tại là [1, 4, 3]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [2, 1, 3]:
                    st.text("round hiện tại là [2, 1, 3]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)     

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [2, 1, 4]:
                    st.text("round hiện tại là [2, 1, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)     

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)      

                elif rounds == [2, 3, 1]:
                    st.text("round hiện tại là [2, 3, 1]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])
                    
                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [2, 3, 4]:
                    st.text("round hiện tại là [2, 3, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])
                    
                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [2, 4, 1]:
                    st.text("round hiện tại là [2, 4, 1]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [2, 4, 3]:
                    st.text("round hiện tại là [2, 4, 3]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [3, 1, 2]:
                    st.text("round hiện tại là [3, 1, 2]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [3, 1, 4]:
                    st.text("round hiện tại là [3, 1, 4]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [3, 2, 1]:
                    st.text("round hiện tại là [3, 2, 1]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                     # Lọc data đã thảo round 2   
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [3, 2, 4]:
                    st.text("round hiện tại là [3, 2, 4]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  
                 
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)    

                elif rounds == [3, 4, 1]:
                    st.text("round hiện tại là [3, 4, 1]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)
                    
                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [3, 4, 2]:
                    st.text("round hiện tại là [3, 4, 2]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                                      
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)                

                elif rounds == [4, 1, 2]:
                    st.text("round hiện tại là [4, 1, 2]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map) 

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                elif rounds == [4, 1, 3]:
                    st.text("round hiện tại là [4, 1, 3]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map) 

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [4, 2, 1]:
                    st.text("round hiện tại là [4, 2, 1]")  
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)    

                     # Lọc data đã thảo round 2   
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)                    

                elif rounds == [4, 2, 3]:
                    st.text("round hiện tại là [4, 2, 3]")  
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [4, 3, 1]:
                    st.text("round hiện tại là [4, 3, 1]")
                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)    

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map, HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)                    

                elif rounds == [4, 3, 2]:
                    st.text("round hiện tại là [4, 3, 2]")
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif  rounds == [1, 2, 3, 4]:
                    st.text("round hiện tại là [1, 2, 3, 4]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [1, 2, 4, 3]:
                    st.text("round hiện tại là [1, 2, 4, 3]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)   

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif  rounds == [1, 3, 2, 4]:
                    st.text("round hiện tại là [1, 3, 2, 4]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                    # Loại bỏ data thỏa round2
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])  
                   
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)   

                elif  rounds == [1, 3, 4, 2]:
                    st.text("round hiện tại là [1, 3, 4, 2]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                   
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [1, 4, 2, 3]:
                    st.text("round hiện tại là [1, 4, 2, 3]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])  

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [1, 4, 3, 2]:
                    st.text("round hiện tại là [1, 4, 3, 2]")
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN, Vigo, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r2 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r2 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                elif rounds == [2, 1, 3, 4]:
                    st.text("round hiện tại là [2, 1, 3, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)     

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [2, 1, 4, 3]:
                    st.text("round hiện tại là [2, 1, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)     

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN_r3, remove_name)
                    Vigo = xuly_hvnname(Vigo_r3, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)       

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [2, 3, 1, 4]:
                    st.text("round hiện tại là [2, 3, 1, 4]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])
                    
                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)        

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [2, 3, 4, 1]:
                    st.text("round hiện tại là [2, 3, 4, 1]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])
                    
                    # Xử lý name
                    st.text("round hiện tại là [3]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [2, 4, 1, 3]:
                    st.text("round hiện tại là [2, 4, 1, 3]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)     

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)    

                elif rounds == [2, 4, 3, 1]:
                    st.text("round hiện tại là [2, 4, 3, 1]")
                    st.text("round hiện tại là [2]")
                    
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN, Vigo, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                    # Lọc data đã thảo round 2   
                    HVN_r2 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r2 = pd.concat([HVN_r2, HVN_r2_khonghtoa])
                    Vigo_r2 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r2 = pd.concat([Vigo_r2, vigo_r2_khongthoa])

                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN_r2, remove_name)
                    Vigo = xuly_hvnname(Vigo_r2, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)    

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [3, 1, 2, 4]:
                    st.text("round hiện tại là [3, 1, 2]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])  
                 
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80) 

                elif rounds == [3, 1, 4, 2]:
                    st.text("round hiện tại là [3, 1, 4]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)
                elif rounds == [3, 2, 1, 4]:
                    st.text("round hiện tại là [3, 2, 1, 4]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                     # Lọc data đã thảo round 2   
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])

                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                elif rounds == [3, 2, 4, 1]:
                    st.text("round hiện tại là [3, 2, 4, 1]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  
                   
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [3, 4, 1, 2]:
                    st.text("round hiện tại là [3, 4, 1, 2]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                  
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)
                    
                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)                     

                elif rounds == [3, 4, 2, 1]:
                    st.text("round hiện tại là [3, 4, 2, 1]")
                    st.text("round hiện tại là [3]")
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    location90storename100 = round3(HVN, Vigo)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                                      
                    st.text("round hiện tại là [4]")
                    storename80 = round4(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)    

                     # Lọc data đã thảo round 2   
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [4, 1, 2, 3]:
                    st.text("round hiện tại là [4, 1, 2, 3]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map) 

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r2, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r2, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                    # Loại bỏ data thỏa round2
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])  

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                elif rounds == [4, 1, 3, 2]:
                    st.text("round hiện tại là [4, 1, 3, 2]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r2, Vigo_r2, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map) 

                    # Loại bỏ data thỏa round1
                    HVN_r3 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_khongthoa])
                    Vigo_r3 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, Vigo_khongthoa])

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)    

                elif rounds == [4, 2, 1, 3]:
                    st.text("round hiện tại là [4, 2, 1, 3]")  
                    st.text("round hiện tại là [4]")                   
                    st.text("Đang xử lý name")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)    

                     # Lọc data đã thảo round 2   
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map) 

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r4, Vigo_r4)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)                                      

                elif rounds == [4, 2, 3, 1]:
                    st.text("round hiện tại là [4, 2, 3, 1]")  
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r2, Vigo_r2, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                    # Loại bỏ data thỏa round2
                    HVN_r3 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r3 = pd.concat([HVN_r3, HVN_r2_khonghtoa])
                    Vigo_r3 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r3 = pd.concat([Vigo_r3, vigo_r2_khongthoa])  

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r3, Vigo_r3)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r4 = HVN_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r4 = Vigo_r3.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                elif rounds == [4, 3, 1, 2]:
                    st.text("round hiện tại là [4, 3, 1, 2]")
                    # Xử lý name                    
                    st.text("round hiện tại là [4]")
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)    

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]
                    
                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map, HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r3, Vigo_r3, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)                    

                    # Loại bỏ data thỏa round1
                    HVN_r4 = HVN_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_khongthoa])
                    Vigo_r4 = Vigo_thoa.loc[lambda df: ~df.OutletID.isin(phonenum_map.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, Vigo_khongthoa])
                    
                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r4, Vigo_r4, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess) 

                elif rounds == [4, 3, 2, 1]:
                    st.text("round hiện tại là [4, 3, 2, 1]")
                    st.text("round hiện tại là [4]")
                    # Xử lý name
                    HVN = xuly_hvnname(HVN, remove_name)
                    Vigo = xuly_hvnname(Vigo, remove_name_2)
                    storename80 = round4(HVN, Vigo)
                    st.subheader("Displaying round 4:")
                    st.dataframe(storename80)

                    # Lọc data đã thảo round 4  
                    HVN_r2 = HVN.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file1)]
                    Vigo_r2 = Vigo.loc[lambda df: ~df.OutletID.isin(storename80.OutletID_file2)]

                    st.text("round hiện tại là [3]")
                    location90storename100 = round3(HVN_r2, Vigo_r2)
                    st.subheader("Displaying round 3:")
                    st.dataframe(location90storename100)

                    # Lọc data đã thảo round 3  
                    HVN_r3 = HVN_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file1)]
                    Vigo_r3 = Vigo_r2.loc[lambda df: ~df.OutletID.isin(location90storename100.OutletID_file2)]

                    st.text("round hiện tại là [2]")
                    matching_addess, df1, df2, HVN_r2_khonghtoa, vigo_r2_khongthoa = apply_round2(HVN_r3, Vigo_r3, OptionalText, text_remove, text_remove_2)

                    st.subheader("Displaying round 2:")
                    st.dataframe(matching_addess)

                     # Lọc data đã thảo round 2   
                    HVN_r4 = df1.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file1)]
                    HVN_r4 = pd.concat([HVN_r4, HVN_r2_khonghtoa])
                    Vigo_r4 = df2.loc[lambda df: ~df.OutletID.isin(matching_addess.OutletID_file2)]
                    Vigo_r4 = pd.concat([Vigo_r4, vigo_r2_khongthoa])

                    st.text("round hiện tại là [1]")
                    st.text("Đang xử lý phone")

                    # Xử lý phone
                    phonenum_map , HVN_thoa, HVN_khongthoa, Vigo_thoa, Vigo_khongthoa = apply_round1(HVN_r4, Vigo_r4, teleco1, teleco2)
                    st.subheader("Displaying round 1:")
                    st.dataframe(phonenum_map)

                else:
                    st.text("Nam ngoai pham vi")
        else:
            # Handle the case when rounds is empty
            st.text("No rounds selected.")
        
if __name__ == '__main__':
    main()