"""
Gold Layer - Dimension Province Configuration
"""

# Source data
SOURCE_CSV_PATH = "/data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv"

# Target table  
# In Iceberg, full path is: catalog.database.table
# Since we use gold catalog, database should be "gold" and table is "dim_province"
# Full reference: gold.gold.dim_province (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"  
GOLD_TABLE = "dim_province"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key
BUSINESS_KEY = ["province_name"]

# Mapping: Province names after new administrative law (2025)
# Format: {current_name: name_after_merger}
PROVINCE_AFTER_LAW = {
    "Hà Giang": "Tuyên Quang",
    "Cao Bằng": "Cao Bằng",
    "Lào Cai": "Lào Cai",
    "Sơn La": "Sơn La",
    "Lai Châu": "Lai Châu",
    "Bắc Kạn": "Thái Nguyên",
    "Lạng Sơn": "Lạng Sơn",
    "Tuyên Quang": "Tuyên Quang",
    "Yên Bái": "Lào Cai",
    "Thái Nguyên": "Thái Nguyên",
    "Điện Biên": "Điện Biên",
    "Phú Thọ": "Phú Thọ",
    "Vĩnh Phúc": "Phú Thọ",
    "Bắc Giang": "Bắc Ninh",
    "Bắc Ninh": "Bắc Ninh",
    "Hà Nội": "Hà Nội",
    "Quảng Ninh": "Quảng Ninh",
    "Hải Dương": "Hải Phòng",
    "Hải Phòng": "Hải Phòng",
    "Hòa Bình": "Phú Thọ",
    "Hưng Yên": "Hưng Yên",
    "Hà Nam": "Ninh Bình",
    "Thái Bình": "Hưng Yên",
    "Nam Định": "Ninh Bình",
    "Ninh Bình": "Ninh Bình",
    "Thanh Hóa": "Thanh Hóa",
    "Nghệ An": "Nghệ An",
    "Hà Tĩnh": "Hà Tĩnh",
    "Quảng Bình": "Quảng Trị",
    "Quảng Trị": "Quảng Trị",
    "Thừa Thiên Huế": "Huế",
    "Đà Nẵng": "Đà Nẵng",
    "Quảng Nam": "Đà Nẵng",
    "Quảng Ngãi": "Quảng Ngãi",
    "Kon Tum": "Quảng Ngãi",
    "Gia Lai": "Gia Lai",
    "Bình Định": "Gia Lai",
    "Phú Yên": "Đắk Lắk",
    "Đắk Lắk": "Đắk Lắk",
    "Khánh Hòa": "Khánh Hòa",
    "Đắk Nông": "Lâm Đồng",
    "Lâm Đồng": "Lâm Đồng",
    "Ninh Thuận": "Khánh Hòa",
    "Bình Phước": "Đồng Nai",
    "Tây Ninh": "Tây Ninh",
    "Bình Dương": "Hồ Chí Minh",
    "Đồng Nai": "Đồng Nai",
    "Bình Thuận": "Lâm Đồng",
    "Hồ Chí Minh": "Hồ Chí Minh",
    "Long An": "Tây Ninh",
    "Bà Rịa Vũng Tàu": "Hồ Chí Minh",
    "Đồng Tháp": "Đồng Tháp",
    "An Giang": "An Giang",
    "Tiền Giang": "Đồng Tháp",
    "Vĩnh Long": "Vĩnh Long",
    "Bến Tre": "Vĩnh Long",
    "Cần Thơ": "Cần Thơ",
    "Kiên Giang": "An Giang",
    "Trà Vinh": "Vĩnh Long",
    "Hậu Giang": "Cần Thơ",
    "Sóc Trăng": "Cần Thơ",
    "Bạc Liêu": "Cà Mau",
    "Cà Mau": "Cà Mau",
}

# Central municipalities (Thành phố trực thuộc trung ương)
CENTRAL_CITIES = ["Đà Nẵng", "Hà Nội", "Cần Thơ", "Hải Phòng", "Hồ Chí Minh"]

