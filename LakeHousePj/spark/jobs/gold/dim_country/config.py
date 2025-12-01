"""
Gold Layer - Dimension Country Configuration

Source: derived from `silver.hotels_reviews` (column `reviewer_country`)
Target: gold.dim_country (Iceberg table)
"""

# Source table in Silver catalog
SOURCE_TABLE = "hotels_reviews"
SOURCE_COLUMN = "reviewer_country"

# Source catalog/database for Silver. Use fully-qualified name when referencing tables
SOURCE_CATALOG = "silver"
SOURCE_DATABASE = "silver"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_country"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key: country name
BUSINESS_KEY = ["country_name"]

# Optional: manual mapping from country_name -> region (e.g., 'Vietnam': 'Asia')
# Leave empty or add as needed.
COUNTRY_TO_REGION  = {
    # =======================
    # CHÂU Á
    # =======================

    # Đông Nam Á
    "Việt Nam": "Đông Nam Á",
    "Singapore": "Đông Nam Á",
    "Malaysia": "Đông Nam Á",
    "Thái Lan": "Đông Nam Á",
    "Philippines": "Đông Nam Á",
    "Campuchia": "Đông Nam Á",
    "Lào": "Đông Nam Á",
    "Myanmar": "Đông Nam Á",
    "Brunei Darussalam": "Đông Nam Á",
    "Đông Timor": "Đông Nam Á",
    "Indonesia": "Đông Nam Á",

    # Đông Á
    "Trung Quốc": "Đông Á",
    "Nhật Bản": "Đông Á",
    "Hàn Quốc": "Đông Á",
    "Mông Cổ": "Đông Á",
    "Bắc Triều Tiên": "Đông Á",

    # Nam Á
    "Ấn Độ": "Nam Á",
    "Bangladesh": "Nam Á",
    "Sri Lanka": "Nam Á",
    "Pakistan": "Nam Á",
    "Nepal": "Nam Á",
    "Bhutan": "Nam Á",
    "Maldives": "Nam Á",
    "Afghanistan": "Nam Á",

    # Trung Á
    "Kazakhstan": "Trung Á",
    "Uzbekistan": "Trung Á",
    "Turkmenistan": "Trung Á",
    "Kyrgyzstan": "Trung Á",
    "Tajikistan": "Trung Á",

    # Tây Á 
    "Israel": "Tây Á",
    "United Arab Emirates": "Tây Á",
    "Saudi Arabia": "Tây Á",
    "Qatar": "Tây Á",
    "Oman": "Tây Á",
    "Kuwait": "Tây Á",
    "Bahrain": "Tây Á",
    "Jordan": "Tây Á",
    "Lebanon": "Tây Á",
    "Syria": "Tây Á",
    "Yemen": "Tây Á",
    "Iraq": "Tây Á",
    "Iran": "Tây Á",
    "Palestine": "Tây Á",
    "Cyprus": "Tây Á",
    "Armenia": "Tây Á",
    "Azerbaijan": "Tây Á",
    "Georgia": "Tây Á",
    "Thổ Nhĩ Kỳ": "Tây Á",
    # =======================
    # CHÂU ÂU
    # =======================

    # Bắc Âu
    "Na Uy": "Bắc Âu",
    "Thụy Điển": "Bắc Âu",
    "Phần Lan": "Bắc Âu",
    "Đan Mạch": "Bắc Âu",
    "Iceland": "Bắc Âu",
    "Lithuania": "Bắc Âu",
    "Latvia": "Bắc Âu",
    "Estonia": "Bắc Âu",

    # Tây Âu
    "Pháp": "Tây Âu",
    "Vương Quốc Anh": "Tây Âu",
    "Hà Lan": "Tây Âu",
    "Bỉ": "Tây Âu",
    "Ireland": "Tây Âu",
    "Luxembourg": "Tây Âu",
    "Monaco": "Tây Âu",
    "Andorra": "Tây Âu",

    # Trung Âu
    "Đức": "Trung Âu",
    "Thụy Sĩ": "Trung Âu",
    "Áo": "Trung Âu",
    "Ba Lan": "Trung Âu",
    "Hungary": "Trung Âu",
    "Cộng hoà Séc": "Trung Âu",
    "Slovakia": "Trung Âu",
    "Slovenia": "Trung Âu",
    "Liechtenstein": "Trung Âu",

    # Nam Âu
    "Ý": "Nam Âu",
    "Tây Ban Nha": "Nam Âu",
    "Bồ Đào Nha": "Nam Âu",
    "Hy Lạp": "Nam Âu",
    "Croatia": "Nam Âu",
    "Bosnia và Herzegovina": "Nam Âu",
    "Albania": "Nam Âu",
    "Montenegro": "Nam Âu",
    "Bắc Macedonia": "Nam Âu",
    "Serbia": "Nam Âu",
    "Kosovo": "Nam Âu",
    "Malta": "Nam Âu",
    "Romania": "Nam Âu",
    "Bulgaria": "Nam Âu",
    "San Marino": "Nam Âu",
    "Vatican": "Nam Âu",

    # Đông Âu (ngoài phần đã cho vào Nam/Trung)
    "Belarus": "Đông Âu",
    "Ukraine": "Đông Âu",
    "Moldova": "Đông Âu",
    "Nga": "Đông Âu",

    # =======================
    # CHÂU MỸ
    # =======================

    # Bắc Mỹ
    "Mỹ": "Bắc Mỹ",
    "Canada": "Bắc Mỹ",

    # Trung Mỹ
    "Mexico": "Trung Mỹ",
    "Guatemala": "Trung Mỹ",
    "Belize": "Trung Mỹ",
    "Honduras": "Trung Mỹ",
    "Nicaragua": "Trung Mỹ",
    "Costa Rica": "Trung Mỹ",
    "Panama": "Trung Mỹ",
    "El Salvador": "Trung Mỹ",

    # Caribe
    "Dominican Republic": "Caribe",
    "Haiti": "Caribe",
    "Cuba": "Caribe",
    "Bahamas": "Caribe",
    "Jamaica": "Caribe",
    "Trinidad và Tobago": "Caribe",
    "Barbados": "Caribe",
    "Grenada": "Caribe",
    "Saint Lucia": "Caribe",
    "Saint Vincent and the Grenadines": "Caribe",
    "Antigua and Barbuda": "Caribe",
    "Saint Kitts and Nevis": "Caribe",
    "Dominica": "Caribe",

    # Nam Mỹ
    "Brazil": "Nam Mỹ",
    "Argentina": "Nam Mỹ",
    "Chile": "Nam Mỹ",
    "Colombia": "Nam Mỹ",
    "Peru": "Nam Mỹ",
    "Uruguay": "Nam Mỹ",
    "Venezuela": "Nam Mỹ",
    "Bolivia": "Nam Mỹ",
    "Ecuador": "Nam Mỹ",
    "Paraguay": "Nam Mỹ",
    "Guyana": "Nam Mỹ",
    "Suriname": "Nam Mỹ",

    # =======================
    # CHÂU PHI
    # =======================

    # Bắc Phi
    "Ai Cập": "Bắc Phi",
    "Morocco": "Bắc Phi",
    "Algeria": "Bắc Phi",
    "Tunisia": "Bắc Phi",
    "Libya": "Bắc Phi",
    "Sudan": "Bắc Phi",

    # Tây Phi
    "Nigeria": "Tây Phi",
    "Ghana": "Tây Phi",
    "Senegal": "Tây Phi",
    "Gambia": "Tây Phi",
    "Sierra Leone": "Tây Phi",
    "Liberia": "Tây Phi",
    "Guinea": "Tây Phi",
    "Guinea-Bissau": "Tây Phi",
    "Burkina Faso": "Tây Phi",
    "Benin": "Tây Phi",
    "Togo": "Tây Phi",
    "Cape Verde": "Tây Phi",
    "Mali": "Tây Phi",
    "Niger": "Tây Phi",
    "Bờ Biển Ngà": "Tây Phi",

    # Đông Phi
    "Kenya": "Đông Phi",
    "Tanzania": "Đông Phi",
    "Uganda": "Đông Phi",
    "Rwanda": "Đông Phi",
    "Burundi": "Đông Phi",
    "Ethiopia": "Đông Phi",
    "Somalia": "Đông Phi",
    "Djibouti": "Đông Phi",
    "Eritrea": "Đông Phi",
    "Madagascar": "Đông Phi",
    "Seychelles": "Đông Phi",
    "Mauritius": "Đông Phi",
    "Comoros": "Đông Phi",

    # Trung Phi
    "Cộng hòa Trung Phi": "Trung Phi",
    "Republic of the Congo": "Trung Phi",
    "Gabon": "Trung Phi",
    "Equatorial Guinea": "Trung Phi",
    "Cameroon": "Trung Phi",
    "Chad": "Trung Phi",

    # Nam Phi
    "Nam Phi": "Nam Phi",
    "Namibia": "Nam Phi",
    "Botswana": "Nam Phi",
    "Zimbabwe": "Nam Phi",
    "Zambia": "Nam Phi",
    "Malawi": "Nam Phi",
    "Mozambique": "Nam Phi",
    "Angola": "Nam Phi",
    "Lesotho": "Nam Phi",
    "Eswatini": "Nam Phi",
    "Mauritania": "Bắc/Tây Phi",  # tuỳ bạn muốn gộp đâu, đa số xếp Tây Phi

    # =======================
    # CHÂU ĐẠI DƯƠNG
    # =======================

    # Úc & New Zealand
    "Úc": "Úc & New Zealand",
    "New Zealand": "Úc & New Zealand ",

    # Melanesia
    "Papua New Guinea": "Melanesia ",
    "Fiji": "Melanesia ",
    "Vanuatu": "Melanesia ",
    "Solomon Islands": "Melanesia ",

    # Micronesia
    "Micronesia": "Micronesia",
    "Palau": "Micronesia",
    "Nauru": "Micronesia",
    "Kiribati": "Micronesia",
    "Marshall Islands": "Micronesia",

    # Polynesia
    "Samoa": "Polynesia",
    "Tonga": "Polynesia",
    "Tuvalu": "Polynesia",

    # =======================
    # KHÁC / ĐẶC BIỆT
    # =======================
    "Nam Cực": "Nam Cực",
}

