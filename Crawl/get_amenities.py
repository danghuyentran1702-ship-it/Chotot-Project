import requests
import time
import urllib.parse
from math import ceil
import csv
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_MAP_API_KEY")

# =============================
# 1. TEXT SEARCH FUNCTION
# =============================
def google_text_search(query, location, radius=3000):
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    
    query_encoded = urllib.parse.quote(query)
    
    lat, lng = location
    url = f"{base_url}?query={query_encoded}&location={lat},{lng}&radius={radius}&key={API_KEY}"

    results = []
    
    while True:
        res = requests.get(url).json()
        results.extend(res.get("results", []))

        if "next_page_token" not in res:
            break
        
        next_token = res["next_page_token"]
        time.sleep(2)  # Google yêu cầu
        url = f"{base_url}?pagetoken={next_token}&key={API_KEY}"
    
    return results


# =============================
# 2. GENERATE GRID OVER HCMC
# =============================
def generate_grid(top_lat, left_lng, bottom_lat, right_lng, steps=6):
    lat_step = (top_lat - bottom_lat) / steps
    lng_step = (right_lng - left_lng) / steps

    grid_points = []
    for i in range(steps):
        for j in range(steps):
            lat = top_lat - i * lat_step
            lng = left_lng + j * lng_step
            grid_points.append((lat, lng))
    
    return grid_points


# =============================
# 3. MAIN: SCAN ENTIRE HCMC WITH KEYWORDS
# =============================

KEYWORDS = [
    "chợ", "siêu thị", "siêu thị mini", "cửa hàng tiện lợi",
    "trường mầm non", "trường tiểu học", "trường thpt", "trường đại học",
    "bệnh viện", "trạm y tế", 
    "công viên", "sân vận động", "khu vui chơi", "phố đi bộ",
    "quán ăn", "nhà hàng", "quán cafe",
    "trung tâm thương mại",
    "ngân hàng", "atm",
    "metro", "bến xe", "ga tàu",
]

# Bounding box của TP.HCM
TOP_LAT = 11.1800
BOTTOM_LAT = 10.3500
LEFT_LNG = 106.3650
RIGHT_LNG = 107.0250

# Tạo grid 6×6 → 36 điểm (đủ để phủ toàn thành phố)
GRID = generate_grid(TOP_LAT, LEFT_LNG, BOTTOM_LAT, RIGHT_LNG, steps=6)

# Store unique places
all_places = {}

# Load dữ liệu cũ nếu có

if os.path.exists("hcm_amenities.csv"):
    with open("hcm_amenities.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row["place_id"]
            all_places[pid] = {
                "name": row["name"],
                "address": row["address"],
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "type": row["type"],
            }
    print(f"Đã tải {len(all_places)} tiện ích từ hcm_amenities.csv")

for keyword in KEYWORDS:
    query = keyword + " ở Thành phố Hồ Chí Minh"
    print(f"\n=== Đang tìm: {query} ===")

    for point in GRID:
        results = google_text_search(query, location=point, radius=4000)
        for r in results:
            pid = r.get("place_id")
            if pid not in all_places:
                all_places[pid] = {
                    "name": r.get("name"),
                    "address": r.get("formatted_address"),
                    "lat": r["geometry"]["location"]["lat"],
                    "lng": r["geometry"]["location"]["lng"],
                    "type": keyword,
                }

        print(f"  Điểm {point}: Tìm được {len(results)} kết quả, Tổng cộng: {len(all_places)} tiện ích")
        # Write to hcm_amenities.csv sau mỗi điểm để tránh mất dữ liệu

        with open("hcm_amenities.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["place_id", "name", "address", "lat", "lng", "type"])
            for pid, info in all_places.items():
                writer.writerow([pid, info["name"], info["address"], info["lat"], info["lng"], info["type"]])
        
        time.sleep(1)  # Tránh quá tải yêu cầu

print("\n=== Hoàn tất ===")
print(f"Tổng số tiện ích tìm được: {len(all_places)}")

# Lưu kết quả cuối cùng
with open("hcm_amenities.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["place_id", "name", "address", "lat", "lng", "type"])
    for pid, info in all_places.items():
        writer.writerow([pid, info["name"], info["address"], info["lat"], info["lng"], info["type"]])

print("Đã lưu dữ liệu vào hcm_amenities.csv")
