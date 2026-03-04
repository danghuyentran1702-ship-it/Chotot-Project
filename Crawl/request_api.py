import os
import requests
import time
from datetime import datetime
from typing import List, Dict, Any
import json

class ChototAPI:
    """
    API client cho Chotot với rate limiting 120 requests/10s
    """
    
    def __init__(self):
        self.base_url = "https://gateway.chotot.com/v1/public/ad-listing"
        self.rate_limit = 100  # requests per 10 seconds
        self.time_window = 10  # seconds
        self.request_times = []
        
    def _wait_if_needed(self):
        """
        Kiểm tra và chờ nếu cần để tuân thủ rate limit
        """
        now = time.time()
        
        # Loại bỏ các request cũ hơn 10 giây
        self.request_times = [t for t in self.request_times if now - t < self.time_window]
        
        # Nếu đã đạt giới hạn, chờ đến khi có thể request tiếp
        if len(self.request_times) >= self.rate_limit:
            sleep_time = self.time_window - (now - self.request_times[0]) + 0.1
            if sleep_time > 0:
                print(f"Rate limit reached. Waiting {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                # Làm mới danh sách sau khi chờ
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < self.time_window]
        
        # Thêm thời gian request hiện tại
        self.request_times.append(time.time())
    
    def get_total_pages(self, st: str, cg: int, region_v2: int = 13000, limit: int = 10) -> int:
        """
        Lấy tổng số trang cần iterate
        
        Args:
            st: 'u' (Mua bán) hoặc 'v' (Cho thuê)
            cg: Category (1010, 1020, 1030, 1040, 1050)
            region_v2: Vùng (13000 = TP HCM)
            limit: Số items mỗi page
            
        Returns:
            Tổng số trang
        """
        self._wait_if_needed()
        
        params = {
            'st': st,
            'cg': cg,
            'key_param_included': 'true',
            'limit': limit,
            'protection_entitlement': 'true',
            'region_v2': region_v2,
            'o': 0
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            total = data.get('total', 0)
            
            print(f"Total pages: {total}")
            return total

        except Exception as e:
            print(f"Error getting total pages: {e}")
            return 0
    
    def fetch_page(self, st: str, cg: int, offset: int, region_v2: int = 13000, limit: int = 10) -> Dict[str, Any]:
        """
        Lấy dữ liệu từ một trang cụ thể
        
        Args:
            st: 'u' (Mua bán) hoặc 'v' (Cho thuê)
            cg: Category (1010, 1020, 1030, 1040, 1050)
            offset: Offset của page
            region_v2: Vùng (13000 = TP HCM)
            limit: Số items mỗi page
            
        Returns:
            Response data
        """
        self._wait_if_needed()
        
        params = {
            'st': st,
            'cg': cg,
            'key_param_included': 'true',
            'limit': limit,
            'protection_entitlement': 'true',
            'region_v2': region_v2,
            'o': offset
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"Error fetching page at offset {offset}: {e}")
            return {}

    def scrape_all(self, st: str, cg: int, region_v2: int = 13000, limit: int = 10, max_pages: int = None, folder: str = None) -> List[Dict]:
        """
        Scrape tất cả dữ liệu cho một combination of st và cg
        
        Args:
            st: 'u' (Mua bán) hoặc 'v' (Cho thuê)
            cg: Category (1010, 1020, 1030, 1040, 1050)
            region_v2: Vùng (13000 = TP HCM)
            limit: Số items mỗi page
            max_pages: Giới hạn số trang (None = tất cả)
            
        Returns:
            List of all ads
        """
        print(f"  \n{'='*60}")
        print(f"  Starting scrape: st={st}, cg={cg}, region_v2={region_v2}")
        print(f"  {'='*60}\n")
        
        # Lấy tổng số trang
        total_pages = self.get_total_pages(st, cg, region_v2, limit)

        print(f"  Total pages to scrape: {total_pages}")
        
        if total_pages == 0:
            print("  No data found or error occurred")
            return []
        
        # Giới hạn số trang nếu cần
        if max_pages:
            total_pages = min(total_pages, max_pages)
            print(f"  Limited to {max_pages} pages")
        
        all_ads = []

        for offset in range(0, total_pages + 1):
            print(f"  Fetching offset {offset}...")
            
            data = self.fetch_page(st, cg, offset, region_v2, limit)

            if folder:
                # Lưu từng trang vào file riêng
                filename = os.path.join(folder, f"chotot_st_{st}_cg_{cg}_offset_{offset}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"   Saved page data to {filename}")
            
            if 'ads' in data:
                ads = data['ads']
                all_ads.extend(ads)
                print(f"   Retrieved {len(ads)} ads (Total: {len(all_ads)})")
            else:
                print(f"   No ads found on this page")
        
        print(f"\n{'='*60}")
        print(f"Scraping completed! Total ads collected: {len(all_ads)}")
        print(f"{'='*60}\n")
        
        return all_ads


def main():
    """
    Example usage
    """
    api = ChototAPI()
    
    # Các giá trị có thể có
    st_values = ['u', 's']  # s=Mua bán, u=Cho thuê
    cg_values = [1010, 1020, 1030, 1040, 1050]
    
    for st in st_values:
        for cg in cg_values:
            print(f"Run Scrape apartments for sale in HCMC: st={st}, cg={cg}")
            folder = f"chotot_st_{st}_cg_{cg}"

            if os.path.exists(folder) is False:
                os.makedirs(folder)

            ads = api.scrape_all(
                st=st,           # Mua bán
                cg=cg,          # Category
                region_v2=13000,  # TP HCM
                limit=20,         # 20 items per page
                max_pages=None,       # Chỉ lấy 5 trang đầu (để test)
                folder=folder,
            )

            print(f"  Total ads scraped: {len(ads)}")
            
            # Lưu kết quả
            if ads:
                filename = os.path.join(folder, f"chotot_st_{st}_cg_{cg}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(ads, f, ensure_ascii=False, indent=2)
                print(f"  Data saved to {filename}")


if __name__ == "__main__":
    main()