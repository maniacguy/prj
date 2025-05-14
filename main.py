import requests
import time
import pg8000
import geopandas as gpd
from shapely.geometry import box

class NSWPointsOfInterestAPI:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def get_poi_within_bbox(self, min_lat, min_lon, max_lat, max_lon):
        """Lấy các điểm quan tâm trong một bounding box (min_lat, min_lon, max_lat, max_lon)."""
        url = f"{self.base_url}/query"
        params = {
            "f": "json",
            "geometry": f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*"  # Lấy tất cả các trường thông tin
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("features", [])
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi lấy dữ liệu POI: {e}")
            return []

class SA2DataProcessor:
    def __init__(self, db_config, shapefile_path, poi_api):
        self.db_config = db_config
        self.shapefile_path = shapefile_path
        self.poi_api = poi_api

    def connect(self):
        """Kết nối đến PostgreSQL và trả về đối tượng kết nối."""
        try:
            conn = pg8000.connect(**self.db_config)
            print("✅ Kết nối đến PostgreSQL thành công!")
            return conn
        except Exception as e:
            print(f"❌ Lỗi kết nối cơ sở dữ liệu: {e}")
            return None

    def insert_pois(self, conn, sa2_code, pois):
        """Chèn dữ liệu POI vào cơ sở dữ liệu PostgreSQL."""
        insert_query = """
        INSERT INTO points_of_interest (poigroup, poitype, poiname, poilabel, shape, startdate, enddate, lastupdate)
        VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s, %s)
        ON CONFLICT (poigroup) DO NOTHING;
        """

        try:
            with conn.cursor() as cur:
                for poi in pois:
                    print(poi)
                    poi_id = poi['attributes']['objectid']
                    poi_name = poi['attributes'].get('poiname', 'Unknown')
                    poi_type = poi['attributes'].get('poilabeltype', 'Unknown')
                    geometry = poi['geometry']
                    startdate = poi['attributes'].get('startdate', None)
                    enddate = poi['attributes'].get('enddate', None)
                    lastupdate = poi['attributes'].get('lastupdate', None)
                    if geometry:
                        geom_wkt = f"POINT({geometry['x']} {geometry['y']})"
                        data = (sa2_code, poi_id, poi_name, poi_type, geom_wkt, None, None, None)
                        # print(data)
                        
                #         cur.execute(insert_query, data)
                # conn.commit()
                print(f"✅ Chèn dữ liệu POI cho SA2 {sa2_code} thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi chèn dữ liệu POI vào cơ sở dữ liệu: {e}")
            conn.rollback()

    def process_sa2_pois(self, conn, gdf):
        """Lặp qua từng SA2 để lấy và chèn dữ liệu POI vào cơ sở dữ liệu."""
        for _, row in gdf.iterrows():
            sa2_code = row['SA2_CODE21']
            min_lon, min_lat, max_lon, max_lat = row['geometry'].bounds
            print(f"📍 Đang xử lý POI cho SA2 {sa2_code}...")

            # Lấy danh sách POI cho vùng SA2 từ API
            pois = self.poi_api.get_poi_within_bbox(min_lat, min_lon, max_lat, max_lon)

            if pois:
                self.insert_pois(conn, sa2_code, pois)
            else:
                print(f"⚠️ Không tìm thấy POI cho SA2 {sa2_code}.")
            
            # Thời gian chờ 1 giây trước khi xử lý tiếp SA2 khác để tránh quá tải API
            time.sleep(10)

    def process_data(self):
        """Đọc dữ liệu từ Shapefile và trả về GeoDataFrame."""
        print(f"📂 Đang xử lý file Shapefile {self.shapefile_path}...")
        try:
            gdf = gpd.read_file(self.shapefile_path, engine="pyogrio")
            print(f"✅ Đọc file Shapefile {self.shapefile_path} thành công!")
            return gdf
        except Exception as e:
            print(f"❌ Lỗi khi đọc Shapefile: {e}")
            return None

# Cấu hình kết nối cơ sở dữ liệu PostgreSQL
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

# Đường dẫn đến Shapefile chứa dữ liệu SA2
shapefile_path = 'data/SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp'

# URL của NSW Points of Interest API
poi_api_url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0"

# Khởi tạo đối tượng API và xử lý dữ liệu
poi_api = NSWPointsOfInterestAPI(poi_api_url)
processor = SA2DataProcessor(db_config, shapefile_path, poi_api)

# Kết nối đến cơ sở dữ liệu
conn = processor.connect()
if conn:
    # Xử lý dữ liệu từ Shapefile
    gdf = processor.process_data()

    if gdf is not None:
        # Lấy và chèn dữ liệu POI cho từng SA2
        processor.process_sa2_pois(conn, gdf)

    # Đóng kết nối cơ sở dữ liệu sau khi hoàn thành
    conn.close()
