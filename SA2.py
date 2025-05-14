import pg8000
import geopandas as gpd

class SA2DataProcessor:
    def __init__(self, db_config, shapefile_path):
        self.db_config = db_config
        self.shapefile_path = shapefile_path

    def connect(self):
        """Kết nối đến PostgreSQL và trả về đối tượng kết nối."""
        try:
            conn = pg8000.connect(**self.db_config)
            print("✅ Kết nối đến PostgreSQL thành công!")
            return conn
        except Exception as e:
            print(f"❌ Lỗi kết nối: {e}")
            return None

    def create_table(self, conn):
        """Tạo bảng SA2 nếu chưa tồn tại."""
        drop_table_query = "DROP TABLE IF EXISTS SA2;"
        create_table_query = """
        CREATE TABLE IF NOT EXISTS SA2 (
            sa2_code21 VARCHAR(15) PRIMARY KEY,
            sa2_name21 VARCHAR(255),
            loci_uri21 TEXT,
            geometry GEOMETRY(MultiPolygon, 4326)
        );
        """

        try:
            with conn.cursor() as cur:
                cur.execute(drop_table_query)
                cur.execute(create_table_query)
                conn.commit()
                print("✅ Tạo bảng SA2 thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi tạo bảng: {e}")
            conn.rollback()

    def insert_data(self, conn, gdf):
        """Chèn dữ liệu từ GeoDataFrame vào bảng PostgreSQL với PostGIS."""
        insert_query = """
        INSERT INTO SA2 (sa2_code21, sa2_name21, loci_uri21, geometry)
        VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
        ON CONFLICT (sa2_code21) DO NOTHING;
        """
        try:
            with conn.cursor() as cur:
                for _, row in gdf.iterrows():
                    geometry = row['geometry']
                    if geometry is None:  # Kiểm tra nếu geometry là None
                        continue  # Bỏ qua bản ghi nếu không có geometry
                    
                    if geometry.geom_type == 'Polygon':
                        geometry = geometry.wkt
                    elif geometry.geom_type == 'MultiPolygon':
                        geometry = geometry.wkt  # Hoặc chuyển đổi theo cách khác nếu cần
                    else:
                        continue  # Bỏ qua các loại geometry không mong muốn
                    
                    data = (
                        str(row['SA2_CODE21']),
                        row['SA2_NAME21'],
                        row['LOCI_URI21'],
                        geometry
                    )
                    cur.execute(insert_query, data)
                conn.commit()
                print("✅ Chèn dữ liệu thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi chèn dữ liệu: {e}")
            conn.rollback()


    def process_data(self):
        """Quy trình xử lý dữ liệu từ Shapefile."""
        print(f"📂 Đang xử lý file {self.shapefile_path}")
        gdf = gpd.read_file(self.shapefile_path, engine="pyogrio")
        print(f"✅ Đọc file Shapefile {self.shapefile_path} thành công!")
        print(f"✅ Đã tải dữ liệu Shapefile SA2:\n{gdf.head()}")
        return gdf

# Cấu hình database
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

shapefile_path = 'data/SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp'

# Khởi tạo đối tượng xử lý dữ liệu
processor = SA2DataProcessor(db_config, shapefile_path)

# Kết nối đến database
conn = processor.connect()
if conn:
    # Xử lý dữ liệu từ Shapefile
    gdf = processor.process_data()

    # Tạo bảng và chèn dữ liệu
    processor.create_table(conn)
    processor.insert_data(conn, gdf)

    # Đóng kết nối
    conn.close()
