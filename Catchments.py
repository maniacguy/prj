import geopandas as gpd
import pandas as pd
import pg8000

# Hàm kết nối đến PostgreSQL
def connect():
    try:
        conn = pg8000.connect(**db_config)
        print("✅ Kết nối đến PostgreSQL thành công!")
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None

# Hàm tạo bảng 'schools' trong PostgreSQL
def create_schools_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS schools (
        USE_ID INT PRIMARY KEY,
        CATCH_TYPE VARCHAR(255),
        USE_DESC VARCHAR(255),
        ADD_DATE DATE,
        KINDERGART VARCHAR(255),
        YEAR1 VARCHAR(255),
        YEAR2 VARCHAR(255),
        YEAR3 VARCHAR(255),
        YEAR4 VARCHAR(255),
        YEAR5 VARCHAR(255),
        YEAR6 VARCHAR(255),
        YEAR7 VARCHAR(255),
        YEAR8 VARCHAR(255),
        YEAR9 VARCHAR(255),
        YEAR10 VARCHAR(255),
        YEAR11 VARCHAR(255),
        YEAR12 VARCHAR(255),
        PRIORITY VARCHAR(255),
        level VARCHAR(50),
        geometry GEOMETRY(MultiPolygon, 4326)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✅ Tạo bảng 'schools' thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng: {e}")
        conn.rollback()

# Đọc dữ liệu từ các shapefile
def read_and_combine_shapefiles():
    print("🌍 Đang xử lý và kết hợp dữ liệu từ các shapefiles...")
    
    # Đọc các shapefiles và thêm cột 'level' để phân biệt
    gdf_future = gpd.read_file("data/Catchments/catchments/catchments_future.shp", engine="pyogrio")
    gdf_future['level'] = 'future'  # Gắn nhãn cho cấp trường future
    
    gdf_primary = gpd.read_file("data/Catchments/catchments/catchments_primary.shp", engine="pyogrio")
    gdf_primary['level'] = 'primary'  # Gắn nhãn cho cấp trường primary
    
    gdf_secondary = gpd.read_file("data/Catchments/catchments/catchments_secondary.shp", engine="pyogrio")
    gdf_secondary['level'] = 'secondary'  # Gắn nhãn cho cấp trường secondary
    
    # Kết hợp tất cả GeoDataFrame
    combined_gdf = pd.concat([gdf_future, gdf_primary, gdf_secondary], ignore_index=True)
    print("✅ Đã kết hợp dữ liệu từ các shapefiles!")
    
    return combined_gdf

# Chèn dữ liệu vào bảng 'schools'
def insert_data_into_schools(conn, gdf):
    try:
        with conn.cursor() as cur:
            for _, row in gdf.iterrows():
                insert_query = """
                INSERT INTO schools (USE_ID, CATCH_TYPE, USE_DESC, ADD_DATE, KINDERGART, YEAR1, YEAR2, YEAR3, YEAR4, YEAR5, YEAR6, YEAR7, YEAR8, YEAR9, YEAR10, YEAR11, YEAR12, PRIORITY, level, geometry)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
                """
                cur.execute(insert_query, (
                    row['USE_ID'], 
                    row['CATCH_TYPE'], 
                    row['USE_DESC'], 
                    row['ADD_DATE'], 
                    row['KINDERGART'], 
                    row['YEAR1'], 
                    row['YEAR2'], 
                    row['YEAR3'], 
                    row['YEAR4'], 
                    row['YEAR5'], 
                    row['YEAR6'], 
                    row['YEAR7'], 
                    row['YEAR8'], 
                    row['YEAR9'], 
                    row['YEAR10'], 
                    row['YEAR11'], 
                    row['YEAR12'], 
                    row['PRIORITY'], 
                    row['level'],  # Thêm thông tin cấp trường vào
                    row['geometry'].wkt  # Chuyển đổi geometry thành WKT
                ))
            conn.commit()
            print("✅ Đã chèn dữ liệu vào bảng 'schools' thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi chèn dữ liệu: {e}")
        conn.rollback()


# Cấu hình kết nối với PostgreSQL
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

# Kết nối và xử lý dữ liệu
conn = connect()
if conn:
    create_schools_table(conn)
    combined_gdf = read_and_combine_shapefiles()
    insert_data_into_schools(conn, combined_gdf)
    conn.close()
