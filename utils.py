import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text

def enable_postgis(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            print("✅ Đã kích hoạt PostGIS!")
    except Exception as e:
        print(f"❌ Lỗi khi kích hoạt PostGIS: {e}")

# 🗃️ Thiết lập kết nối PostgreSQL
def get_engine(db_name="sa2_data", user="postgres", password="1234", host="localhost", port="5432"):
    try:
        engine = create_engine(f"postgresql+pg8000://{user}:{password}@{host}:{port}/{db_name}")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Kết nối thành công! - ", result.fetchone())
        return engine
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None

# 📝 Hàm đọc file CSV
def read_csv_file(csv_path):
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Đọc file CSV {csv_path} thành công!")
        return df
    except Exception as e:
        print(f"❌ Lỗi khi đọc CSV {csv_path}: {e}")
        return None

# 📄 Hàm đọc file TXT (có thể là dạng TSV hoặc CSV)
def read_txt_file(txt_path, delimiter='\t'):
    try:
        df = pd.read_csv(txt_path, delimiter=delimiter)
        print(f"✅ Đọc file TXT {txt_path} thành công!")
        return df
    except Exception as e:
        print(f"❌ Lỗi khi đọc TXT {txt_path}: {e}")
        return None

# 🌍 Hàm đọc file Shapefile
def read_shapefile(shapefile_path):
    try:
        gdf = gpd.read_file(shapefile_path, engine="pyogrio")
        print(f"✅ Đọc file Shapefile {shapefile_path} thành công!")
        return gdf
    except Exception as e:
        print(f"❌ Lỗi khi đọc Shapefile {shapefile_path}: {e}")
        return None

def insert_data_to_postgres(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Dữ liệu đã được lưu vào bảng {table_name} thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi lưu dữ liệu vào PostgreSQL: {e}")
