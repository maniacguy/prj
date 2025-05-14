import pg8000
import pandas as pd

class StopsDataProcessor:
    def __init__(self, db_config, txt_path):
        self.db_config = db_config
        self.txt_path = txt_path

    def connect(self):
        """Kết nối đến PostgreSQL."""
        try:
            conn = pg8000.connect(**self.db_config)
            print("✅ Kết nối đến PostgreSQL thành công!")
            return conn
        except Exception as e:
            print(f"❌ Lỗi kết nối: {e}")

    def create_table(self, conn):
        """Tạo bảng stops nếu chưa tồn tại."""
        drop_table_query = "DROP TABLE IF EXISTS stops;"
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stops (
            stop_id VARCHAR(50) PRIMARY KEY,
            stop_code VARCHAR(50),
            stop_name VARCHAR(255),
            stop_lat DOUBLE PRECISION,
            stop_lon DOUBLE PRECISION,
            location_type VARCHAR(50),
            parent_station VARCHAR(50),
            wheelchair_boarding VARCHAR(50),
            platform_code VARCHAR(50)
        );
        """
        with conn.cursor() as cur:
            try:
                cur.execute(drop_table_query)
                conn.commit()
                print("✅ Xóa bảng 'stops' cũ thành công!")
                cur.execute(create_table_query)
                conn.commit()
                print("✅ Tạo bảng 'stops' thành công!")
            except Exception as e:
                print(f"❌ Lỗi khi tạo bảng: {e}")
                conn.rollback()

    def read_data(self):
        """Đọc dữ liệu từ file Stops.txt."""
        try:
            df = pd.read_csv(self.txt_path, delimiter=",", quotechar='"')
            print("✅ Đã đọc dữ liệu từ file Stops.txt:")
            print(df.head())
            return df
        except Exception as e:
            print(f"❌ Lỗi khi đọc file: {e}")

    def normalize_data(self, df):
        """Chuẩn hóa dữ liệu từ file Stops.txt."""
        # Thay thế giá trị NaN bằng None
        df = df.where(pd.notnull(df), None)

        # Xử lý các cột dạng số thành dạng chuỗi (nếu cần)
        df['stop_id'] = df['stop_id'].astype(str).str.replace('"', '').str.strip()
        df['stop_code'] = df['stop_code'].astype(str).str.replace('"', '').str.strip()
        df['stop_name'] = df['stop_name'].astype(str).str.replace('"', '').str.strip()
        
        return df

    def insert_data(self, conn, df):
        """Chèn dữ liệu từ DataFrame vào bảng stops."""
        insert_query = """
        INSERT INTO stops (stop_id, stop_code, stop_name, stop_lat, stop_lon, location_type, parent_station, wheelchair_boarding, platform_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stop_id) DO NOTHING;
        """
        with conn.cursor() as cur:
            try:
                for _, row in df.iterrows():
                    data = (
                        row['stop_id'],
                        row['stop_code'],
                        row['stop_name'],
                        float(row['stop_lat']) if row['stop_lat'] else None,
                        float(row['stop_lon']) if row['stop_lon'] else None,
                        row['location_type'],
                        row['parent_station'],
                        row['wheelchair_boarding'],
                        row['platform_code']
                    )
                    cur.execute(insert_query, data)
                conn.commit()
                print("✅ Đã chèn dữ liệu vào bảng 'stops' thành công!")
            except Exception as e:
                print(f"❌ Lỗi khi chèn dữ liệu: {e}")
                conn.rollback()

# Cấu hình kết nối đến PostgreSQL
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

# Đường dẫn đến file Stops.txt
txt_path = 'data/Stops.txt'

# Khởi tạo đối tượng xử lý dữ liệu
processor = StopsDataProcessor(db_config, txt_path)

# Thực thi các bước xử lý dữ liệu
conn = processor.connect()
if conn:
    processor.create_table(conn)  # Tạo bảng
    df = processor.read_data()    # Đọc dữ liệu từ file
    if df is not None:
        df = processor.normalize_data(df)  # Chuẩn hóa dữ liệu
        processor.insert_data(conn, df)    # Chèn dữ liệu vào bảng
    conn.close()
