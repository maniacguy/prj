import pg8000
import pandas as pd

class IncomeDataProcessor:
    def __init__(self, db_config, csv_path):
        self.db_config = db_config
        self.csv_path = csv_path

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
        """Tạo bảng Income nếu chưa tồn tại."""
        drop_table_query = "DROP TABLE IF EXISTS Income;"
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Income (
            sa2_code21 VARCHAR(15) PRIMARY KEY,
            sa2_name VARCHAR(255),
            earners INTEGER,
            median_age INTEGER,
            median_income INTEGER,
            mean_income INTEGER
        );
        """

        try:
            with conn.cursor() as cur:
                cur.execute(drop_table_query)
                cur.execute(create_table_query)
                conn.commit()
                print("✅ Tạo bảng Income thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi tạo bảng: {e}")
            conn.rollback()

    def clean_data(self, df):
        """Chuẩn hóa dữ liệu từ CSV."""
        # Loại bỏ các ký tự thừa trong tên cột
        df.columns = [col.strip().replace("-", "_").replace(" ", "_").lower() for col in df.columns]
        
        # Thay thế các giá trị không hợp lệ (ví dụ: 'np' hoặc NaN) bằng NaN
        df.replace('np', pd.NA, inplace=True)

        # Chuyển đổi các giá trị không hợp lệ thành NaN và thay thế NaN bằng giá trị trung bình hoặc 0
        df['earners'] = pd.to_numeric(df['earners'], errors='coerce').fillna(0).astype(int)
        df['median_age'] = pd.to_numeric(df['median_age'], errors='coerce').fillna(df['median_age'].median()).astype(int)
        df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce').fillna(df['median_income'].median()).astype(int)
        df['mean_income'] = pd.to_numeric(df['mean_income'], errors='coerce').fillna(df['mean_income'].median()).astype(int)

        print(f"✅ Đã chuẩn hóa dữ liệu:\n{df.head()}")
        return df

    def insert_data(self, conn, df):
        """Chèn dữ liệu từ DataFrame vào bảng."""
        insert_query = """
        INSERT INTO Income (sa2_code21, sa2_name, earners, median_age, median_income, mean_income)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sa2_code21) DO NOTHING;
        """
        try:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    data = (
                        str(row['sa2_code21']),
                        row['sa2_name'],
                        int(row['earners']),
                        int(row['median_age']),
                        int(row['median_income']),
                        int(row['mean_income'])
                    )
                    cur.execute(insert_query, data)
                conn.commit()
                print("✅ Chèn dữ liệu thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi chèn dữ liệu: {e}")
            conn.rollback()

    def process_data(self):
        """Quy trình xử lý toàn bộ dữ liệu từ CSV."""
        print(f"📂 Đang xử lý file {self.csv_path}")
        df = pd.read_csv(self.csv_path)
        print(f"✅ Đọc file CSV {self.csv_path} thành công!")
        print(f"✅ Đã tải dữ liệu Income:\n{df.head()}")

        # Chuẩn hóa dữ liệu
        df = self.clean_data(df)
        return df

# Cấu hình database
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

csv_path = 'data/Income.csv'

# Khởi tạo đối tượng xử lý dữ liệu
processor = IncomeDataProcessor(db_config, csv_path)

# Kết nối đến database
conn = processor.connect()
if conn:
    # Xử lý dữ liệu từ CSV
    df = processor.process_data()
    
    # Tạo bảng và chèn dữ liệu
    processor.create_table(conn)
    processor.insert_data(conn, df)
    
    # Đóng kết nối
    conn.close()
