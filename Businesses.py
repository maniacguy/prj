import pg8000
import pandas as pd

class BusinessesDataProcessor:
    def __init__(self, db_config, csv_path):
        self.db_config = db_config
        self.csv_path = csv_path

    def connect(self):
        """Kết nối đến PostgreSQL."""
        try:
            conn = pg8000.connect(**self.db_config)
            print("✅ Kết nối đến PostgreSQL thành công!")
            return conn
        except Exception as e:
            print(f"❌ Lỗi kết nối: {e}")

    def create_table(self, conn):
        """Tạo bảng Businesses nếu chưa tồn tại."""
        # Xóa bảng cũ nếu có
        drop_table_query = "DROP TABLE IF EXISTS Businesses;"
        with conn.cursor() as cur:
            cur.execute(drop_table_query)
            conn.commit()
        
        # Tạo bảng mới
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Businesses (
            industry_code VARCHAR(10),
            industry_name VARCHAR(255),
            sa2_code VARCHAR(15) PRIMARY KEY,
            sa2_name VARCHAR(255),
            "0_to_50k_businesses" INTEGER,
            "50k_to_200k_businesses" INTEGER,
            "200k_to_500k_businesses" INTEGER,
            "500k_to_2m_businesses" INTEGER,
            "2m_to_5m_businesses" INTEGER,
            "5m_to_10m_businesses" INTEGER,
            "10m_or_more_businesses" INTEGER,
            total_businesses INTEGER
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✅ Tạo bảng Businesses thành công!")

    def normalize_data(self, df):
        """Chuẩn hóa và tính toán lại dữ liệu với các phân đoạn doanh thu hợp lý hơn."""
        # Kiểm tra tên các cột
        print("Tên cột trong CSV:", df.columns)
        
        # Chuẩn hóa tên cột (nếu cần)
        df.columns = [col.replace("-", "_").replace(" ", "_").lower() for col in df.columns]
        
        # Kiểm tra các cột cần thiết và xử lý dữ liệu thiếu (nếu có)
        required_columns = [
            '0_to_50k_businesses', '50k_to_200k_businesses', '200k_to_2m_businesses',
            '2m_to_5m_businesses', '5m_to_10m_businesses', '10m_or_more_businesses'
        ]
        
        # Đảm bảo các cột không bị thiếu
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Các cột sau không tồn tại trong dữ liệu: {missing_columns}")
            return df
        
        # Xử lý dữ liệu thiếu (nếu có), ví dụ thay thế bằng 0
        df[required_columns] = df[required_columns].fillna(0)

        # Cập nhật lại các khoảng doanh thu hợp lý hơn
        # Chia lại các khoảng từ 200k đến 2 triệu thành 2 phân đoạn nhỏ hơn
        df['200k_to_500k_businesses'] = (df['200k_to_2m_businesses'] * 0.25).astype(int)  # 25% cho 200k-500k
        df['500k_to_2m_businesses'] = (df['200k_to_2m_businesses'] * 0.75).astype(int)  # 75% cho 500k-2m
        
        # Tính lại tổng doanh nghiệp
        df['total_businesses'] = df[required_columns + ['200k_to_500k_businesses', '500k_to_2m_businesses']].sum(axis=1)

        return df

    def insert_data(self, conn):
        """Chèn dữ liệu từ CSV vào bảng."""
        df = pd.read_csv(self.csv_path)
        
        # Chuẩn hóa dữ liệu
        df = self.normalize_data(df)
        
        insert_query = """
        INSERT INTO Businesses (industry_code, industry_name, sa2_code, sa2_name,
                                "0_to_50k_businesses", "50k_to_200k_businesses",
                                "200k_to_500k_businesses", "500k_to_2m_businesses",
                                "2m_to_5m_businesses", "5m_to_10m_businesses", "10m_or_more_businesses", total_businesses)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sa2_code) DO NOTHING;
        """
        
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                data = (
                    row['industry_code'],
                    row['industry_name'],
                    str(row['sa2_code']),
                    str(row['sa2_name']),
                    int(row['0_to_50k_businesses']),
                    int(row['50k_to_200k_businesses']),
                    int(row['200k_to_500k_businesses']),
                    int(row['500k_to_2m_businesses']),
                    int(row['2m_to_5m_businesses']),
                    int(row['5m_to_10m_businesses']),
                    int(row['10m_or_more_businesses']),
                    int(row['total_businesses'])
                )
                cur.execute(insert_query, data)
            conn.commit()
            print("✅ Chèn dữ liệu thành công!")

# Cấu hình database
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

csv_path = 'data/Businesses.csv'
processor = BusinessesDataProcessor(db_config, csv_path)
conn = processor.connect()
if conn:
    processor.create_table(conn)
    processor.insert_data(conn)
    conn.close()
