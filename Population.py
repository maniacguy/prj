import pg8000
import pandas as pd

class PopulationDataProcessor:
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
        """Tạo bảng Population nếu chưa tồn tại."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Population (
            sa2_code VARCHAR(15) PRIMARY KEY,
            sa2_name VARCHAR(255),
            "0_to_10_people" INTEGER,
            "11_to_20_people" INTEGER,
            "21_to_30_people" INTEGER,
            "31_to_40_people" INTEGER,
            "41_to_50_people" INTEGER,
            "51_to_60_people" INTEGER,
            "61_to_70_people" INTEGER,
            "71_to_80_people" INTEGER,
            "81_and_over_people" INTEGER,
            total_people INTEGER
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✅ Tạo bảng Population thành công!")

    def normalize_data(self, df):
        """Chuẩn hóa dữ liệu trước khi chèn vào cơ sở dữ liệu."""
        # Kiểm tra tên các cột
        print("Tên cột trong CSV:", df.columns)

        # Kiểm tra và xử lý các cột nếu chúng không tồn tại
        required_columns = [
            '0_4_people', '5_9_people', '10_14_people', '15_19_people', '20_24_people',
            '25_29_people', '30_34_people', '35_39_people', '40_44_people', '45_49_people',
            '50_54_people', '55_59_people', '60_64_people', '65_69_people', '70_74_people',
            '75_79_people', '80_84_people', '85_and_over_people'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Các cột sau không tồn tại trong dữ liệu: {missing_columns}")
            return df
        
        # Gộp các nhóm tuổi lại thành các nhóm lớn hơn
        df['0_to_10_people'] = df['0_4_people'] + df['5_9_people']
        df['11_to_20_people'] = df['10_14_people'] + df['15_19_people']
        df['21_to_30_people'] = df['20_24_people'] + df['25_29_people']
        df['31_to_40_people'] = df['30_34_people'] + df['35_39_people']
        df['41_to_50_people'] = df['40_44_people'] + df['45_49_people']
        df['51_to_60_people'] = df['50_54_people'] + df['55_59_people']
        df['61_to_70_people'] = df['60_64_people'] + df['65_69_people']
        df['71_to_80_people'] = df['70_74_people'] + df['75_79_people']
        df['81_and_over_people'] = df['80_84_people'] + df['85_and_over_people']

        # Tính tổng số người
        df['total_people'] = df[['0_to_10_people', '11_to_20_people', '21_to_30_people', '31_to_40_people', 
                                '41_to_50_people', '51_to_60_people', '61_to_70_people', '71_to_80_people', 
                                '81_and_over_people']].sum(axis=1)

        # Loại bỏ các cột cũ
        df = df.drop(columns=required_columns)

        # Chuyển đổi các cột số liệu từ kiểu chuỗi sang kiểu số nguyên
        numeric_columns = [
            '0_to_10_people', '11_to_20_people', '21_to_30_people', '31_to_40_people', '41_to_50_people',
            '51_to_60_people', '61_to_70_people', '71_to_80_people', '81_and_over_people', 'total_people'
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df


    def insert_data(self, conn):
        """Chèn dữ liệu đã chuẩn hóa từ CSV vào bảng."""
        df = pd.read_csv(self.csv_path)
        # Chuẩn hóa tên cột
        df.columns = [col.replace("-", "_").replace(" ", "_").lower() for col in df.columns]
        
        # Chuẩn hóa dữ liệu
        df = self.normalize_data(df)
        
        insert_query = """
        INSERT INTO Population (sa2_code, sa2_name, "0_to_10_people", "11_to_20_people", "21_to_30_people", "31_to_40_people",
                            "41_to_50_people", "51_to_60_people", "61_to_70_people", "71_to_80_people", 
                            "81_and_over_people", total_people)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sa2_code) DO NOTHING;
        """

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                data = (
                    row['sa2_code'],
                    row['sa2_name'],
                    int(row['0_to_10_people']),
                    int(row['11_to_20_people']),
                    int(row['21_to_30_people']),
                    int(row['31_to_40_people']),
                    int(row['41_to_50_people']),
                    int(row['51_to_60_people']),
                    int(row['61_to_70_people']),
                    int(row['71_to_80_people']),
                    int(row['81_and_over_people']),
                    int(row['total_people'])  # Đảm bảo `total_people` được đưa vào
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

csv_path = 'data/Population.csv'
processor = PopulationDataProcessor(db_config, csv_path)
conn = processor.connect()
if conn:
    processor.create_table(conn)
    processor.insert_data(conn)
    conn.close()
