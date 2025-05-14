import pg8000

def create_poi_table(conn):
    """Tạo bảng points_of_interest nếu chưa tồn tại."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS points_of_interest (
        poigroup VARCHAR(255),
        poitype VARCHAR(255),
        poiname VARCHAR(255),
        poilabel VARCHAR(255),
        shape geometry,
        startdate DATE,
        enddate DATE,
        lastupdate TIMESTAMP,
        PRIMARY KEY (poigroup)
    );
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✅ Tạo bảng points_of_interest thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng: {e}")
        conn.rollback()

# Cấu hình kết nối đến PostgreSQL
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

# Kết nối đến cơ sở dữ liệu
conn = pg8000.connect(**db_config)

# Tạo bảng
create_poi_table(conn)

# Đóng kết nối
conn.close()
