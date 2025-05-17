import pandas as pd
from shapely import wkb
import pg8000

# Load CSV
df = pd.read_csv("Population.csv")

# Clean column names
df.columns = [col.strip() for col in df.columns]

# Create '0_19' column
df["0_19"] = (
    df["0-4_people"] +
    df["5-9_people"] +
    df["10-14_people"] +
    df["15-19_people"]
)

# Database configuration
db_config = {
    "user": "postgres",
    "password": "Thinh123!",
    "host": "localhost",
    "port": 5432,
    "database": "postgres"
}

# Create SQL table
create_table_sql = """
CREATE TABLE IF NOT EXISTS population_data (
    sa2_code TEXT PRIMARY KEY,
    sa2_name TEXT,
    "0-4_people" INTEGER,
    "5-9_people" INTEGER,
    "10-14_people" INTEGER,
    "15-19_people" INTEGER,
    "20-24_people" INTEGER,
    "25-29_people" INTEGER,
    "30-34_people" INTEGER,
    "35-39_people" INTEGER,
    "40-44_people" INTEGER,
    "45-49_people" INTEGER,
    "50-54_people" INTEGER,
    "55-59_people" INTEGER,
    "60-64_people" INTEGER,
    "65-69_people" INTEGER,
    "70-74_people" INTEGER,
    "75-79_people" INTEGER,
    "80-84_people" INTEGER,
    "85-and-over_people" INTEGER,
    total_people INTEGER,
    "0_19" INTEGER
);
"""

# Insert statement
insert_sql = """
INSERT INTO population_data (
    sa2_code, sa2_name, "0-4_people", "5-9_people", "10-14_people", "15-19_people",
    "20-24_people", "25-29_people", "30-34_people", "35-39_people",
    "40-44_people", "45-49_people", "50-54_people", "55-59_people",
    "60-64_people", "65-69_people", "70-74_people", "75-79_people",
    "80-84_people", "85-and-over_people", total_people, "0_19"
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (sa2_code) DO NOTHING;
"""

# Connect and execute
conn = pg8000.connect(**db_config)
cur = conn.cursor()
cur.execute(create_table_sql)
conn.commit()

for _, row in df.iterrows():
    cur.execute(insert_sql, (
        row["sa2_code"], row["sa2_name"], row["0-4_people"], row["5-9_people"],
        row["10-14_people"], row["15-19_people"], row["20-24_people"],
        row["25-29_people"], row["30-34_people"], row["35-39_people"],
        row["40-44_people"], row["45-49_people"], row["50-54_people"],
        row["55-59_people"], row["60-64_people"], row["65-69_people"],
        row["70-74_people"], row["75-79_people"], row["80-84_people"],
        row["85-and-over_people"], row["total_people"], row["0_19"]
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Population data inserted into PostgreSQL with '0_19' column.")
