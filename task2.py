import requests
import time
import pg8000
import geopandas as gpd

class NSWPointsOfInterestAPI:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_poi_within_bbox(self, min_lat, min_lon, max_lat, max_lon):
        """
        i) Return all points of interest within bounding box (min_lat, min_lon, max_lat, max_lon)
        """
        url = f"{self.base_url}/query"
        params = {
            "f": "json",
            "geometry": f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*"
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("features", [])
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching POI data: {e}")
            return []

class SA2DataProcessor:
    def __init__(self, db_config, shapefile_path, poi_api, selected_sa4):
        self.db_config = db_config
        self.shapefile_path = shapefile_path
        self.poi_api = poi_api
        self.selected_sa4 = selected_sa4  # SA4 code to filter SA2 regions within it

    def connect(self):
        try:
            conn = pg8000.connect(**self.db_config)
            print("✅ Connected to PostgreSQL!")
            return conn
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            return None

    def insert_pois(self, conn, pois):
        """
        iii) Insert POIs into DB with meaningful columns, respecting NSW Topographic Data Dictionary.
        Adjust columns as necessary.
        """
        insert_query = """
        INSERT INTO points_of_interest (
            poigroup, poitype, poiname, poilabel, shape, startdate, enddate, lastupdate
        ) VALUES (
            %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326), %s, %s, %s
        )
        ON CONFLICT (poiname, poilabel) DO NOTHING;
        """
        try:
            with conn.cursor() as cur:
                for poi in pois:
                    attr = poi.get('attributes', {})
                    geom = poi.get('geometry', None)

                    if geom is None:
                        continue  # Skip POIs with no geometry

                    data = (
                        attr.get('poigroup'),
                        attr.get('poitype', 'Unknown'),
                        attr.get('poiname', 'Unknown'),
                        attr.get('poilabel', 'Unknown'),
                        f"POINT({geom['x']} {geom['y']})",
                        attr.get('startdate'),
                        attr.get('enddate'),
                        attr.get('lastupdate')
                    )

                    try:
                        cur.execute(insert_query, data)
                    except Exception as e:
                        print(f"⚠️ Insert error for POI {attr.get('poiname')}: {e}")
                conn.commit()
                print(f"✅ Inserted {len(pois)} POIs successfully.")
        except Exception as e:
            print(f"❌ Error during POI insertion: {e}")
            conn.rollback()

    def process_data(self):
        print(f"📂 Reading Shapefile: {self.shapefile_path}...")
        try:
            gdf = gpd.read_file(self.shapefile_path, engine="pyogrio")
            print("✅ Shapefile read successfully.")
            return gdf
        except Exception as e:
            print(f"❌ Error reading Shapefile: {e}")
            return None

    def process_sa2_within_sa4(self, conn):
        """
        ii) Loop through SA2 regions within the selected SA4, get POIs for each,
        wait 1 second between calls, and insert all POIs into the DB.
        """
        gdf = self.process_data()
        if gdf is None:
            return
        
        # Filter SA2 regions inside the selected SA4 region
        sa2_within_sa4 = gdf[gdf['SA4_CODE21'] == self.selected_sa4]

        for idx, row in sa2_within_sa4.iterrows():
            sa2_code = row['SA2_CODE21']
            bounds = row['geometry'].bounds  # returns (minx, miny, maxx, maxy)
            min_lon, min_lat, max_lon, max_lat = bounds
            print(f"📍 Processing POIs for SA2 {sa2_code}...")

            pois = self.poi_api.get_poi_within_bbox(min_lat, min_lon, max_lat, max_lon)

            if pois:
                self.insert_pois(conn, pois)
            else:
                print(f"⚠️ No POIs found for SA2 {sa2_code}.")

            time.sleep(1)  # wait 1 second to respect API limits

# === Configuration ===
db_config = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

shapefile_path = 'data/SA2_2021_AUST_SHP_GDA2020/SA2_2021_AUST_GDA2020.shp'
poi_api_url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0"

selected_sa4 = "11601"  
poi_api = NSWPointsOfInterestAPI(poi_api_url)
processor = SA2DataProcessor(db_config, shapefile_path, poi_api, selected_sa4)

conn = processor.connect()
if conn:
    processor.process_sa2_within_sa4(conn)
    conn.close()
