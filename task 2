def get_bounding_box(geometry):
    minx, miny, maxx, maxy = geometry.bounds
    return minx, miny, maxx, maxy

# Function to fetch points of interest within a bounding box
def fetch_points_of_interest(minx, miny, maxx, maxy):
    """
    Fetches all points of interest within a given bounding box using the NSW Points of Interest API.
    
    Parameters:
    minx, miny, maxx, maxy: The coordinates defining the bounding box
    
    Returns:
    A list of POI features within the bounding box
    """
    # NSW Points of Interest API base URL
    base_url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0/query"
    
    # Define the bounding box as a parameter for the API request
    bbox_str = f"{minx},{miny},{maxx},{maxy}"
    
    # Define parameters for the API request
    params = {
        'geometry': bbox_str,
        'geometryType': 'esriGeometryEnvelope',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': '*',  # Request all fields
        'returnGeometry': 'true',
        'f': 'json'  # Request response in JSON format
    }
    
    # Make the API request
    response = requests.get(base_url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if 'features' in data:
            return data['features']
        else:
            print("No features found in the response.")
            return []
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

# Function to process and convert API response into a DataFrame
def process_poi_response(features):
    """
    Process the API response features into a pandas DataFrame
    """
    if not features:
        return pd.DataFrame()
    
    # Extract attributes and geometry for each feature
    poi_data = []
    for feature in features:
        attributes = feature['attributes']
        geometry = feature.get('geometry', {})
        
        # Add coordinates to attributes
        if geometry:
            attributes['x_coord'] = geometry.get('x')
            attributes['y_coord'] = geometry.get('y')
        
        poi_data.append(attributes)
    
    # Convert to DataFrame
    poi_df = pd.DataFrame(poi_data)
    
    # Select relevant columns - adjust as needed based on the actual API response
    relevant_columns = [
        'NAME', 'DESCRIPTION', 'POITYPE', 'POIgroup', 'CLASS', 'FTYPE', 
        'x_coord', 'y_coord'
    ]
    
    # Filter columns that exist in the dataframe
    existing_columns = [col for col in relevant_columns if col in poi_df.columns]
    poi_df = poi_df[existing_columns]
    
    # Rename columns to lowercase for consistency
    poi_df.columns = [col.lower() for col in poi_df.columns]
    
    return poi_df

# Function to create a GeoDataFrame from POI data
def create_poi_geodataframe(poi_df):
    """
    Convert POI DataFrame to GeoDataFrame with Point geometry
    """
    if poi_df.empty:
        return gpd.GeoDataFrame()
    
    # Create geometry column from coordinates
    if 'x_coord' in poi_df.columns and 'y_coord' in poi_df.columns:
        # Create GeoDataFrame
        poi_gdf = gpd.GeoDataFrame(
            poi_df,
            geometry=gpd.points_from_xy(poi_df.x_coord, poi_df.y_coord),
            crs="EPSG:4326"  # Assuming the API returns WGS84 coordinates
        )
        return poi_gdf
    else:
        print("Coordinate columns not found in POI data")
        return gpd.GeoDataFrame()

# Main function to fetch POIs for all SA2 regions in selected SA4 zones
def fetch_pois_for_sa4_zones(sa4_zones):
    """
    Fetch POIs for all SA2 regions within specified SA4 zones
    
    Parameters:
    sa4_zones: List of SA4 zone names to process
    
    Returns:
    GeoDataFrame containing all POIs with SA2 code assignments
    """
    # Connect to database
    engine = connect_to_db()
    
    # Query SA2 boundaries for the selected SA4 zones
    query = f"""
    SELECT sa2_code, sa2_name, sa4_name, geometry 
    FROM sa2_boundaries 
    WHERE sa4_name IN ({', '.join([f"'{zone}'" for zone in sa4_zones])})
    """
    
    # Load SA2 boundaries for selected SA4 zones
    sa2_gdf = gpd.read_postgis(query, engine, geom_col='geometry')
    
    print(f"Found {len(sa2_gdf)} SA2 regions in the selected SA4 zones")
    
    # Initialize an empty list to store POI data for all SA2 regions
    all_pois = []
    
    # Process each SA2 region
    for idx, row in sa2_gdf.iterrows():
        sa2_code = row['sa2_code']
        sa2_name = row['sa2_name']
        geometry = row['geometry']
        
        print(f"Processing SA2: {sa2_name} ({sa2_code})...")
        
        # Get bounding box for the SA2 region
        minx, miny, maxx, maxy = get_bounding_box(geometry)
        
        # Fetch POIs for this bounding box
        poi_features = fetch_points_of_interest(minx, miny, maxx, maxy)
        
        # Process the response
        poi_df = process_poi_response(poi_features)
        
        if not poi_df.empty:
            # Add SA2 code and name to the POI data
            poi_df['sa2_code'] = sa2_code
            poi_df['sa2_name'] = sa2_name
            
            # Append to the list of all POIs
            all_pois.append(poi_df)
            
            print(f"  Found {len(poi_df)} POIs")
        else:
            print(f"  No POIs found")
        
        # Wait before the next API request to avoid rate limiting
        time.sleep(1)
    
    # Combine all POI data
    if all_pois:
        combined_poi_df = pd.concat(all_pois, ignore_index=True)
        
        # Create GeoDataFrame
        poi_gdf = create_poi_geodataframe(combined_poi_df)
        
        # Import to PostgreSQL
        poi_gdf.to_postgis("points_of_interest", engine, if_exists="replace", index=False)
        
        # Create spatial index
        with engine.connect() as connection:
            connection.execute("CREATE INDEX IF NOT EXISTS idx_poi_geom ON points_of_interest USING GIST (geometry);")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_poi_sa2_code ON points_of_interest (sa2_code);")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_poi_poigroup ON points_of_interest (poigroup);")
        
        print(f"Successfully imported {len(poi_gdf)} POIs to PostgreSQL")
        return poi_gdf
    else:
        print("No POIs found for any SA2 region")
        return gpd.GeoDataFrame()
