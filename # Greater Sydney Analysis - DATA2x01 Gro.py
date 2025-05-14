
# Selected SA4 Zones: Sydney - Eastern Suburbs, Sydney - Inner West, Sydney - North Sydney and Hornsby

import pandas as pd
import geopandas as gpd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import requests
import json
import time
from shapely.geometry import box
import numpy as np
from scipy.stats import zscore
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
import folium
from folium.plugins import MarkerCluster
import os
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
import matplotlib.patches as mpatches

# Create results directory if it doesn't exist
if not os.path.exists('results'):
    os.makedirs('results')

# Connect to PostgreSQL database
def connect_to_db():
    # Update these with your database credentials
    username = "postgres"
    password = "postgres"  # Update this with your actual password
    database = "sydney_analysis"
    
    # Create connection string
    connection_string = f"postgresql://{username}:{password}@localhost:5432/{database}"
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Return the engine
    return engine

# Function to create PostGIS extension if it doesn't exist
def create_postgis_extension(engine):
    with engine.connect() as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        print("PostGIS extension activated.")

# -------------- TASK 1: DATA LOADING AND CLEANING --------------

# Load and clean SA2 digital boundaries
def load_sa2_boundaries():
    print("Loading SA2 boundaries...")
    # Load shapefile
    sa2_gdf = gpd.read_file("data/SA2_Boundaries.shp")
    
    # Filter to Greater Sydney GCC
    sydney_sa2 = sa2_gdf[sa2_gdf['GCC_NAME16'] == 'Greater Sydney']
    
    # Ensure correct SRID
    sydney_sa2 = sydney_sa2.to_crs(epsg=4326)
    
    # Select relevant columns
    sydney_sa2 = sydney_sa2[['SA2_MAIN16', 'SA2_NAME16', 'SA4_NAME16', 'AREASQKM16', 'geometry']]
    
    # Rename columns for clarity
    sydney_sa2 = sydney_sa2.rename(columns={
        'SA2_MAIN16': 'sa2_code',
        'SA2_NAME16': 'sa2_name',
        'SA4_NAME16': 'sa4_name',
        'AREASQKM16': 'area_sqkm'
    })
    
    return sydney_sa2

# Load and clean business data
def load_business_data():
    print("Loading business data...")
    businesses_df = pd.read_csv("data/Businesses.csv")
    
    # Clean column names (remove spaces, lowercase)
    businesses_df.columns = [col.lower().replace(' ', '_') for col in businesses_df.columns]
    
    # Convert SA2 code to string if needed
    if businesses_df['sa2_code'].dtype != 'object':
        businesses_df['sa2_code'] = businesses_df['sa2_code'].astype(str)
    
    # Select industries of interest
    selected_industries = [
        'Retail Trade', 
        'Education and Training',
        'Health Care and Social Assistance',
        'Accommodation and Food Services',
        'Arts and Recreation Services'
    ]
    
    # Filter for selected industries
    businesses_filtered = businesses_df[businesses_df['industry'].isin(selected_industries)]
    
    return businesses_filtered

# Load and clean public transport stops
def load_transport_stops():
    print("Loading transport stops...")
    stops_df = pd.read_csv("data/Stops.txt")
    
    # Clean column names
    stops_df.columns = [col.lower() for col in stops_df.columns]
    
    # Create GeoDataFrame from stops data
    stops_gdf = gpd.GeoDataFrame(
        stops_df, 
        geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat),
        crs="EPSG:4326"
    )
    
    # Select relevant columns
    stops_gdf = stops_gdf[['stop_id', 'stop_name', 'stop_type', 'geometry']]
    
    return stops_gdf

# Load and clean school catchment data
def load_school_catchments():
    print("Loading school catchments...")
    school_gdf = gpd.read_file("data/SchoolCatchments.zip")
    
    # Ensure correct SRID
    school_gdf = school_gdf.to_crs(epsg=4326)
    
    # Select relevant columns
    school_gdf = school_gdf[['SCHOOL_NAME', 'SCHOOL_TYPE', 'geometry']]
    
    # Rename columns
    school_gdf = school_gdf.rename(columns={
        'SCHOOL_NAME': 'school_name',
        'SCHOOL_TYPE': 'school_type'
    })
    
    return school_gdf

# Load population data
def load_population_data():
    print("Loading population data...")
    population_df = pd.read_csv("data/Population.csv")
    
    # Clean column names
    population_df.columns = [col.lower().replace(' ', '_') for col in population_df.columns]
    
    # Convert SA2 code to string if needed
    if population_df['sa2_code'].dtype != 'object':
        population_df['sa2_code'] = population_df['sa2_code'].astype(str)
    
    # Create columns for young population (0-19)
    young_age_columns = [col for col in population_df.columns if any(age in col for age in ['0_4', '5_9', '10_14', '15_19'])]
    population_df['young_population'] = population_df[young_age_columns].sum(axis=1)
    population_df['total_population'] = population_df.filter(regex='_years').sum(axis=1)
    
    return population_df

# Load income data
def load_income_data():
    print("Loading income data...")
    income_df = pd.read_csv("data/Income.csv")
    
    # Clean column names
    income_df.columns = [col.lower().replace(' ', '_') for col in income_df.columns]
    
    # Convert SA2 code to string if needed
    if income_df['sa2_code'].dtype != 'object':
        income_df['sa2_code'] = income_df['sa2_code'].astype(str)
    
    return income_df

# Create indexes for better query performance
def create_indexes(engine):
    print("Creating indexes...")
    
    with engine.connect() as connection:
        # Index on SA2 boundaries
        connection.execute("CREATE INDEX IF NOT EXISTS idx_sa2_boundaries_sa2_code ON sa2_boundaries (sa2_code);")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_sa2_boundaries_sa4_name ON sa2_boundaries (sa4_name);")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_sa2_boundaries_geom ON sa2_boundaries USING GIST (geometry);")
        
        # Index on businesses
        connection.execute("CREATE INDEX IF NOT EXISTS idx_businesses_sa2_code ON businesses (sa2_code);")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_businesses_industry ON businesses (industry);")
        
        # Index on transport stops
        connection.execute("CREATE INDEX IF NOT EXISTS idx_transport_stops_geom ON transport_stops USING GIST (geometry);")
        
        # Index on school catchments
        connection.execute("CREATE INDEX IF NOT EXISTS idx_school_catchments_geom ON school_catchments USING GIST (geometry);")
        
        # Index on population
        connection.execute("CREATE INDEX IF NOT EXISTS idx_population_sa2_code ON population (sa2_code);")
        
        # Index on income
        connection.execute("CREATE INDEX IF NOT EXISTS idx_income_sa2_code ON income (sa2_code);")
    
    print("Indexes created successfully!")

# Import all data to PostgreSQL
def import_to_postgresql():
    # Connect to database
    engine = connect_to_db()
    
    # Create PostGIS extension
    create_postgis_extension(engine)
    
    # Load datasets
    sa2_gdf = load_sa2_boundaries()
    businesses_df = load_business_data()
    stops_gdf = load_transport_stops()
    school_gdf = load_school_catchments()
    population_df = load_population_data()
    income_df = load_income_data()
    
    # Import datasets to PostgreSQL
    print("Importing datasets to PostgreSQL...")
    
    # Import SA2 boundaries
    sa2_gdf.to_postgis("sa2_boundaries", engine, if_exists="replace", index=False)
    
    # Import business data
    businesses_df.to_sql("businesses", engine, if_exists="replace", index=False)
    
    # Import transport stops
    stops_gdf.to_postgis("transport_stops", engine, if_exists="replace", index=False)
    
    # Import school catchments
    school_gdf.to_postgis("school_catchments", engine, if_exists="replace", index=False)
    
    # Import population data
    population_df.to_sql("population", engine, if_exists="replace", index=False)
    
    # Import income data
    income_df.to_sql("income", engine, if_exists="replace", index=False)
    
    # Create indexes for better performance
    create_indexes(engine)
    
    print("All datasets imported successfully!")
    
    return engine
