
# SQL function to compute z-scores
def create_zscore_function(engine):
    """Create a PostgreSQL function to calculate z-scores"""
    with engine.connect() as connection:
        connection.execute("""
        CREATE OR REPLACE FUNCTION z_score(values NUMERIC[])
        RETURNS NUMERIC[] AS $$
        DECLARE
            avg_val NUMERIC;
            std_val NUMERIC;
            result NUMERIC[];
        BEGIN
            -- Calculate mean
            SELECT AVG(val) INTO avg_val FROM unnest(values) AS val;
            
            -- Calculate standard deviation
            SELECT SQRT(AVG(POWER(val - avg_val, 2))) INTO std_val FROM unnest(values) AS val;
            
            -- Calculate z-scores
            IF std_val = 0 THEN
                -- Handle case where all values are the same
                SELECT array_agg(0) INTO result FROM unnest(values) AS val;
            ELSE
                SELECT array_agg((val - avg_val) / std_val) INTO result FROM unnest(values) AS val;
            END IF;
            
            RETURN result;
        END;
        $$ LANGUAGE plpgsql;
        """)
        print("Z-score function created in PostgreSQL.")

# SQL function to compute sigmoid of a value
def create_sigmoid_function(engine):
    """Create a PostgreSQL function to calculate sigmoid"""
    with engine.connect() as connection:
        connection.execute("""
        CREATE OR REPLACE FUNCTION sigmoid(x NUMERIC)
        RETURNS NUMERIC AS $$
        BEGIN
            RETURN 1.0 / (1.0 + EXP(-x));
        END;
        $$ LANGUAGE plpgsql;
        """)
        print("Sigmoid function created in PostgreSQL.")

# Main SQL query to calculate scores
def calculate_scores_sql(sa4_zones):
    """
    SQL query to calculate well-resourced scores for SA2 regions
    This combines all datasets and computes the final score
    """
    # Format the SA4 zones for SQL query
    sa4_zones_str = ', '.join([f"'{zone}'" for zone in sa4_zones])
    
    sql = f"""
    -- Create temporary table with base SA2 data and filtering out low population areas
    WITH sa2_base AS (
        SELECT 
            sb.sa2_code,
            sb.sa2_name,
            sb.sa4_name,
            sb.area_sqkm,
            sb.geometry,
            p.total_population,
            p.young_population
        FROM 
            sa2_boundaries sb
        JOIN 
            population p ON sb.sa2_code = p.sa2_code
        WHERE 
            p.total_population >= 100
            AND sb.sa4_name IN ({sa4_zones_str})
    ),
    
    -- Calculate business metric
    business_metric AS (
        SELECT 
            sa2.sa2_code,
            COUNT(b.id) AS business_count,
            (COUNT(b.id)::NUMERIC / sa2.total_population * 1000) AS businesses_per_1000
        FROM 
            sa2_base sa2
        LEFT JOIN 
            businesses b ON sa2.sa2_code = b.sa2_code
        WHERE 
            b.industry IN ('Retail Trade', 'Education and Training', 'Health Care and Social Assistance', 
                         'Accommodation and Food Services', 'Arts and Recreation Services')
        GROUP BY 
            sa2.sa2_code, sa2.total_population
    ),
    
    -- Calculate transport stops metric
    stops_metric AS (
        SELECT 
            sa2.sa2_code,
            COUNT(ts.stop_id) AS stops_count
        FROM 
            sa2_base sa2
        LEFT JOIN 
            transport_stops ts ON ST_Contains(sa2.geometry, ts.geometry)
        GROUP BY 
            sa2.sa2_code
    ),
    
    -- Calculate schools metric
    schools_metric AS (
        SELECT 
            sa2.sa2_code,
            COUNT(sc.school_name) AS school_catchments,
            (COUNT(sc.school_name)::NUMERIC / NULLIF(sa2.young_population, 0) * 1000) AS catchments_per_1000_young
        FROM 
            sa2_base sa2
        LEFT JOIN 
            school_catchments sc ON ST_Intersects(sa2.geometry, sc.geometry)
        GROUP BY 
            sa2.sa2_code, sa2.young_population
    ),
    
    -- Calculate POI metric
    poi_metric AS (
        SELECT 
            sa2.sa2_code,
            COUNT(poi.name) AS poi_count
        FROM 
            sa2_base sa2
        LEFT JOIN 
            points_of_interest poi ON sa2.sa2_code = poi.sa2_code
        WHERE 
            poi.poigroup IN ('RECREATION', 'EDUCATION', 'HEALTH', 'COMMUNITY', 'RETAIL')
        GROUP BY 
            sa2.sa2_code
    ),
    
    -- Combine all metrics
    combined_metrics AS (
        SELECT 
            sa2.sa2_code,
            sa2.sa2_name,
            sa2.sa4_name,
            sa2.area_sqkm,
            sa2.geometry,
            sa2.total_population,
            sa2.young_population,
            COALESCE(bm.businesses_per_1000, 0) AS businesses_per_1000,
            COALESCE(sm.stops_count, 0) AS stops_count,
            COALESCE(scm.catchments_per_1000_young, 0) AS catchments_per_1000_young,
            COALESCE(pm.poi_count, 0) AS poi_count,
            i.median_income
        FROM 
            sa2_base sa2
        LEFT JOIN 
            business_metric bm ON sa2.sa2_code = bm.sa2_code
        LEFT JOIN 
            stops_metric sm ON sa2.sa2_code = sm.sa2_code
        LEFT JOIN 
            schools_metric scm ON sa2.sa2_code = scm.sa2_code
        LEFT JOIN 
            poi_metric pm ON sa2.sa2_code = pm.sa2_code
        LEFT JOIN 
            income i ON sa2.sa2_code = i.sa2_code
    ),
    
    -- Compute z-scores
    z_scores AS (
        SELECT 
            *,
            -- Apply z-score function to each metric across all rows
            (z_score(ARRAY_AGG(businesses_per_1000) OVER ()))[row_number() OVER ()] AS z_business,
            (z_score(ARRAY_AGG(stops_count) OVER ()))[row_number() OVER ()] AS z_stops,
            (z_score(ARRAY_AGG(catchments_per_1000_young) OVER ()))[row_number() OVER ()] AS z_schools,
            (z_score(ARRAY_AGG(poi_count) OVER ()))[row_number() OVER ()] AS z_poi
        FROM 
            combined_metrics
    )
    
    -- Calculate final score using sigmoid function
    SELECT 
        sa2_code,
        sa2_name,
        sa4_name,
        area_sqkm,
        geometry,
        total_population,
        young_population,
        businesses_per_1000,
        stops_count,
        catchments_per_1000_young,
        poi_count,
        median_income,
        z_business,
        z_stops,
        z_schools,
        z_poi,
        -- Final score formula: sigmoid(z_business + z_stops + z_schools + z_poi)
        sigmoid(z_business + z_stops + z_schools + z_poi) AS score
    FROM 
        z_scores
    ORDER BY 
        score DESC;
    """
    return sql

# Execute score calculation and save results
def calculate_and_save_scores(sa4_zones):
    # Connect to database
    engine = connect_to_db()
    
    # Create necessary SQL functions
    create_zscore_function(engine)
    create_sigmoid_function(engine)
    
    # Get SQL query
    sql = calculate_scores_sql(sa4_zones)
    
    # Execute query and load results
    print("Calculating scores...")
    scores_df = gpd.read_postgis(sql, engine, geom_col='geometry')
    
    # Save results to CSV
    scores_df.drop(columns=['geometry']).to_csv("results/sa2_scores.csv", index=False)
    
    # Save results to database
    scores_df.to_postgis("sa2_scores", engine, if_exists="replace", index=False)
    
    print(f"Scores calculated for {len(scores_df)} SA2 regions and saved to database and CSV")
    
    return scores_df

# -------------- TASK 4: VISUALIZATION AND REPORT GENERATION --------------

# Function to create distribution plots for scores
def visualize_score_distribution(scores_df):
    """
    Create visualizations for score distribution
    """
    # Overall distribution of scores
    plt.figure(figsize=(12, 6))
    
    # Histogram with kernel density estimate
    sns.histplot(scores_df['score'], kde=True)
    plt.title('Distribution of Well-Resourced Scores Across All SA2 Regions')
    plt.xlabel('Score')
    plt.ylabel('Frequency')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('results/score_distribution.png')
    
    # Distribution by SA4 zone
    plt.figure(figsize=(12, 8))
    sns.boxplot(x='sa4_name', y='score', data=scores_df)
    plt.title('Distribution of Well-Resourced Scores by SA4 Zone')
    plt.xlabel('SA4 Zone')
    plt.ylabel('Score')
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('results/score_by_sa4.png')
    
    # Component contribution to scores
    plt.figure(figsize=(12, 8))
    
    # Melt the DataFrame to get component scores in long format
    components_df = scores_df[['sa2_name', 'z_business', 'z_stops', 'z_schools', 'z_poi']].copy()
    components_melted = pd.melt(components_df, id_vars=['sa2_name'], 
                             value_vars=['z_business', 'z_stops', 'z_schools', 'z_poi'],
                             var_name='Component', value_name='Z-Score')
    
    # Create stacked bar chart for top 15 SA2 regions by total score
    top15_sa2 = scores_df.sort_values('score', ascending=False).head(15)['sa2_name'].tolist()
    top_components = components_melted[components_melted['sa2_name'].isin(top15_sa2)]
    
    # Plot
    sns.barplot(x='sa2_name', y='Z-Score', hue='Component', data=top_components)
    plt.title('Z-Score Components for Top 15 SA2 Regions')
    plt.xlabel('SA2 Region')
    plt.ylabel('Z-Score Contribution')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Component')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('results/score_components.png')
    
    print("Score distribution visualizations created and saved.")

# Function to create map visualization of scores
def create_score_map(scores_df):
    """
    Create a choropleth map of SA2 regions colored by their scores
    """
    # Create a folium map centered on Sydney
    m = folium.Map(location=[-33.8688, 151.2093], zoom_start=10, tiles='CartoDB positron')
    
    # Add a choropleth layer
    folium.Choropleth(
        geo_data=scores_df,
        name='Well-Resourced Score',
        data=scores_df,
        columns=['sa2_code', 'score'],
        key_on='feature.properties.sa2_code',
        fill_color='YlGnBu',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Well-Resourced Score'
    ).add_to(m)
    
    # Add tooltips to show region name and score on hover
    folium.GeoJson(
        scores_df,
        name='SA2 Regions',
        style_function=lambda x: {'fillColor': 'transparent', 'color': 'black', 'weight': 1},
        tooltip=folium.GeoJsonTooltip(
            fields=['sa2_name', 'sa4_name', 'score'],
            aliases=['SA2 Region:', 'SA4 Zone:', 'Score:'],
            localize=True,
            sticky=False,
            labels=True
        )
    ).add_to(m)
    
    # Save the map
    m.save('results/score_map.html')
    print("Score map created and saved.")
    
    # Create another map showing component contributions
    # For this, we'll create a static map using matplotlib
    fig, axs = plt.subplots(2, 2, figsize=(15, 15))
    
    # Plot each
