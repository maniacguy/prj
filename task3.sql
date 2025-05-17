ALTER TABLE stops ADD COLUMN geom_4283 geometry(Point, 4283);

UPDATE stops
SET geom_4283 = ST_SetSRID(ST_MakePoint(longitude, latitude), 4283);

CREATE INDEX stops_geom_idx ON stops USING GIST (geom_4283);

CREATE OR REPLACE FUNCTION sigmoid(x double precision)
RETURNS double precision AS $$
BEGIN
  RETURN 1 / (1 + exp(-x));
END;
$$ LANGUAGE plpgsql;

-- === Metric Aggregation ===

WITH business_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(b.*) AS business_count
    FROM sa2 s
    LEFT JOIN businesses b ON b.sa2_code = s.sa2_code21
    WHERE b.industry_name ILIKE ANY (ARRAY[
        '%Retail%', '%Health%', '%Education%', '%Accommodation%', '%Food%'
    ])
    GROUP BY s.sa2_code21
),

poi_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(p.*) AS poi_count
    FROM sa2 s
    LEFT JOIN poi_csv p ON p.sa2_code21 = s.sa2_code21
    GROUP BY s.sa2_code21
),

school_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(sc.*) AS school_count
    FROM sa2 s
    LEFT JOIN schools sc ON ST_Intersects(ST_Transform(sc.geometry, 4283), s.geometry)
    GROUP BY s.sa2_code21
),

stop_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(st.*) AS stop_count
    FROM sa2 s
    LEFT JOIN stops st ON ST_Within(st.geom_4283, s.geometry)
    GROUP BY s.sa2_code21
),

-- Join population table
population_filtered AS (
    SELECT
        sa2_code21 AS sa2_code21,
        total_population AS population,
        population_0_19
    FROM populations
    WHERE total_population >= 100
),

-- Combine everything
combined AS (
    SELECT
        s.sa2_code21,
        b.business_count,
        p.poi_count,
        sc.school_count,
        st.stop_count,
        pop.population,
        pop.population_0_19,
        (b.business_count * 1000.0) / NULLIF(pop.population, 0) AS business_per_1000,
        (sc.school_count * 1000.0) / NULLIF(pop.population_0_19, 0) AS schools_per_1000young
    FROM sa2 s
    LEFT JOIN population_filtered pop ON s.sa2_code21 = pop.sa2_code21
    LEFT JOIN business_metrics b ON s.sa2_code21 = b.sa2_code21
    LEFT JOIN poi_metrics p ON s.sa2_code21 = p.sa2_code21
    LEFT JOIN school_metrics sc ON s.sa2_code21 = sc.sa2_code21
    LEFT JOIN stop_metrics st ON s.sa2_code21 = st.sa2_code21
),

-- Z-score normalization
z_scores AS (
    SELECT
        sa2_code21,
        business_per_1000,
        poi_count,
        schools_per_1000young,
        stop_count,

        (business_per_1000 - AVG(business_per_1000) OVER()) / NULLIF(STDDEV_SAMP(business_per_1000) OVER(), 0) AS zbusiness,
        (poi_count - AVG(poi_count) OVER()) / NULLIF(STDDEV_SAMP(poi_count) OVER(), 0) AS zpoi,
        (schools_per_1000young - AVG(schools_per_1000young) OVER()) / NULLIF(STDDEV_SAMP(schools_per_1000young) OVER(), 0) AS zschools,
        (stop_count - AVG(stop_count) OVER()) / NULLIF(STDDEV_SAMP(stop_count) OVER(), 0) AS zstops
    FROM combined
)

-- Final score
SELECT
    sa2_code21,
    ROUND(zbusiness, 2) AS zbusiness,
    ROUND(zpoi, 2) AS zpoi,
    ROUND(zschools, 2) AS zschools,
    ROUND(zstops, 2) AS zstops,
    ROUND(sigmoid(zbusiness + zpoi + zschools + zstops)::numeric, 4) AS final_score
FROM z_scores
ORDER BY final_score DESC;
