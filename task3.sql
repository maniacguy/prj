-- Only transform stops to match SRID
DO $$
BEGIN
    BEGIN
        ALTER TABLE stops DROP COLUMN IF EXISTS geometry;
    EXCEPTION WHEN undefined_column THEN
        RAISE NOTICE 'Geometry column did not exist.';
    END;

    ALTER TABLE stops ADD COLUMN geometry GEOMETRY(Point, 4326);
    UPDATE stops SET geometry = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326);

    ALTER TABLE stops ADD COLUMN geom_4283 GEOMETRY(Point, 4283);
    UPDATE stops SET geom_4283 = ST_Transform(geometry, 4283);
END
$$;

-- Compute z-scores and final score
WITH business_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(b.*) AS business_count
    FROM sa2 s
    LEFT JOIN businesses b ON b.sa2_code = s.sa2_code21
    GROUP BY s.sa2_code21
),

poi_metrics AS (
    SELECT
        s.sa2_code21,
        COUNT(p.*) AS poi_count
    FROM sa2 s
    LEFT JOIN points_of_interest p ON p.sa2_code = s.sa2_code21
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

combined AS (
    SELECT
        s.sa2_code21,
        b.business_count,
        p.poi_count,
        sc.school_count,
        st.stop_count
    FROM sa2 s
    LEFT JOIN business_metrics b ON s.sa2_code21 = b.sa2_code21
    LEFT JOIN poi_metrics p ON s.sa2_code21 = p.sa2_code21
    LEFT JOIN school_metrics sc ON s.sa2_code21 = sc.sa2_code21
    LEFT JOIN stop_metrics st ON s.sa2_code21 = st.sa2_code21
),

z_scores AS (
    SELECT
        sa2_code21,
        business_count,
        poi_count,
        school_count,
        stop_count,
        (business_count - AVG(business_count) OVER()) / NULLIF(STDDEV(business_count) OVER(), 0) AS zbusiness,
        (poi_count - AVG(poi_count) OVER()) / NULLIF(STDDEV(poi_count) OVER(), 0) AS zpoi,
        (school_count - AVG(school_count) OVER()) / NULLIF(STDDEV(school_count) OVER(), 0) AS zschools,
        (stop_count - AVG(stop_count) OVER()) / NULLIF(STDDEV(stop_count) OVER(), 0) AS zstops
    FROM combined
)

SELECT
    *,
    ROUND(1 / (1 + EXP(-(zbusiness + zpoi + zschools + zstops))), 4) AS final_score
FROM z_scores
ORDER BY final_score DESC;
