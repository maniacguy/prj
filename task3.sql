-- ✅ Tạo hàm sigmoid
CREATE OR REPLACE FUNCTION sigmoid(x DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  RETURN 1 / (1 + EXP(-x));
END;
$$ LANGUAGE plpgsql;

-- === TÍNH CHỈ SỐ ===

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

-- ✅ Lọc bảng dân số, chuẩn hóa mã
population_filtered AS (
    SELECT
        sa2_code21,
        total_population AS population,
        population_0_19
    FROM populations
    WHERE total_population >= 100
),

-- ✅ Tổng hợp dữ liệu
combined AS (
    SELECT
        s.sa2_code21,
        COALESCE(b.business_count, 0) AS business_count,
        COALESCE(p.poi_count, 0) AS poi_count,
        COALESCE(sc.school_count, 0) AS school_count,
        COALESCE(st.stop_count, 0) AS stop_count,
        COALESCE(pop.population, 0) AS population,
        COALESCE(pop.population_0_19, 0) AS population_0_19,

        -- ✅ Chia theo dân số/người trẻ
        CASE 
            WHEN COALESCE(pop.population, 0) > 0 THEN (COALESCE(b.business_count, 0) * 1000.0) / pop.population
            ELSE 0
        END AS business_per_1000,
        
        CASE 
            WHEN COALESCE(pop.population_0_19, 0) > 0 THEN (COALESCE(sc.school_count, 0) * 1000.0) / pop.population_0_19
            ELSE 0
        END AS schools_per_1000young
    FROM sa2 s
    LEFT JOIN population_filtered pop ON s.sa2_code21 = pop.sa2_code21
    LEFT JOIN business_metrics b ON s.sa2_code21 = b.sa2_code21
    LEFT JOIN poi_metrics p ON s.sa2_code21 = p.sa2_code21
    LEFT JOIN school_metrics sc ON s.sa2_code21 = sc.sa2_code21
    LEFT JOIN stop_metrics st ON s.sa2_code21 = st.sa2_code21
),

-- Calculate averages and standard deviations first
stats AS (
    SELECT
        AVG(business_per_1000) AS avg_business_per_1000,
        STDDEV_SAMP(business_per_1000) AS stddev_business_per_1000,
        AVG(poi_count) AS avg_poi_count,
        STDDEV_SAMP(poi_count) AS stddev_poi_count,
        AVG(schools_per_1000young) AS avg_schools_per_1000young,
        STDDEV_SAMP(schools_per_1000young) AS stddev_schools_per_1000young,
        AVG(stop_count) AS avg_stop_count,
        STDDEV_SAMP(stop_count) AS stddev_stop_count
    FROM combined
    WHERE population > 0  -- Consider only areas with population
),

-- ✅ Tính z-score với xử lý NULL
z_scores AS (
    SELECT
        c.sa2_code21,
        c.business_per_1000,
        c.poi_count,
        c.schools_per_1000young,
        c.stop_count,

        -- Handle possible division by zero or NULL values
        CASE
            WHEN s.stddev_business_per_1000 IS NULL OR s.stddev_business_per_1000 = 0 THEN 0
            ELSE (c.business_per_1000 - s.avg_business_per_1000) / s.stddev_business_per_1000
        END AS zbusiness,
        
        CASE
            WHEN s.stddev_poi_count IS NULL OR s.stddev_poi_count = 0 THEN 0
            ELSE (c.poi_count - s.avg_poi_count) / s.stddev_poi_count
        END AS zpoi,
        
        CASE
            WHEN s.stddev_schools_per_1000young IS NULL OR s.stddev_schools_per_1000young = 0 THEN 0
            ELSE (c.schools_per_1000young - s.avg_schools_per_1000young) / s.stddev_schools_per_1000young
        END AS zschools,
        
        CASE
            WHEN s.stddev_stop_count IS NULL OR s.stddev_stop_count = 0 THEN 0
            ELSE (c.stop_count - s.avg_stop_count) / s.stddev_stop_count
        END AS zstops
    FROM combined c
    CROSS JOIN stats s
    WHERE c.population > 0  -- Only include populated areas
)

-- ✅ Tính điểm tổng
SELECT
    sa2_code21,
    ROUND(zbusiness::NUMERIC, 2) AS zbusiness,
    ROUND(zpoi::NUMERIC, 2) AS zpoi,
    ROUND(zschools::NUMERIC, 2) AS zschools,
    ROUND(zstops::NUMERIC, 2) AS zstops,
    ROUND(sigmoid(zbusiness + zpoi + zschools + zstops)::NUMERIC, 4) AS final_score
FROM z_scores
ORDER BY final_score DESC;
