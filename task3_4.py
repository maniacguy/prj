import pandas as pd
import numpy as np
from scipy.special import expit as sigmoid
import geopandas as gpd
from shapely import wkt

# Hàm tính z-score
def z_score(value, mean, std):
    if std == 0:
        return 0
    return (value - mean) / std

# Hàm tính điểm S từ z-score
def calculate_score(z):
    return sigmoid(z)

# Đọc dữ liệu từ các file CSV
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f'✅ Đọc file {file_path} thành công!')
        print(f'📂 Các cột trong file {file_path}: {df.columns.tolist()}')
        return df
    except Exception as e:
        print(f'❌ Lỗi khi đọc file {file_path}: {e}')
        return None

# Hàm gán sa2_code cho dataframe có cột tọa độ lat/lon
def add_sa2_code_from_coords(df, lat_col, lon_col, gdf_sa2):
    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=gdf_sa2.crs)
    gdf_joined = gpd.sjoin(gdf_points, gdf_sa2, how='left', predicate='within')
    return pd.DataFrame(gdf_joined.drop(columns=['geometry', 'index_right']))

# Hàm gán sa2_code cho dataframe có cột geometry dạng WKT
def add_sa2_code_from_wkt(df, wkt_col, gdf_sa2):
    df['geometry'] = df[wkt_col].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=gdf_sa2.crs)
    gdf_joined = gpd.sjoin(gdf, gdf_sa2, how='left', predicate='within')
    return pd.DataFrame(gdf_joined.drop(columns=['geometry', 'index_right']))

# Tính toán điểm "well-resourced" cho từng vùng SA2
def calculate_well_resourced_score(df_business, df_population, df_stops, df_schools, df_poi):
    result = []
    sa2_codes = pd.concat([df_business['sa2_code'], df_population['sa2_code']]).drop_duplicates().tolist()

    # Tính các thống kê để tính z-score
    business_per_1000_list = []
    stops_count_list = []
    school_per_1000_young_list = []
    poi_count_list = []

    # Tính trước cho từng SA2 các chỉ số
    for sa2 in sa2_codes:
        try:
            pop = df_population[df_population['sa2_code'] == sa2]['total_people'].values
            if len(pop) == 0 or pop[0] < 100:
                business_per_1000_list.append(np.nan)
                stops_count_list.append(np.nan)
                school_per_1000_young_list.append(np.nan)
                poi_count_list.append(np.nan)
                continue

            businesses = df_business[df_business['sa2_code'] == sa2]['total_businesses'].sum()
            business_per_1000 = businesses / (pop[0] / 1000)

            stops_count = df_stops[df_stops['sa2_code'] == sa2].shape[0]

            young_pop = df_population[df_population['sa2_code'] == sa2][['0-4_people', '5-9_people', '10-14_people', '15-19_people']].sum(axis=1).values
            young_pop = young_pop[0] if len(young_pop) > 0 else 0
            if young_pop == 0:
                school_per_1000_young = 0
            else:
                school_per_1000_young = df_schools[df_schools['sa2_code'] == sa2].shape[0] / (young_pop / 1000)

            poi_count = df_poi[df_poi['sa2_code'] == sa2].shape[0]

            business_per_1000_list.append(business_per_1000)
            stops_count_list.append(stops_count)
            school_per_1000_young_list.append(school_per_1000_young)
            poi_count_list.append(poi_count)
        except Exception as e:
            print(f'⚠️ Lỗi khi xử lý SA2 {sa2}: {e}')
            business_per_1000_list.append(np.nan)
            stops_count_list.append(np.nan)
            school_per_1000_young_list.append(np.nan)
            poi_count_list.append(np.nan)

    # Chuyển thành numpy array để tính mean/std, bỏ nan
    business_arr = np.array(business_per_1000_list, dtype=np.float64)
    stops_arr = np.array(stops_count_list, dtype=np.float64)
    school_arr = np.array(school_per_1000_young_list, dtype=np.float64)
    poi_arr = np.array(poi_count_list, dtype=np.float64)

    mean_business, std_business = np.nanmean(business_arr), np.nanstd(business_arr)
    mean_stops, std_stops = np.nanmean(stops_arr), np.nanstd(stops_arr)
    mean_school, std_school = np.nanmean(school_arr), np.nanstd(school_arr)
    mean_poi, std_poi = np.nanmean(poi_arr), np.nanstd(poi_arr)

    for i, sa2 in enumerate(sa2_codes):
        try:
            if np.isnan(business_arr[i]) or np.isnan(stops_arr[i]) or np.isnan(school_arr[i]) or np.isnan(poi_arr[i]):
                result.append((sa2, None))
                continue

            zbusiness = z_score(business_arr[i], mean_business, std_business)
            zstops = z_score(stops_arr[i], mean_stops, std_stops)
            zschools = z_score(school_arr[i], mean_school, std_school)
            zpoi = z_score(poi_arr[i], mean_poi, std_poi)

            score = calculate_score(zbusiness + zstops + zschools + zpoi)
            result.append((sa2, score))
        except Exception as e:
            print(f'⚠️ Lỗi khi tính điểm cho SA2 {sa2}: {e}')
            result.append((sa2, None))

    result_df = pd.DataFrame(result, columns=['sa2_code', 'score'])
    result_df.to_csv('well_resourced_scores.csv', index=False)
    print('✅ Đã lưu kết quả vào file well_resourced_scores.csv')
    return result_df


if __name__ == '__main__':
    # Đọc shapefile vùng SA2
    gdf_sa2 = gpd.read_file(r'data\SA2_2021_AUST_SHP_GDA2020\SA2_2021_AUST_GDA2020.shp', engine='pyogrio')
    gdf_sa2 = gdf_sa2[['SA2_CODE21', 'geometry']].rename(columns={'SA2_CODE21': 'sa2_code'})

    # Đọc dữ liệu
    df_business = read_csv('data/Businesses (1).csv')
    df_population = read_csv('data/Population (1).csv')
    df_stops = read_csv('data/Stops.txt')
    df_schools = read_csv('data/schools_combined.csv')
    df_poi = read_csv('data/points_of_interest.csv')

    # Gán sa2_code cho df_stops
    df_stops = add_sa2_code_from_coords(df_stops, 'stop_lat', 'stop_lon', gdf_sa2)

    # Gán sa2_code cho df_schools (dựa trên cột 'geometry' dạng WKT)
    df_schools = add_sa2_code_from_wkt(df_schools, 'geometry', gdf_sa2)

    # Gán sa2_code cho df_poi (dựa trên cột 'shape_wkt')
    df_poi = add_sa2_code_from_wkt(df_poi, 'shape_wkt', gdf_sa2)

    # Kiểm tra đủ dữ liệu rồi tính điểm
    if all(df is not None for df in [df_business, df_population, df_stops, df_schools, df_poi]):
        calculate_well_resourced_score(df_business, df_population, df_stops, df_schools, df_poi)
