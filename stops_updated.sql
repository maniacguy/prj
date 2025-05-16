SELECT * FROM stops LIMIT 1;
ALTER TABLE stops ADD COLUMN geometry GEOMETRY(Point, 4326);
UPDATE stops
SET geometry = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326);



