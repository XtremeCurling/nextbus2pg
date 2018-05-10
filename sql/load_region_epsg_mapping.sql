/*
Create region_epsg_mapping table.
*/
CREATE TABLE IF NOT EXISTS nextbus.region_epsg_mapping (
    region TEXT,
    epsg   INTEGER,
    units  TEXT
);

\COPY nextbus.region_epsg_mapping FROM '../data/region_epsg.csv' WITH DELIMITER ',' CSV HEADER;

