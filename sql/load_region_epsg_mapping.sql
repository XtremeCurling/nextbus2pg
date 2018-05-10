/*
Create region_epsg_mapping table.
*/
CREATE TABLE IF NOT EXISTS nextbus.region_epsg_mapping (
    region TEXT,
    epsg   INTEGER,
    units  TEXT
);

