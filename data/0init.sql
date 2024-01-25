CREATE EXTENSION postgis;
-- enabling raster support
CREATE EXTENSION postgis_raster;

-- enabling advanced 3d support
CREATE EXTENSION postgis_sfcgal;
-- enabling SQL/MM Net Topology
CREATE EXTENSION postgis_topology;

-- using US census data for geocoding and standardization
CREATE EXTENSION address_standardizer;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
SELECT postgis_full_version();