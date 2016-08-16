DROP TABLE IF EXISTS elevation_by_country
CREATE TABLE elevation_by_country (
	country         varchar(40),
	elevation       integer );

COPY elevation_by_country FROM '/Users/xuanthu/Google\ Drive/MIDS/W205/w205\ final\ project/elevation_by_country.txt' DELIMITER ',' CSV;
