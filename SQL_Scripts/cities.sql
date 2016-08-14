--SQL Reference for copying in Cities data to new tables in Postgres instance
--psql
--psql -h midsdb.c82uau1daq0n.us-east-1.rds.amazonaws.com -p 5432 -d kaggle_airbnb -U master


CREATE TABLE age_gender_bkts (
  age_bucket varchar(25),
  country_destination char(2),
  gender varchar(10),
  population_in_thousands int,
  year int
);
\copy age_gender_bkts FROM 'C:\Users\Chris\OneDrive\Documents\MIDS\WS207\Projects\Final\data\age_gender_bkts.csv\age_gender_bkts.csv' WITH DELIMITER ',';
select * from age_gender_bkts limit 10;

CREATE TABLE countries (
  country_destination char(2),
  lat_destination float(6),
  lng_destination float(6),
  distance_km float(4),
  distance_km2 int,
  destination_language varchar(25),
  language_levenshtein_distance float(2)
);
\copy countries FROM 'C:\Users\Chris\OneDrive\Documents\MIDS\WS207\Projects\Final\data\countries.csv\countries.csv' WITH DELIMITER ',';
select * from countries limit 10;

CREATE TABLE countries (
  country_destination char(2),
  lat_destination float(6)
);
\copy countries FROM 'C:\Users\Chris\OneDrive\Documents\MIDS\WS207\Projects\Final\data\countries.csv\countries.csv' WITH DELIMITER ',';
select * from countries limit 10;



