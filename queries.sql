--Priya Chaganti analytics code

use psc358;

--create external tables
create external table deaths_extra (extra STRING, entity STRING, year INT, household_deaths DOUBLE, outdoor_deaths DOUBLE, all_deaths DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/psc358/final_drop/cleaned_death_rate.csv' overwrite into table deaths_extra;

create table deaths AS SELECT entity, year, household_deaths, outdoor_deaths, all_deaths FROM deaths_extra; 

create external table air_quality_extra (extra STRING, year INT, country STRING, sdi STRING, pollutant STRING, exposure_lower DOUBLE, exposure_upper DOUBLE, exposure_mean DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath 'hdfs://horton.hpc.nyu.edu:8020/user/psc358/final_drop/cleaned_air_quality.csv' overwrite into table air_quality_extra;

create table air_quality AS SELECT year, country, sdi, pollutant, exposure_lower, exposure_upper, exposure_mean FROM air_quality_extra;

--create a new table joining pollutant data with deaths data
create table if not exists joined_outdoor as SELECT d.entity, d.year, aq.pollutant, aq.exposure_mean, d.outdoor_deaths FROM deaths d JOIN air_quality aq ON (d.entity = aq.country AND d.year = aq.year) WHERE (aq.pollutant = "pm25" OR aq.pollutant = "ozone");

--ANALYTIC: compare outdoor air pollution deaths across countries in different SDI categories

--query US
create table us_outdoor_deaths AS SELECT (entity, year, outdoor_deaths, FROM deaths WHERE entity = "United States") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
--query remaining countries and save them to hdfs and local to create graphs
insert overwrite directory '/user/psc358/final_drop/us' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE entity = 'United States' GROUP BY entity, year, outdoor_deaths;
--(in peel) hdfs dfs -cat final_drop/us/000000_0 > us_outdoor.csv
--(in peel) scp psc358@peel.hpc.nyu.edu:/home/psc358/final_code_drop/us_outdoor.csv .

insert overwrite directory '/user/psc358/final_drop/india' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE entity = 'India' GROUP BY entity, year, outdoor_deaths;
--(in peel) hdfs dfs -cat final_drop/india/000000_0 > india_outdoor.csv
--(in peel) scp psc358@peel.hpc.nyu.edu:/home/psc358/final_code_drop/india_outdoor.csv .

insert overwrite directory '/user/psc358/final_drop/japan' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE entity = 'Japan' GROUP BY entity, year, outdoor_deaths;
--(in peel) hdfs dfs -cat final_drop/japan/000000_0 > japan_outdoor.csv
--(in peel) scp psc358@peel.hpc.nyu.edu:/home/psc358/final_code_drop/japan_outdoor.csv .

insert overwrite directory '/user/psc358/final_drop/canada' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE entity = 'Canada' GROUP BY entity, year, outdoor_deaths;
--(in peel) hdfs dfs -cat final_drop/canada/000000_0 > canada_outdoor.csv
--(in peel) scp psc358@peel.hpc.nyu.edu:/home/psc358/final_code_drop/canada_outdoor.csv .

insert overwrite directory '/user/psc358/final_drop/zimbabwe' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE entity = 'Zimbabwe' GROUP BY entity, year, outdoor_deaths;
--(in peel) hdfs dfs -cat final_drop/zimbabwe/000000_0 > zimbabwe_outdoor.csv
--(in peel) scp psc358@peel.hpc.nyu.edu:/home/psc358/final_code_drop/zimbabwe_outdoor.csv .

--compare outdoor pollutants and outdoor air pollution deaths in 1990 and 2017 
--I will use this info to calculate the percentage change 
SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE year = 1990 AND (entity = 'India' OR entity = 'Canada' OR entity = 'Japan' OR entity = 'United States' OR entity = 'Zimbabwe') GROUP BY entity, year, outdoor_deaths;

SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor WHERE year = 2017 AND (entity = 'India' OR entity = 'Canada' OR entity = 'Japan' OR entity = 'United States' OR entity = 'Zimbabwe') GROUP BY entity, year, outdoor_deaths;
 
--create a new table with the combined outdoor exposure means (ozone + pm25) 
create table if not exists total_outdoor as SELECT entity, year, outdoor_deaths, SUM(exposure_mean) outdoor_exposure_sum FROM joined_outdoor GROUP BY entity, year, outdoor_deaths;

--what is the percentage difference of outdoor air pollution and deaths for each country between 1990 and 2017?
create table if not exists intermediate_percent_change as SELECT entity, year, outdoor_exposure_sum, outdoor_deaths, LEAD (outdoor_exposure_sum) OVER (PARTITION BY entity ORDER BY year) AS exposuremean_2017, LEAD (outdoor_deaths) OVER (PARTITION BY entity ORDER BY year) AS outdoor_deaths_2017 FROM total_outdoor WHERE ((entity = 'United States' OR entity = 'Canada' OR entity = 'Japan' OR entity = 'India' OR entity = 'Zimbabwe') AND (year = 1990 OR year = 2017));

SELECT entity, outdoor_exposure_sum as exposuremean_1990, exposuremean_2017, (((exposuremean_2017 - outdoor_exposure_sum) / outdoor_exposure_sum) * 100) as percent_change_outdoor_exposure, outdoor_deaths as outdoor_deaths_1990, outdoor_deaths_2017, (((outdoor_deaths_2017 - outdoor_deaths) / outdoor_deaths) * 100) as percent_change_outdoor_deaths FROM intermediate_percent_change WHERE ((entity = 'United States' OR entity = 'Canada' OR entity = 'Japan' OR entity = 'India' OR entity = 'Zimbabwe') AND (year = 1990));
