-- Group 6: Jean Cherubini, Anibal Fuentes, Juan Saez, Claudio Urbina
-- Pig script to compute most alcoholic counties and cities each year


-- Load iowa liquor sales dataset
liquor_sales_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/Iowa_Liquor_Sales_10k.csv' USING PigStorage(';') AS (Invoice, Date, StoreNumber, StoreName, Address, City, ZipCode, StoreLocation, CountyNumber, County, Category, CategoryName, VendorNumber, VendorName, ItemNumber, ItemDescription, Pack, BottleVolumeML, StateBottleCost, StateBottleRetail, BottlesSold, SaleDollars, VolumeSoldLiters:float, VolumeSoldGallons);
liquor_sales_dataset = FILTER liquor_sales_dataset BY Date != 'Date';

-- Load county population dataset
county_population_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/county_population_by_year.csv' USING PigStorage(';') AS (County, year:bytearray, Population:int, Coordinates);
county_population_dataset = FILTER county_population_dataset BY County != 'County';


-- Load city population dataset
city_population_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/city_population_by_year.csv' USING PigStorage(';') AS (Coordinates, County, City, year, Population:int);
city_population_dataset = FILTER city_population_dataset BY County != 'County';


-- Select liquor sales dataset columns and parse date
liquor_sales_selected_columns = FOREACH liquor_sales_dataset GENERATE Date, City, County, VolumeSoldLiters;
liquor_sales_dateParsed = FOREACH liquor_sales_selected_columns GENERATE FLATTEN(STRSPLIT(Date,'/',3)) as (month, day, year), City, County, VolumeSoldLiters;


-- Compute most alcoholic counties
-- Group by year, county
liquor_sales_dateParsed_filtered = FILTER liquor_sales_dateParsed BY County != '';
liquor_sales_year_county = GROUP liquor_sales_dateParsed_filtered BY (year,County);

-- Get sum VolumeSoldLiters
county_volumeSold = FOREACH liquor_sales_year_county GENERATE FLATTEN(group) as (year,County), SUM($1.VolumeSoldLiters) as totalVolume;

-- Join dataset with county population
county_volumeSold_population = JOIN county_volumeSold BY County LEFT OUTER, county_population_dataset BY County;
county_volumeSold_population_correct = FILTER county_volumeSold_population BY (county_volumeSold::year matches county_population_dataset::year);

-- County alcohol volume per capita
county_volume_per_capita = FOREACH county_volumeSold_population_correct GENERATE county_volumeSold::year as year, county_volumeSold::County as County, county_volumeSold::totalVolume as totalVolume, county_population_dataset::Population as Population, (county_volumeSold::totalVolume/county_population_dataset::Population) as volumePerCapita, county_population_dataset::Coordinates as coordinates;

-- group by year
county_volume_per_capita_gb_year = GROUP county_volume_per_capita BY (year);

-- sort by volumePerCapita and get the most alcoholic county
most_alcoholic_county_by_year = FOREACH county_volume_per_capita_gb_year {sortByMax = ORDER county_volume_per_capita BY volumePerCapita DESC; topMax = LIMIT sortByMax 1; GENERATE FLATTEN(topMax);}


-- Compute most alcoholic cities
-- Group by year, city
liquor_sales_dateParsed_filtered_city = FILTER liquor_sales_dateParsed BY City != '';
liquor_sales_year_city = GROUP liquor_sales_dateParsed_filtered_city BY (year,County,City);

-- Get sum VolumeSoldLiters
city_volumeSold = FOREACH liquor_sales_year_city GENERATE FLATTEN(group) as (year,County,City), SUM($1.VolumeSoldLiters) as totalVolume;

-- Join dataset with county population
city_volumeSold_population = JOIN city_volumeSold BY (County,City) LEFT OUTER, city_population_dataset BY (County,City);
city_volumeSold_population_correct = FILTER city_volumeSold_population BY (city_volumeSold::year matches city_population_dataset::year);

-- County alcohol volume per capita
city_volume_per_capita = FOREACH city_volumeSold_population_correct GENERATE city_volumeSold::year as year, city_volumeSold::County as County, city_volumeSold::City as City, city_volumeSold::totalVolume as totalVolume, city_population_dataset::Population as Population, (city_volumeSold::totalVolume/city_population_dataset::Population) as volumePerCapita, city_population_dataset::Coordinates as coordinates;

-- group by year
city_volume_per_capita_gb_year = GROUP city_volume_per_capita BY (year);

-- sort by volumePerCapita and get the most alcoholic county
most_alcoholic_city_by_year = FOREACH city_volume_per_capita_gb_year {sortByMax = ORDER city_volume_per_capita BY volumePerCapita DESC; topMax = LIMIT sortByMax 1; GENERATE FLATTEN(topMax);}
