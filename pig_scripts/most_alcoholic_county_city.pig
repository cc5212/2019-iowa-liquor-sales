-- Group 6: Jean Cherubini, Anibal Fuentes, Juan Saez, Claudio Urbina
-- Pig script to compute most alcoholic counties and cities each year


-- Load iowa liquor sales dataset
liquor_sales_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/Iowa_Liquor_Sales_10k.csv' USING PigStorage(';') AS (Invoice, Date, StoreNumber, StoreName, Address, City, ZipCode, StoreLocation, CountyNumber, County, Category, CategoryName, VendorNumber, VendorName, ItemNumber, ItemDescription, Pack, BottleVolumeML, StateBottleCost, StateBottleRetail, BottlesSold, SaleDollars, VolumeSoldLiters:float, VolumeSoldGallons);
liquor_sales_dataset = FILTER liquor_sales_dataset BY Date != 'Date';

-- Load county population dataset
county_population_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/county_population_by_year.csv' USING PigStorage(';') AS (County, year:int, Population:int, Coordinates);
county_population_dataset = FILTER county_population_dataset BY County != 'County';


-- Load city population dataset
city_population_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/city_population_by_year.csv' USING PigStorage(';') AS (Coordinates, County, City, year:int, Population:int);
city_population_dataset = FILTER city_population_dataset BY County != 'County';


-- Select liquor sales dataset columns and parse date
liquor_sales_selected_columns = FOREACH liquor_sales_dataset GENERATE Date, City, County, VolumeSoldLiters;
liquor_sales_dateParsed = FOREACH liquor_sales_selected_columns GENERATE FLATTEN(STRSPLIT(Date,'/',3)) as (month:int, day:int, (int)year:int), City, County, VolumeSoldLiters;


-- Compute most alcoholic counties
-- Group by year, county
liquor_sales_dateParsed_filtered = FILTER liquor_sales_dateParsed BY County != '';
liquor_sales_year_county = GROUP liquor_sales_dateParsed_filtered BY (year,County);

-- Get sum VolumeSoldLiters
county_volumeSold = FOREACH liquor_sales_year_county GENERATE FLATTEN(group) as (year,County), SUM($1.VolumeSoldLiters) as totalVolume;

-- Join dataset with county population
county_volumeSold_population = JOIN county_volumeSold BY (year,County), county_population_dataset by (year,County);

-- Divide volume sold with county population
county_volume_per_capita = FOREACH county_volumeSold_population GENERATE county_volumeSold::year as year, county_volumeSold::County as County, county_volumeSold::totalVolume as totalVolume, county_population_dataset::Population as Population, (county_volumeSold::totalVolume/county_population_dataset::Population) as volumePerCapita, county_population_dataset::Coordinates as coordinates;

dump county_volumeSold_population;
