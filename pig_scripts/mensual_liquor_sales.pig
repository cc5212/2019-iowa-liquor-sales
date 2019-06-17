-- Group 6: Jean Cherubini, Anibal Fuentes, Juan Saez, Claudio Urbina
-- Pig script to compute the amount of alcohol sold in Iowa monthly in number of transactions, liters and money.


-- Load iowa liquor sales dataset
liquor_sales_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/Iowa_Liquor_Sales_10k.csv' USING PigStorage(';') AS (Invoice, Date:chararray, StoreNumber, StoreName, Address, City, ZipCode, StoreLocation, CountyNumber, County, Category, CategoryName, VendorNumber, VendorName, ItemNumber, ItemDescription, Pack, BottleVolumeML, StateBottleCost, StateBottleRetail, BottlesSold:int, SaleDollars:chararray, VolumeSoldLiters:float, VolumeSoldGallons);
liquor_sales_dataset = FILTER liquor_sales_dataset BY Date != 'Date';

-- Select useful columns
selectColumns = FOREACH liquor_sales_dataset GENERATE Date, SaleDollars, VolumeSoldLiters, BottlesSold;

-- Parse date
dateParsed = FOREACH selectColumns GENERATE FLATTEN(STRSPLIT(Date,'/',3)) as (month:int, day:int, year:int), SaleDollars, VolumeSoldLiters, BottlesSold;

-- Parse money and select only month and year from the date
moneyParsed = FOREACH dateParsed GENERATE month, year, FLATTEN(STRSPLIT(SaleDollars,'\\u0024', 2)) as (nothing:chararray, dollars:chararray), VolumeSoldLiters, BottlesSold;
moneyParsed2 = FOREACH moneyParsed GENERATE month, year, (float)dollars, VolumeSoldLiters, BottlesSold;

-- Group by year and month
yearMonthGrouped = GROUP moneyParsed2 BY (year, month);

-- Get number of transactions, sum of sales (dollars), volume sold (liters) and bottles sold
yearMonthGrouped_quantity = FOREACH yearMonthGrouped GENERATE FLATTEN(group) as (year, month), COUNT($1) as numberOfTransactions, SUM($1.BottlesSold) as TotalBottles, SUM($1.VolumeSoldLiters) as TotalVolume, SUM($1.dollars) as TotalSales;