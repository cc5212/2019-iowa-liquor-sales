-- Group 6: Jean Cherubini, Anibal Fuentes, Juan Saez, Claudio Urbina
-- Pig script to compute top 10 liquor categories and vendors by number of sells 


-- Load iowa liquor sales dataset
liquor_sales_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/Iowa_Liquor_Sales_10k.csv' USING PigStorage(';') AS (Invoice, Date, StoreNumber, StoreName, Address, City, ZipCode, StoreLocation, CountyNumber, County, Category, CategoryName, VendorNumber, VendorName, ItemNumber, ItemDescription, Pack, BottleVolumeML, StateBottleCost, StateBottleRetail, BottlesSold, SaleDollars, VolumeSoldLiters, VolumeSoldGallons);
liquor_sales_dataset = FILTER liquor_sales_dataset BY Date != 'Date';


-- Compute top 10 liquor categories (by number of sells)
-- Group by liquor category and count
category_grouped = GROUP liquor_sales_dataset BY CategoryName;
by_category_count = FOREACH category_grouped GENERATE FLATTEN(group) as (CategoryName), COUNT($1) as categorySales;

-- Sort categories
by_category_count_sorted = ORDER by_category_count BY categorySales DESC;
top_10_categories = LIMIT by_category_count_sorted 10;


-- Compute top 10 vendors (by number of sells)
-- Group by vendor and count
vendor_grouped = GROUP liquor_sales_dataset BY VendorName;
by_vendor_count = FOREACH vendor_grouped GENERATE FLATTEN(group) as (VendorName), COUNT($1) as vendorSales;

-- Sort categories
by_vendor_count_sorted = ORDER by_vendor_count BY vendorSales DESC;
top_10_vendors = LIMIT by_vendor_count_sorted 10;