-- Group 6: Jean Cherubini, Anibal Fuentes, Juan Saez, Claudio Urbina

-- Load iowa liquor sales dataset
liquor_sales_dataset = LOAD 'hdfs://cm:9000/uhadoop2019/grupo6/data/Iowa_Liquor_Sales_10k.csv' USING PigStorage(';') AS (Invoice, Date, StoreNumber, StoreName, Address, City, ZipCode, StoreLocation, CountyNumber, County, Category, CategoryName, VendorNumber, VendorName, ItemNumber, ItemDescription, Pack, BottleVolumeML, StateBottleCost, StateBottleRetail, BottlesSold, SaleDollars, VolumeSoldLiters, VolumeSoldGallons);