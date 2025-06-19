-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022






SELECT * 
FROM world_layoffs.layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs; # this command just create empty table structure like 'layoffs' table

select * from layoffs_staging;  # empty table with structure like 'layoffs' table

INSERT INTO layoffs_staging  # INTO clause here is optional
SELECT * FROM world_layoffs.layoffs;
# after INSERT INTO, the layoffs_staging table is populated

drop table layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM world_layoffs.layoffs_staging
;

-- these are our REAL DUPLICATES 😃 
SELECT *
FROM (
	# SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
    select *,  # instead of write all columns name, best just use "*"
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

/*
-- now you may want to write it like this:

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;
*/

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);

select * from layoffs_staging2;

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
-- Until this step, table layoffs_staging2 already have same entries as layoffs_staging.

-- Check that all duplicate entries are already in new table (layoffs_staging2)
select * 
from layoffs_staging2
where row_num > 1; 

-- now that we have this we can delete rows were row_num is greater than 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2; # until this step, there's no more duplicate entries in layoffs_staging2 table

-- So, we can drop the column 'row_num'
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- 2. Standardize Data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT company, TRIM(company)
FROM world_layoffs.layoffs_staging2
where company <> trim(company)
#ORDER BY industry
; # there are 10 rows here that have leading and/or trailing whitespace

-- Get rid of all leading and/or trailing whitespace
update layoffs_staging2
set company = trim(company);

-- Check the possibility of NULL value in layoffs_staging2 table
describe layoffs_staging2;

-- Check whether there is NULL value of company
select *
from layoffs_staging2
where company is null or company = ''
;

select *
from layoffs_staging2
where location is null or location = ''
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-- alternatively
select distinct industry, company
from layoffs_staging2
order by 1;  # turns out, there are NULL values and non-standardized name

select *
from layoffs_staging2
where total_laid_off is null OR total_laid_off = ''
;

select *
from layoffs_staging2
where percentage_laid_off is null OR percentage_laid_off = ''
;

select *
from layoffs_staging2
where date is null OR date = ''
;

select *
from layoffs_staging2
where stage is null OR stage = ''
;

select distinct country
from layoffs_staging2
where country is null OR country = ''
;

select *
from layoffs_staging2
where funds_raised_millions is null OR funds_raised_millions = ''
;

#### I'm here ...........

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

select industry, company
from layoffs_staging2
where company like 'bally%' ; 
#and (not industry is null OR industry = '')
;
-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Get the non-NULL/Blank value of 'industry' for Airbnb%
select distinct industry
from layoffs_staging2
where company like 'airbnb%' and trim(industry) <> ''
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'juul%';

-- Get the non-NULL/Blank value of 'industry' for Juul%
select distinct industry
from layoffs_staging2
where company like 'juul%' and trim(industry) <> ''
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'carvana%';

-- Get the non-NULL/Blank value of 'industry' for Carvana%
select distinct industry, company
from layoffs_staging2
where company like 'carvana%' and trim(industry) <> ''
;

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- first, we have to backup the layoffs_staging2 table
create table layoffs_staging2a 
as select * from layoffs_staging2;

select * from layoffs_staging2a;

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null
;

select *
from layoffs_staging2
where industry is null; # should be there are 4 rows that have NULL value for 'industry' column

# There are blanks for 'industry' value for company Airbnb, Juul and Carvana
select *
from layoffs_staging
where industry = ''; 

-- now we need to populate those nulls if possible (confirmed it works)
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Remove all dot trailing in all countries with TRAILING FROM clause
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Change all the 'United States.' value to 'United States'
update layoffs_staging2
set country = 'United States'
where country = 'United States.'
;

-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

# STR_TO_DATE function is used to convert text into DATE format
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

describe layoffs_staging2; # the 'date' column is still TEXT format

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

-- until this point, we already get the properly formatted date but still in TEXT format
-- if we want to use DATE format-related functions, we have to convert the 'date' column
-- into DATE type.
-- now we can convert the data type properly to DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
-- 
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2
where row_num > 1;  

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;


































