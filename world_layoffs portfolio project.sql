-- Data Cleaning


SELECT *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove any Columns




CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
from layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
row_number() over(
partition by company,total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging;

with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company,location,total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;


SELECT *
from layoffs_staging
WHERE company='Casper';


with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company,location,total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)
DELETE 
FROM duplicate_cte 
WHERE row_num > 1;








CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
row_number() over(
partition by company,location,total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging;

DELETE  
FROM  layoffs_staging2
WHERE row_num > 1;

select * 
FROM  layoffs_staging2;


-- Standardizing Data

select company, TRIM(company)
FROM  layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

select distinct industry 
FROM  layoffs_staging2
ORDER BY 1;

select distinct industry 
FROM  layoffs_staging2;

UPDATE layoffs_staging2
set industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


select distinct country, trim(TRAILING '.' from country) 
FROM  layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
set country = trim(TRAILING '.' from country)
WHERE country LIKE 'United States%';

select `date`

FROM  layoffs_staging2;

UPDATE layoffs_staging2
set `date`= str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;

select * 
FROM  layoffs_staging2
WHERE total_laid_off IS null
and percentage_laid_off is null;

select *
FROM  layoffs_staging2
WHERE industry is null 
or industry = '';

select * 
FROM  layoffs_staging2
WHERE company = 'Airbnb';

select * 
FROM  layoffs_staging2 t1
JOIN layoffs_staging2  t2
     on t1.company= t2.company
WHERE (t1.industry is null or t1.industry = '')
and t2.industry is not null;

UPDATE layoffs_staging2
SET industry = null
	WHERE industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2 
	ON t1.company=t2.company
    SET t1.industry=t2.industry
    WHERE  t1.industry is null
    and t2.industry is not null;
    
    
    alter table  layoffs_staging2
    drop column row_num;



-- Exploratory Data Analysis

select *
FROM  layoffs_staging2;
    
select  max(total_laid_off), max(percentage_laid_off)
FROM  layoffs_staging2;
    

select *
FROM  layoffs_staging2
WHERE percentage_laid_off=1
order by funds_raised_millions desc;

select company, SUM(total_laid_off)
FROM  layoffs_staging2
group by company
order by 2 desc;

select company, industry, country,SUM(total_laid_off)
FROM  layoffs_staging2
group by company, industry, country
order by 3;


select min(`date`) , max(`date`)
FROM  layoffs_staging2;

select stage, SUM(total_laid_off)
FROM  layoffs_staging2
group by stage
order by 1 desc;

select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
FROM  layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL 
GROUP BY `MONTH`
ORDER BY 1 ASC;

with rolling_total AS
(
select substring(`date`,1,7) as `MONTH`, sum(total_laid_off) as total_off
FROM  layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL 
GROUP BY `MONTH`
ORDER BY 1 ASC
)
select `MONTH`, total_off,sum(total_off) OVER(order by `MONTH`) as rolling_total
From Rolling_Total; 

select company, year(`date`) ,SUM(total_laid_off)
FROM  layoffs_staging2
group by company,  year(`date`)
order by company asc;

select *
from layoffs_staging2
WHERE country like 'united states'



    





 
