--- CHECKING bronze.crm_cust_info

-- CHeck for NULLS or Duplicates in Primary key
-- Expectation: Nothing

--Step 1
select cst_id, count(*)
FROM bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

-- Resultset = 6

--Step 2
-- Check one as a sample
select * from bronze.crm_cust_info where cst_id = 29466;
-- The dates are different so we will use the latest one by ranking them via a window function.

--Step 3
with cte_cust_info as
(
select * ,
ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
from bronze.crm_cust_info
where cst_id IS NOT NULL
) -- Exceute a CTE to return the data with duplicate Values in a ranked format
select * from cte_cust_info
where flag_last = 1 ; -- Select only one rank from the selected format to insert into the destibation table.


SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL 
select * from silver.crm_cust_info where cst_id=29466

-- Check for Unwanted Spaces
-- Expectation: Nothing

--Step 1
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

--Step 2
select cst_id,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_gndr,
cst_marital_status
from bronze.crm_cust_info

--Repeat Step one and two for other columns


-- Data standardization & Consistency

--Step One
select distinct cst_gndr, cst_marital_status
FROM bronze.crm_cust_info

--Step 2
select * from (
select cst_id,
CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr -- Normalize gender values to readable format
from bronze.crm_cust_info) t
where cst_gndr not in ('Female', 'Male') 
--where cst_marital_status not in ('Single', 'Married')



--- CHECKING bronze.crm_prd_info

-- Check for Invalid Date Orders (Start Date > End Date)
select top(2) * from bronze.crm_cust_info
select top(2)
cid,
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid)) else cid end as cid
from bronze.erp_cust_az12


select * 
from bronze.erp_cust_az12
where bdate > GETDATE()


select 
bdate,
case when bdate > GETDATE() then  NULL else bdate end as bdate
from bronze.erp_cust_az12



select distinct(gen)
from bronze.erp_cust_az12;




select distinct 
gen,
case when UPPER(TRIM(gen)) IN ('F','Female')then 'Female'
when UPPER(TRIM(gen)) IN ('M', 'Male') then 'Male'
else 'n/a' 
end as gen
from 
bronze.erp_cust_az12;


select * from bronze.erp_loc_a101

select *,
replace(cid, '-','') as cid
from bronze.erp_loc_a101


select distinct cntry from bronze.erp_loc_a101
where LEN(cntry) < 2