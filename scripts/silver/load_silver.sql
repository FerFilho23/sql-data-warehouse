/*
===============================================================================
Load Script: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' tables from the 'bronze' tables.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/


/*
===============================================================================
CRM CUSTOMER INFO
===============================================================================
*/  

-- Check for Nulls or Duplicates in Primary Key
SELECT c.cst_id, COUNT(*) as unique_id
FROM bronze.crm_cust_info c 
GROUP BY c.cst_id
HAVING unique_id > 1 OR c.cst_id IS NULL OR c.cst_id = 0;


-- Check for Unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info c 
WHERE c.cst_firstname != TRIM(c.cst_firstname )


-- Data Standardization & Consistency
SELECT DISTINCT c.cst_gndr
FROM bronze.crm_cust_info c 


INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT 
	t.cst_id,
	t.cst_key,
	TRIM(t.cst_firstname) AS cst_firstname, -- Remove unwanted spaces
	TRIM(t.cst_lastname) AS cst_lastname,
	CASE UPPER(TRIM(t.cst_marital_status))
		WHEN 'S' THEN 'Single'
		WHEN 'M' THEN 'Married'
		ELSE 'N/A' -- Handling missing values
	END cst_marital_status, -- Normalize to readable format
	CASE UPPER(TRIM(t.cst_gndr))
		WHEN 'F' THEN 'Female'
		WHEN 'M' THEN 'Male'
		ELSE 'N/A'
	END cst_gndr, -- Normalize to readable format
	t.cst_create_date
FROM (
	SELECT *,
		RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS date_rank -- Identifying duplicates
	FROM bronze.crm_cust_info c
)t WHERE t.date_rank = 1 and t.cst_id != 0; -- Filtering the duplicates by the most recent record per customer


/*
===============================================================================
CRM PRODUCT INFO
===============================================================================
*/  

-- Check for Nulls or Duplicates in Primary Key
SELECT c.prd_id , COUNT(*) as unique_id
FROM bronze.crm_prd_info c 
GROUP BY c.prd_id 
HAVING unique_id > 1 OR c.prd_id IS NULL OR c.prd_id = 0;

-- Check for Unwanted Spaces
SELECT c.prd_nm 
FROM bronze.crm_prd_info c 
WHERE c.prd_nm != TRIM(c.prd_nm )

-- Check for NULLS or Negative Numbers
SELECT c.prd_cost 
FROM bronze.crm_prd_info c 
WHERE c.prd_cost < 0 or c.prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT c.prd_line 
FROM bronze.crm_prd_info c 

-- Check for Invalid Date Others
SELECT * FROM bronze.crm_prd_info c 
WHERE c.prd_end_dt < c.prd_start_dt or c.prd_start_dt = 0000-00-00 or c.prd_end_dt = 0000-00-00

SELECT 
	c.prd_id,
	c.prd_key,
	c.prd_nm,
	c.prd_start_dt,
	DATE_SUB(LEAD(c.prd_start_dt) OVER (PARTITION BY c.prd_key ORDER BY c.prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM bronze.crm_prd_info c 
WHERE c.prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_key,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt 
)
SELECT 
	c.prd_id,
	REPLACE(SUBSTRING(c.prd_key, 1, 5), '-', '_') as cat_id, -- separate cat_id and transform it to match 'id' from bronze.erp_px_cat_g1v2
	SUBSTRING(c.prd_key, 7, LENGTH(c.prd_key)) as prd_key, -- separate prd_key and transform it to match 'sls_prod_key' from bronze.crm_sales_details
	c.prd_nm,
	c.prd_cost,
	CASE UPPER(TRIM(c.prd_line))
		WHEN 'M' THEN 'Mountain' 
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A' -- Handling missing values
	END prd_line, -- Normalize to readable format
	c.prd_start_dt,
	DATE_SUB(LEAD(c.prd_start_dt) OVER (PARTITION BY c.prd_key ORDER BY c.prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt -- Fix invalid date intervals
FROM bronze.crm_prd_info c 
-- WHERE REPLACE(SUBSTRING(c.prd_key, 1, 5), '-', '_') IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2) -- check for the cat_id matching with bronze.erp_px_cat_g1v2 
-- WHERE SUBSTRING(c.prd_key, 7, LENGTH(c.prd_key)) IN (SELECT DISTINCT c.sls_prd_key FROM bronze.crm_sales_details c) -- check for the prd_key matching with bronze.crm_sales_details 







