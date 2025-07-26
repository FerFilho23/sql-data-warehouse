/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' tables from the 'bronze' tables.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
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


-- Populate the Table
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
WHERE c.prd_nm != TRIM(c.prd_nm)

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


-- Populate the Table
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


/*
===============================================================================
CRM SALES DETAILS
===============================================================================
*/

-- Check for Unwanted Spaces
SELECT c.sls_ord_num 
FROM bronze.crm_sales_details c 
WHERE c.sls_ord_num != TRIM(c.sls_ord_num)

-- Check for Matching Keys Between Tables
SELECT c.sls_cust_id  
FROM bronze.crm_sales_details c 
WHERE c.sls_cust_id NOT IN (
	SELECT cst_id FROM silver.crm_cust_info
)

-- Check for Invalid Dates
SELECT
	NULLIF(c.sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details c
WHERE c.sls_order_dt  <= 0 OR LENGTH(c.sls_order_dt) != 8

-- Check for Invalid Date Orders
SELECT 
	*
FROM bronze.crm_sales_details c 
WHERE c.sls_order_dt  > c.sls_ship_dt OR c.sls_order_dt  > c.sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- Sales = Quantity * Price -> Values must not be NULL, zero or negative
-- Rules:
-- 		1. If Sales are NEGATIVE, ZERO, or NULL, derive it from Quantity and Price
-- 		2. If Price is ZERO or NULL, calculate it using Sales and Quantity
-- 		3. If Prive is NEGATIVE, convert it to a positive value
SELECT DISTINCT
	c.sls_sales,
	c.sls_quantity,
	c.sls_price,
	CASE
    	WHEN (c.sls_sales IS NULL OR c.sls_sales <= 0)
        	OR (
            	c.sls_sales != c.sls_quantity * ABS(c.sls_price)
            	AND c.sls_quantity > 0
            	AND c.sls_price > 0
         	)
        		THEN c.sls_quantity * ABS(c.sls_price)
    	ELSE c.sls_sales
	END AS new_sls_sales,
	CASE 
        WHEN c.sls_price IS NULL OR c.sls_price = 0
            THEN c.sls_sales / NULLIF(c.sls_quantity, 0)
        WHEN c.sls_price < 0
            THEN ABS(c.sls_price)
        ELSE c.sls_price
    END AS new_sls_price
FROM bronze.crm_sales_details c 
WHERE sls_sales != c.sls_quantity * c.sls_price 
OR c.sls_sales <= 0 OR c.sls_quantity <= 0 OR c.sls_price <= 0
OR c.sls_sales IS NULL OR c.sls_quantity IS NULL OR c.sls_price IS NULL
ORDER BY c.sls_sales , c.sls_quantity , c.sls_price 



INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	c.sls_ord_num,
	c.sls_prd_key,
	c.sls_cust_id,
	CASE 
        WHEN c.sls_order_dt = 0 OR LENGTH(c.sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(c.sls_order_dt AS CHAR), '%Y%m%d')
    END sls_order_dt,
    CASE 
        WHEN c.sls_ship_dt = 0 OR LENGTH(c.sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(c.sls_ship_dt AS CHAR), '%Y%m%d')
    END sls_ship_dt,
    CASE 
        WHEN c.sls_due_dt = 0 OR LENGTH(c.sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(c.sls_due_dt AS CHAR), '%Y%m%d')
    END sls_due_dt,
	CASE
    	WHEN (c.sls_sales IS NULL OR c.sls_sales <= 0)
        	OR (
            	c.sls_sales != c.sls_quantity * ABS(c.sls_price)
            	AND c.sls_quantity > 0
            	AND c.sls_price > 0
         	)
        		THEN c.sls_quantity * ABS(c.sls_price)
    	ELSE c.sls_sales
	END AS sls_sales,
	c.sls_quantity,
	CASE 
        WHEN c.sls_price IS NULL OR c.sls_price = 0
            THEN c.sls_sales / NULLIF(c.sls_quantity, 0)
        WHEN c.sls_price < 0
            THEN ABS(c.sls_price)
        ELSE c.sls_price
    END AS sls_price
FROM bronze.crm_sales_details c 

