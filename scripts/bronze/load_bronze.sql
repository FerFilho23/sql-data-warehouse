/*
===============================================================================
Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' database from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA LOCAL INFILE` command to load data from CSV files to bronze tables.
===============================================================================
*/

USE bronze;

-- ===============================================================================
-- CRM TABLES
-- ===============================================================================


TRUNCATE TABLE crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  -- Skip header row 



TRUNCATE TABLE crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  



TRUNCATE TABLE crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  


-- ===============================================================================
-- ERP TABLES
-- ===============================================================================

TRUNCATE TABLE erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_erp/cust_az12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;



TRUNCATE TABLE erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  



TRUNCATE TABLE erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/ferfilho/Desktop/sql-data-warehouse/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  
