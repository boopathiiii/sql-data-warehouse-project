-- Finding is there any NULLs or duplicates in the PK.

SELECT 
	cst_id, 
	COUNT(*) AS Quality_check
FROM Bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

/*
Output:
cst_id	Quality_check
29449	2
29473	2
29433	2
NULL	3
29483	2
29466	3
*/

--

-- Check for unwanted Spaces in both cst_firstname and cst_lastname

SELECT
	cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

/*
Output:
cst_firstname
 Jon
 Elizabeth
  Lauren
 Ian 
  Chloe
 Destiny
 Angela  
 Caleb
 Willie 
 Ruben 
 Javier 
 Nicole
 Maria 
 Allison 
 Adrian 
*/

-- Data standardization & consistency

SELECT 
	DISTINCT cst_gndr
FROM Bronze.crm_cust_info

/*
Output:
cst_gndr
NULL
F
M
*/

SELECT 
	DISTINCT cst_marital_status
FROM Bronze.crm_cust_info


-- prd_end_dt is invalid so we are taking the previous day of next the start date
SELECT
	prd_start_dt,
	-- Can't subtract date from DATE data type, alternate approach above.
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM Bronze.crm_prd_info

-- Check for any NULL or Negative values
SELECT
	prd_cost
FROM Bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for Invalid Date Orders

SELECT 
	*
FROM Silver.crm_prd_info
WHERE prd_end_date < prd_start_dt

-- Check for invalid date columns

SELECT sls_order_dt
FROM Bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;

-- Order date must be always smaller than the shipping date and the due date
SELECT * FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check for invalid dates

SELECT 
	bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
