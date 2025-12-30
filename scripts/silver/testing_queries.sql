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
