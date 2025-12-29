INSERT INTO Silver.crm_prd_info 
(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	-- Fetching cat_id (id in Bronze.erp_px_cat_g1v2)
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	-- Fetching prd_key (sls_prd_key in Bronze.crm_sales_details) 
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
	-- Changing NULLs to 0
    ISNULL(prd_cost, 0) AS prd_cost,
	-- Enriching prd_line
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
    prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM Bronze.crm_prd_info

/*
-- If need we can check whether we have additional values in this table., 
Eg:
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
(SELECT DISTINCT id FROM Bronze.erp_px_cat_g1v2)
*/
