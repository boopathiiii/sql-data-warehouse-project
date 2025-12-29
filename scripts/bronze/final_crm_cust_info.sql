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
	cst_id,
	cst_key,
	-- Found and removed white spaces in firstname and lastname
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	-- Enriched cst_marital_status and cst_gndr == best for reporting
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A' -- Handled NULLs
	END AS cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A' -- Handled NULLs
	END AS cst_gndr,
	cst_create_date
FROM
(
-- Assigns row number based on the last cst_create_date (Row number > 1 = Duplicates) 
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_recent
FROM Bronze.crm_cust_info
)t WHERE flag_recent = 1 AND cst_id IS NOT NULL
