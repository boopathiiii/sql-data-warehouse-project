/*
This SP performs ETL to populate the tables in Silver schema from Bronze schema. 

Performs:
	Data Cleansing
		Removes Duplicates
		Data Filtering
		Handling missing Data
		Handling invalid values
		Handling unwanted spaces
		Type casting
		Outlier Detection
	Data Aggregation
	Data Enrichment
	Data Integration
	Data Normalization & Standardization
	Implements Business logic in the data

Sample output:
=========================================
         Starting CRM Tables          
=========================================
    Truncating Table crm_cust_info    
     Inserting into crm_cust_info     
=========================================

(18484 rows affected)
=========================================
    Truncating Table crm_prd_info    
     Inserting into crm_prd_info     
=========================================

(397 rows affected)
=========================================
    Truncating Table crm_sales_details    
     Inserting into crm_sales_details     
=========================================

(60398 rows affected)
Load Duration for CRM Tables:1 seconds
=========================================
         Starting ERP Tables          
=========================================
    Truncating Table erp_cust_az12    
     Inserting into erp_cust_az12     
=========================================

(18483 rows affected)
=========================================
    Truncating Table erp_loc_a101    
     Inserting into erp_loc_a101     
=========================================

(18484 rows affected)
=========================================
    Truncating Table erp_px_cat_g1v2    
     Inserting into erp_px_cat_g1v2     
=========================================

(37 rows affected)
Load Duration for ERP Tables:0 seconds

*/

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		TRUNCATE TABLE Silver.crm_cust_info;
		PRINT '=========================================';
		PRINT '         Starting CRM Tables          ';
		PRINT '=========================================';
		PRINT '    Truncating Table crm_cust_info    ';
		PRINT '     Inserting into crm_cust_info     ';
		PRINT '=========================================';
		SET @start_time = GETDATE()
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

		--

		TRUNCATE TABLE Silver.crm_prd_info;
		PRINT '=========================================';
		PRINT '    Truncating Table crm_prd_info    ';
		PRINT '     Inserting into crm_prd_info     ';
		PRINT '=========================================';
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

		--

		TRUNCATE TABLE Silver.crm_sales_details;
		PRINT '=========================================';
		PRINT '    Truncating Table crm_sales_details    ';
		PRINT '     Inserting into crm_sales_details     ';
		PRINT '=========================================';
		INSERT INTO	Silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_ship_dt,
			sls_order_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			-- Checking for invalid dates and casting to DATE data type
			CASE 
				WHEN sls_ship_dt =  0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_order_dt =  0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_due_dt =  0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- Business Rule: Sales = Quantity * Price
			-- Negative, Zeros and NULLs are not allowed!

			-- If Sales is negative, zero or NULL, derive it using Quantity and Price
			-- If Price is zero or NULL, calculate it using Sales and Quantity
			-- If Price is negative, convert it to a positive value
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM Bronze.crm_sales_details

		SET @end_time = GETDATE()
		PRINT 'Load Duration for CRM Tables:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		--
		PRINT '=========================================';
		PRINT '         Starting ERP Tables          ';
		TRUNCATE TABLE Silver.erp_cust_az12;
		PRINT '=========================================';
		PRINT '    Truncating Table erp_cust_az12    ';
		PRINT '     Inserting into erp_cust_az12     ';
		PRINT '=========================================';
		SET @start_time = GETDATE()
		INSERT INTO Silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'N/A'
			END AS gen
		FROM Bronze.erp_cust_az12

		--

		TRUNCATE TABLE Silver.erp_loc_a101;
		PRINT '=========================================';
		PRINT '    Truncating Table erp_loc_a101    ';
		PRINT '     Inserting into erp_loc_a101     ';
		PRINT '=========================================';
		INSERT INTO Silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT 
			-- Fetching cid (cst_key in Silver.crm_cust_info)
			REPLACE(cid, '-', '') AS cid,
			-- Enriching countries
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry
		FROM Bronze.erp_loc_a101

		--

		TRUNCATE TABLE Silver.erp_px_cat_g1v2;
		PRINT '=========================================';
		PRINT '    Truncating Table erp_px_cat_g1v2    ';
		PRINT '     Inserting into erp_px_cat_g1v2     ';
		PRINT '=========================================';
		INSERT INTO Silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM Bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE()
		PRINT 'Load Duration for ERP Tables:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'Error occured during loading silver layer';
		PRINT 'Error message '+ ERROR_MESSAGE();
		PRINT 'Error message '+ CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error message '+ CAST(ERROR_STATE() AS VARCHAR);
		PRINT '=========================================';
	END CATCH
END
