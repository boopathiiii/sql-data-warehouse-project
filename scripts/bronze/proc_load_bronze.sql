/*
This SP loads data into the tables in Bronze schema from external csv files. It truncates the table before loading data and uses 'BULK INSERT'

Sample Output:
=========================================
         Loading Bronze Layer
=========================================
          Loading CRM Tables
=========================================
(18493 rows affected)
(397 rows affected)
(60398 rows affected)
Load Duration for CRM Tables:0 seconds
=========================================
          Loading ERP Tables
=========================================
(18483 rows affected)
(18484 rows affected)
(37 rows affected)
Load Duration for ERP Tables:1 seconds
*/

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '=========================================';
		PRINT '         Loading Bronze Layer';
		PRINT '=========================================';
		PRINT '          Loading CRM Tables';
		PRINT '=========================================';
		
		SET @start_time = GETDATE()

		TRUNCATE TABLE Bronze.crm_cust_info;
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\B\Downloads\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE Bronze.crm_prd_info;
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\B\Downloads\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE Bronze.crm_sales_details;
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\B\Downloads\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE()

		PRINT 'Load Duration for CRM Tables:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '=========================================';
		PRINT '          Loading ERP Tables';
		PRINT '=========================================';

		SET @start_time = GETDATE()

		TRUNCATE TABLE Bronze.erp_cust_az12;
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\B\Downloads\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE Bronze.erp_loc_a101;
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\B\Downloads\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\B\Downloads\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE()

		PRINT 'Load Duration for ERP Tables:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'Error occured during loading bronze layer';
		PRINT 'Error message '+ ERROR_MESSAGE();
		PRINT '=========================================';
	END CATCH
END
