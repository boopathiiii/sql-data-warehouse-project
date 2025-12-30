/*
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.
*/
-- =============================================================================
-- Create Dimension: Gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW Gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	ca.bdate AS birth_date,
	CASE
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM is the primary source for gender
		ELSE COALESCE(ca.gen, 'N/A')               -- Fallback to ERP data
	END AS gender,
	ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON ci.cst_key = la.cid

/*
-- Checking is the values in both columns are the same
-- From Business: CRM is the master table for gender info
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2;
*/

-- =============================================================================
-- Create Dimension: Gold.dim_products
-- =============================================================================
IF OBJECT_ID('Gold.dim_products', 'V') IS NOT NULL
    DROP VIEW Gold.dim_products;
GO
CREATE VIEW Gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM Silver.crm_prd_info pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- If start date is there and end date is NULL, that is the current info

-- =============================================================================
-- Create Dimension: Gold.fact_sales
-- =============================================================================
IF OBJECT_ID('Gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW Gold.fact_sales;
GO
CREATE VIEW Gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price AS price
FROM Silver.crm_sales_details sd
LEFT JOIN Gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN Gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

-- =============================================================================

/*
-- Final: Foreign key Integrity (Dimensions)

SELECT
	*
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN Gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL
*/
