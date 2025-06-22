/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

create view gold.dim_products as (
select 
	ROW_NUMBER() over(order by p1.prd_start_dt,p1.prd_key) as product_key,
	p1.prd_id as product_id,
	p1.prd_key as product_number,
	p1.prd_nm as product_name,
	p1.cat_id as category_id, 
	p2.cat as category,
	p2.subcat as subcategory,
	p2.maintenance,
	p1.prd_cost as product_cost, 
	p1.prd_line as product_line, 
	p1.prd_start_dt as start_date
	
from [silver].[crm_prd_info] p1
left join [silver].[erp_px_cat_g1v2] p2
on p1.cat_id = p2.id
where p1.prd_end_dt is null --only the current version of products
)
