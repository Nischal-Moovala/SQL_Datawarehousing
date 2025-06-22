/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
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

create or alter procedure silver.load_silver as
begin 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime

	begin try

		SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		-- Loading silver.crm_cust_info

		SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_cust_info';
				TRUNCATE TABLE silver.crm_cust_info;
				PRINT '>> Inserting Data Into: silver.crm_cust_info';

		insert into silver.crm_cust_info
		(
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)

		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status)) = 'M' then 'Married'
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				else 'n/a'
			end as cst_marital_status,
			case when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'n/a'
			end as cst_gndr,
			cst_create_date
		from 
		(
			select *, 
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t
		where flag_last = 1

		SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		-- Loading silver.crm_prd_info

		SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_prd_info';
				TRUNCATE TABLE silver.crm_prd_info;
				PRINT '>> Inserting Data Into: silver.crm_prd_info';

		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_') as cat_id, -- Extract category ID
			substring(prd_key,7,len(prd_key)) as prd_key,      -- Extract product key
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			case upper(trim(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end as prd_line,
			prd_start_dt,
			dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)) as prd_end_dt
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		-- Loading silver.crm_sales_details

		SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_sales_details';
				TRUNCATE TABLE silver.crm_sales_details;
				PRINT '>> Inserting Data Into: silver.crm_sales_details';

		insert into silver.crm_sales_details(
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
			  sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
      
			  case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
					else cast(cast(sls_order_dt as varchar) as date)
			  end as sls_order_dt,

			  case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
					else cast(cast(sls_ship_dt as varchar) as date)
			  end as sls_ship_dt,

      
			  case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
					else cast(cast(sls_due_dt as varchar) as date)
			  end as sls_due_dt,

			  CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
			   END AS sls_sales,

			  sls_quantity,
      
			  CASE 
					WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price  -- Derive price if original value is invalid
				END AS sls_price
		  FROM [DataWarehouse].[bronze].[crm_sales_details]

		SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		-- Loading [silver].[erp_cust_az12] table

		SET @start_time = GETDATE();
				PRINT '>> Truncating Table: [silver].[erp_cust_az12]';
				TRUNCATE TABLE [silver].[erp_cust_az12];
				PRINT '>> Inserting Data Into: [silver].[erp_cust_az12]';

		insert into [silver].[erp_cust_az12]
		(
			cid, 
			bdate, 
			gen
		)
		select 
				case when cid like '%NAS%' then SUBSTRING(cid,4,len(cid))
				else cid
			end as cid,
				case when bdate>GETDATE() then null
				else bdate
			end as bdate,
				case when upper(trim(gen)) in ('M','MALE') then 'Male'
				when upper(trim(gen)) in ('F', 'Female') then 'Female'
				else 'n/a'
			end as gen
		from[bronze].[erp_cust_az12]

		SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		-- loading silver.erp_loc_a101 table
		set @start_time = GETDATE()

		print'>>Truncating table silver.erp_loc_a101 table'
		TRUNCATE TABLE [silver].[erp_loc_a101];
		print'>>Inserting data into silver.erp_loc_a101 table'
		insert into silver.erp_loc_a101(cid,cntry)
		select 
			REPLACE(cid, '-', '') as cid,
			case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US', 'USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry) 
			end as cntry 
		from [bronze].[erp_loc_a101]

		SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		--loading silver.erp_px_cat_g1v2 table

		set @start_time = GETDATE()

			print'>>Truncating table silver.erp_px_cat_g1v2'
			TRUNCATE TABLE [silver].[erp_px_cat_g1v2]
			print'>>Inserting into silver.erp_px_cat_g1v2'
			insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
			SELECT
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;


		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	
	END try

	begin catch
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
	END CATCH

end
