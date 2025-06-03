/*
=======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=======================================================================
Script Purpose:
	This stored procedure loads data into the 'Silver' schema from 
	bronze schema.
	It performs the following actions:
		- Truncates the Silver tables before loading the data.
		- Uses the 'INSERT INTO' command to load data bronze to silver tables.
	Parameters:
		None.
		This Stored procedure does not accept any parameters or return any value.
	Usage Example:
		EXEC siver.load_silver
========================================================================================
*/
create or alter procedure silver.load_silver as 
begin
	declare @start_time as datetime , @end_time as datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		set @batch_start_time = GETDATE();
		set @start_time = GETDATE();
		print '>> Truncating Table : silver.crm_cust_info'
		truncate table silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info'
		insert into silver.crm_cust_info(
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
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_marital_status,

		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
		from (select *,
		row_number() over(partition by cst_id order by cst_create_date desc ) as flag_last
		from bronze.crm_cust_info) as x
		-- where cst_id = 29466;
		where flag_last = 1 and cst_id is not null;
		set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.crm_cust_info:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'
		print '=========================================='

	
		set @start_time = GETDATE();
		print '>> Truncating Table : silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';

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
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case UPPER(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.crm_prd_info:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'

		print '============================================'

		set @start_time = GETDATE();
		print '>> Truncating Table : silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>> Inserting Data Into: crm_sales_details';

		insert into silver.crm_sales_details
		(
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
		SELECT  [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,case when [sls_order_dt] = 0 or len(sls_order_dt) != 8 then null
					 else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt
			  ,case when [sls_ship_dt] = 0 or len([sls_ship_dt]) != 8 then null
					else cast(cast([sls_ship_dt] as varchar) as date) end as [sls_ship_dt]
			  ,case when [sls_due_dt] = 0 or len([sls_due_dt]) != 8 then null
					else cast(cast([sls_due_dt] as varchar) as date) end as [sls_due_dt]
			  ,[sls_sales]
			  ,[sls_quantity]
			  ,[sls_price]
		  FROM [DataWarehouse].[bronze].[crm_sales_details]
		  set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.crm_sales_details:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'

		print '=========================================================='

		set @start_time = GETDATE();
		print '>> Truncating Table : silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';

		insert into silver.erp_cust_az12(cid,
			bdate,
			gen
		)
		select 
		case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			else cid
			end as cid,
		case when bdate > GETDATE() then null
			else bdate 
			end as bdate,
		case when UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
			when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
			else 'n/a'
			end as gen
		from bronze.erp_cust_az12
		set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.erp_cust_az12:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'

		print '============================================================'

		set @start_time = GETDATE();
		print '>> Truncating Table : silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';

		insert into silver.erp_loc_a101
		(
			cid,
			cntry
			)
		select
		REPLACE(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US','USA') then 'United States'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
			end as cntry
		from bronze.erp_loc_a101;

		set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.erp_loc_a101:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'

		print '============================================================'

		set @start_time = GETDATE();
		print '>> Truncating Table : silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		insert into silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time = GETDATE()
		print 'Time taken to truncate and load silver.erp_px_cat_g1v2:  ' + cast(datediff(second,@start_time ,@end_time)as nvarchar) + 'seconds'
	end try
	begin catch
		print '========================================================';
		print 'ERROR OCCURED DURING LOADING SILVER LAYER.';
		print 'ERROR MESSAGE'+  ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	end catch
	set @batch_end_time = GETDATE()
	print '-------------------------------------------------------'
	print 'Time taken to complete the batch & load silver: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds'
end 
