/*
=======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from 
	external CSV files.
	It performs the following actions:
		- Truncates the bronze tables before loading the data.
		- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.
	Parameters:
		None.
		This Stored procedure does not accept any parameters or return any value.
	Usage Example:
		EXEC bronze.load_bronze
========================================================================================
*/
create or alter procedure bronze.load_bronze
as
begin
	declare @start_time datetime , @end_time datetime , @batch_start_time datetime, @batch_end_time datetime
	begin try
		set @batch_start_time = GETDATE();
		print '============================================';
		print ' Loading the BRONZE Layer';
		print '============================================';

		print '----------------------------------------------';
		print 'Loading CRM Tables';
		print '----------------------------------------------';
		-- Bulk insert into bronze.crm_cust_info
		--make sure you delete the data before the insert 
		-- as it might load the data again and again

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info; -- every time this code executes it truncates the table first
	
		print '>> Inserting data into:bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info     -- then does the bulk insert
		from 'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';

		-- Same for the bronze.crm_prd_info

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;

		print '>> Inserting data into:bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';

		-- Same for the bronze.crm_sales_details

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details;

		print '>> Inserting data into:bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';


		print '----------------------------------------------';
		print 'Loading ERP Tables';
		print '----------------------------------------------';
		-- Same for the bronze.erp_cust_az12

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12;

		print '>> Inserting data into:bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12
		from 'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';

		-- Same for the bronze.erp_loc_a101

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101;

		print '>> Inserting data into:bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from  'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';

		-- Same for the bronze.crm_prd_info

		set @start_time = GETDATE();
		print '>>Truncating Table: bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting data into:bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from  'C:\FromHardDisk\sql_learning\sql-data-warehouseProject\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = GETDATE();
		print '>> Load Duration: '+ cast (datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '>>-----------------------';

		set @batch_end_time = GETDATE();
		print '==========================================='
		print 'Loading Beonze Layer is Completed';
		print '   -Total Load Duration: ' + cast (datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' Seconds';
		print '==========================================='
	end try
	begin catch
		print '==================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Error Message' + cast (error_state() as nvarchar);
	end catch
end
