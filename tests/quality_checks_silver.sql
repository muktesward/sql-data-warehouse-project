/*
====================================================================================
DATA QUALITY CHECKS FOR crm_cust_info
====================================================================================
*/
-- Check for nulls or Duplicate Keys in Primary Key
--Expectations: No Result
SELECT sls_ord_num, count(*)
  FROM [DataWarehouse].[bronze].[crm_sales_details]
group by sls_ord_num
having count(*) > 1 or sls_ord_num is null;

-- Check for unwanted spaces
-- Expectations: No Results
select sls_prd_key
from [DataWarehouse].[bronze].[crm_sales_details]
where len(sls_prd_key) != len(trim(sls_prd_key))


--Data Standadization & Consistency
select distinct sls_ord_num
from [DataWarehouse].[bronze].[crm_sales_details]


select sls_cust_id from [DataWarehouse].[bronze].[crm_sales_details] 
where sls_cust_id   not in 
(select cst_id from silver.crm_cust_info)

---Check for invalid dates in bronze.crm_sales_details

select 
nullif(sls_order_dt,0) as sls_order_dt
from [DataWarehouse].[bronze].[crm_sales_details]
where sls_order_dt <= 0 or len(sls_order_dt) != 8

select 
*
from [DataWarehouse].[bronze].[crm_sales_details]
where sls_order_dt > sls_ship_dt or sls_order_dt >  sls_due_dt

select distinct
sls_sales,
sls_quantity,
sls_price
from [DataWarehouse].[bronze].[crm_sales_details]
where sls_sales != sls_quantity * sls_sales
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0  or sls_quantity <= 0 or sls_price <= 0


--Indentifying out of range date
select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

--distinct gender for bronze.erp_cust_az12
select distinct gen,
case when UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
	when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Male'
	else 'n/a'
	end as gen
from bronze.erp_cust_az12

-- silver.erp_loc_a101
select * from bronze.erp_loc_a101;
select * from silver.crm_cust_info;

-- removing the dash in the cid of bronze.erp_loc_a101

select
REPLACE(cid,'-','') as cid,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US','USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
	end as cntry
from bronze.erp_loc_a101

select distinct cntry as oldcountry,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US','USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
	end as cntry
from bronze.erp_loc_a101

select * from silver.erp_loc_a101


--- bronze.erp_px_cat_

select distinct id from bronze.erp_px_cat_g1v2
where id not in (
select distinct cat_id from silver.crm_prd_info
);

