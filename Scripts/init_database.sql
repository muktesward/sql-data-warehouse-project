/*============================================================================================
  Create Database and Schemas
  ========================================================================================
Script Purpose:
    This script creates a new databse named 'DataWareouse' after checking if it already exists.
    If the databse already exists , it is dropped and recreated. Additionally, the script created 3 schemas
    within the database: 'bronze', 'silver', 'gold'.
WARNING:
    Running the script will drop the entire database names 'DataWarehouse' if it exists.
    All data in the database will be permanently deleted. Procced with caution and ensure you have 
    proper backups before running the script.
*/


use master;
go

-- Drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter Database DataWarehouse set SINGLE_USER with rollback immediate;
	drop database DataWarehouse;
end;
go

--Create the 'DataWarehouse' database
create database DataWarehouse;
go

use DataWarehouse;
go

-- Create Schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
