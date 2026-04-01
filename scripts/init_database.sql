/*
=====================================================================
Create Database and Schemas
=====================================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

-- For PostgreSQL 13 and newer, you can forcefully drop a database 
-- and disconnect other users using the WITH (FORCE) clause:
DROP DATABASE IF EXISTS "DataWarehouse" WITH (FORCE);

-- Create the 'DataWarehouse' database
CREATE DATABASE "DataWarehouse";

-- To switch to the new database in the 'psql' command line tool, use:
\c DataWarehouse

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
