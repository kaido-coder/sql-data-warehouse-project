/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    Ushbu protsedura 'bronze' sxemasidagi jadvallarni tashqi CSV fayllardan yuklaydi.
    Bajariladigan amallar:
    - Bronze jadvallarini tozalash (TRUNCATE).
    - 'COPY' buyrug'i orqali CSV fayllardan ma'lumotlarni yuklash.
    - Yuklash davomiyligini hisoblash va xabarlar chiqarish.

Parameters:
    batch_id (INT): Yuklash partiyasi raqami (Audit uchun).

Usage Example:
    CALL bronze.load_bronze(100);
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze(batch_id INT)
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
	
BEGIN
	batch_start_time := clock_timestamp();
	
	RAISE NOTICE '=======================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '=======================================';

	RAISE NOTICE '---------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '---------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	COPY bronze.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';
	

	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	
	COPY bronze.crm_prd_info (
		prd_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	
	COPY bronze.crm_sales_details (
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
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';

	RAISE NOTICE '---------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '---------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	COPY bronze.erp_loc_a101 (
		cid,
		cntry
	)
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	COPY bronze.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Trancating table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	COPY bronze.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance 
	)
	FROM 'C:\my projects\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FORMAT CSV, 
		HEADER,
		DELIMITER ','
	);
	end_time := clock_timestamp();
	RAISE NOTICE '>> Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '------------------------------------';

	batch_end_time := clock_timestamp();

	RAISE NOTICE '======================================';
	RAISE NOTICE 'Loading Broze Layer is Completed';
	RAISE NOTICE ' - Total Load Duration: %', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
	RAISE NOTICE '======================================';
	
EXCEPTION 
	WHEN OTHERS THEN
		RAISE NOTICE 'Xatolik yuz berdi!';
		RAISE NOTICE 'Xato xabari: %', SQLERRM;
		RAISE NOTICE 'Xato kodi: %', SQLSTATE;
		
END;
$$;
