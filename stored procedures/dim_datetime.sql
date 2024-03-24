-- dim_datetime.sql
CREATE TABLE IF NOT EXISTS stock_market_project4.dim_datetime(
datetime_id STRING,
quarter_date STRING,
close_date STRING);
-- Create the dimension table
DELETE FROM stock_market_project4.dim_datetime WHERE TRUE;
INSERT INTO  stock_market_project4.dim_datetime(datetime_id,quarter_date,close_date)
	SELECT DISTINCT
		TO_HEX(SHA256(CONCAT(quarter_date, '||', close_date))) AS datetime_id
	    ,Quarter_Date AS quarter_date
		,close_date
	FROM stock_market_project4.raw_sp500_companies
