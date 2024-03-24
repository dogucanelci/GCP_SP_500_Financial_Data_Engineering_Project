-- dim_location.sql

-- Create the dimension table

CREATE TABLE IF NOT EXISTS stock_market_project4.dim_location(
location_id STRING,
State STRING,
Province STRING);

DELETE FROM stock_market_project4.dim_location WHERE TRUE;
INSERT INTO  stock_market_project4.dim_location(location_id,state,province)
	SELECT DISTINCT
			TO_HEX(SHA256(CONCAT(State, '||', Province))) AS location_id
	    ,State AS state
		,Province AS province
	FROM stock_market_project4.raw_sp500_companies