-- dim_company.sql
CREATE TABLE IF NOT EXISTS stock_market_project4.dim_company(
company_id STRING,
stock_code STRING,
company_name STRING,
found_year STRING);
-- Create the dimension table
DELETE FROM stock_market_project4.dim_company WHERE TRUE;
INSERT INTO  stock_market_project4.dim_company(company_id,stock_code,company_name,found_year)
	SELECT DISTINCT
		TO_HEX(SHA256(stock_code)) AS company_id
	  ,Stock_Code AS stock_code
		,`Company Name` AS company_name
		,Founded AS found_year
	FROM stock_market_project4.raw_sp500_companies
