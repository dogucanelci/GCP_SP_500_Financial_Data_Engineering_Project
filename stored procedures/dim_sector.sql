-- dim_sector.sql
CREATE TABLE IF NOT EXISTS stock_market_project4.dim_sector(
sector_id STRING,
gics_sector STRING,
gics_sub_industry STRING);

DELETE FROM stock_market_project4.dim_sector WHERE TRUE;
INSERT INTO  stock_market_project4.dim_sector(sector_id,gics_sector,gics_sub_industry)
	SELECT DISTINCT
			TO_HEX(SHA256(CONCAT(`GICS Sector`, '||', `GICS Sub Industry`))) AS sector_id
	    ,`GICS Sector` AS gics_sector
		,`GICS Sub Industry` AS gics_sub_industry
	FROM stock_market_project4.raw_sp500_companies
