-- fct_stock_info.sql
CREATE TABLE IF NOT EXISTS stock_market_project4.fct_stock_info(
stock_info_id STRING,
company_id STRING,
datetime_id STRING,
location_id STRING,
sector_id STRING,
gross_profit FLOAT64,
revenue_cost FLOAT64,
total_revenue FLOAT64,
net_income FLOAT64,
close_price FLOAT64
);
-- Create the fact table by joining the relevant keys from dimension table
DELETE FROM stock_market_project4.fct_stock_info WHERE TRUE;
INSERT INTO  stock_market_project4.fct_stock_info(stock_info_id,company_id,datetime_id,location_id,sector_id,gross_profit,revenue_cost,total_revenue,net_income,close_price)
    SELECT
        TO_HEX(SHA256(CONCAT(stock_code,quarter_date,close_date,Province,State,`GICS Sector`, `GICS Sub Industry`))) AS stock_info_id
        ,TO_HEX(SHA256(stock_code)) AS company_id
        ,TO_HEX(SHA256(CONCAT(quarter_date, '||', close_date))) AS datetime_id
        ,TO_HEX(SHA256(CONCAT(State, '||', Province))) AS location_id
        ,TO_HEX(SHA256(CONCAT(`GICS Sector`, '||', `GICS Sub Industry`))) AS sector_id
        ,`Gross Profit` AS gross_profit
        ,`Cost Of Revenue` AS revenue_cost
        ,`Total Revenue` AS total_revenue
        ,`Net Income` AS net_income
        ,`Price` AS close_price
	FROM stock_market_project4.raw_sp500_companies