-- ads_stock_info.sql
CREATE TABLE IF NOT EXISTS stock_market_project4.ads_stock_info(
stock_code STRING
,company_name  STRING
,found_year  STRING
,State  STRING
,Province  STRING
,gics_sector STRING
,gics_sub_industry STRING
,quarter_date  STRING
,close_date  STRING
,close_price FLOAT64
,gross_profit FLOAT64
,revenue_cost FLOAT64
,total_revenue FLOAT64
,net_income FLOAT64
);
-- Create the fact table by joining the relevant keys from dimension table
DELETE FROM stock_market_project4.ads_stock_info WHERE TRUE;
INSERT INTO  stock_market_project4.ads_stock_info
(stock_code 
,company_name 
,found_year 
,State 
,Province 
,gics_sector 
,gics_sub_industry 
,quarter_date 
,close_date 
,close_price
,gross_profit 
,revenue_cost 
,total_revenue 
,net_income )
    SELECT 
    stock_code 
    ,company_name 
    ,found_year 
    ,State 
    ,Province 
    ,gics_sector 
    ,gics_sub_industry 
    ,quarter_date 
    ,close_date 
    ,close_price
    ,gross_profit 
    ,revenue_cost 
    ,total_revenue 
    ,net_income 
    from `stock_market_project4.fct_stock_info` t1
    inner join `stock_market_project4.dim_datetime` t2 on t1.datetime_id = t2.datetime_id
    inner join `stock_market_project4.dim_location` t3 on t1.location_id = t3.location_id
    inner join `stock_market_project4.dim_company` t4 on t1.company_id = t4.company_id
    inner join `stock_market_project4.dim_sector` t5 on t1.sector_id = t5.sector_id;