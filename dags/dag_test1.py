import bs4 as bs
from bs4 import BeautifulSoup
import requests
import yfinance as yf
import datetime
import pandas as pd


pivot_table_columns = pivot_table_columns = ['Date', 'Tax Effect Of Unusual Items', 'Tax Rate For Calcs',
       'Normalized EBITDA', 'Total Unusual Items',
       'Total Unusual Items Excluding Goodwill',
       'Net Income From Continuing Operation Net Minority Interest',
       'Reconciled Depreciation', 'Reconciled Cost Of Revenue', 'EBITDA',
       'EBIT', 'Net Interest Income', 'Interest Expense', 'Interest Income',
       'Normalized Income',
       'Net Income From Continuing And Discontinued Operation',
       'Total Expenses', 'Total Operating Income As Reported',
       'Diluted Average Shares', 'Basic Average Shares', 'Diluted EPS',
       'Basic EPS', 'Diluted NI Availto Com Stockholders',
       'Net Income Common Stockholders', 'Net Income', 'Minority Interests',
       'Net Income Including Noncontrolling Interests',
       'Net Income Continuous Operations',
       'Earnings From Equity Interest Net Of Tax', 'Tax Provision',
       'Pretax Income', 'Other Income Expense', 'Special Income Charges',
       'Gain On Sale Of Business', 'Net Non Operating Interest Income Expense',
       'Interest Expense Non Operating', 'Interest Income Non Operating',
       'Operating Income', 'Operating Expense', 'Research And Development',
       'Selling General And Administration',
       'General And Administrative Expense', 'Other Gand A',
       'Salaries And Wages', 'Gross Profit', 'Cost Of Revenue',
       'Total Revenue', 'Operating Revenue', 'Stock_Code']


# DESCRIPTIVE TABLE (country,foundation_date,company_name,company_symbol,sector etc.)

# İstediğiniz web sayfasının URL'sini belirtin
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

# Web page extraction
response = requests.get(url)
# HTML içeriğini alın
html_content = response.text
# BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

table = soup.find('table', {'class': 'wikitable sortable'})
# DataFrame
data = []
rows = table.find_all('tr')
for row in rows:
    columns = row.find_all('td')
    if columns:
        data.append([column.text.strip() for column in columns])
# Create DataFrame
df_desc = pd.DataFrame(data, columns=['Symbol', 'Security','GICS Sector', 'GICS Sub Industry', 'Headquarters Location', 'Date first added', 'CIK', 'Founded'])
df_desc['State'] = df_desc['Headquarters Location'].str.split(',',expand=True)[1].str[1:]
df_desc['Province'] = df_desc['Headquarters Location'].str.split(',',expand=True)[0]
df_desc.drop(columns=['Headquarters Location','CIK','Date first added'],inplace=True)
df_desc.rename(columns={'Security':'Company Name'},inplace=True)
tickers = list(df_desc.Symbol)

# FINANCIAL METRIC QUARTER TABLE(Net Income,Total Revenue,Stock_Code, Quarter Date etc)
pivot_df_main = pd.DataFrame(columns=pivot_table_columns)
for ticker in tickers:
    # Pivot işlemi
    df = yf.Ticker(ticker).quarterly_income_stmt
    df.loc[:,df.columns[:-1]]
    pivot_df = df.T.reset_index()
    # 'Date' ismini 'date' olarak değiştirme
    pivot_df.rename(columns={'index': 'Quarter_Date'}, inplace=True)
    pivot_df['Stock_Code'] = ticker
    pivot_df_main = pd.concat([pivot_df_main,pivot_df],axis=0)
    

pivot_df_main.fillna(0,inplace=True)
pivot_df_main.drop(columns='Date',inplace=True)

pivot_df_main = pivot_df_main[['Stock_Code','Quarter_Date','Gross Profit', 'Cost Of Revenue',
       'Total Revenue','Net Income']]



# Tarih aralığını belirleyelim (son 7 gün)
end_date = datetime.datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

# Hisse senedi fiyatlarını yfinance kullanarak çekelim
dfs = []
for code in tickers:
    stock_data = yf.download(code, start=start_date, end=end_date)
    stock_data['Stock'] = code  # Hisse kodunu ekleyelim
    dfs.append(stock_data)

# Tüm veri kümelerini birleştirelim
df_close = pd.concat(dfs)
# Son 7 günlük fiyatları alalım
last_seven_days_prices = df_close.reset_index().pivot_table(index='Stock', columns='Date', values='Close')
df_close_last7 = last_seven_days_prices.reset_index().melt(id_vars='Stock', var_name='Date', value_name='Price')
# 'Date' sütununu datetime formatına dönüştürelim
df_close_last7['Date'] = pd.to_datetime(df_close_last7['Date'])


pivot_df_main_merged = pivot_df_main.merge(df_close_last7.rename(columns={'Date':'close_date','Stock':'Stock_Code'}),on=['Stock_Code'],how='left') \
    .merge(df_desc.rename(columns={'Symbol':'Stock_Code'}),on=['Stock_Code'],how='left')
# /usr/local/airflow/include/raw_dataset/
pivot_df_main_merged.to_csv('out.csv', index=False)



if __name__=="__main__": 
    main() 