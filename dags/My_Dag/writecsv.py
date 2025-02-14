# We'll start by importing the DAG object
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

import yfinance as yf
import time


# get dag directory path
dag_path = os.getcwd()


def writecsvfile(exec_date):

    print(f"writecsvfile for date: {exec_date}")

    import yfinance as yf
    import time

    symbols = ['MSFT', 'GOOGL', 'AMZN', 'TSLA','META','AAPL','1605.T','TECHM.NS']

    # # stock = yf.Ticker('AMZN')
    # # info=stock.info

    # # info.get('marketCap','N/A')
    # # info.get('P/E Ratio','N/A')

    screener_data = []

    # # for ticker in tickers_df['Ticker']:
    for ticker in symbols:
        print(f'fetching info for {ticker}')
        stock = yf.Ticker(ticker)
        info = stock.info
        
        screener_data.append({
            'Ticker':ticker,
            'Company Name':info.get('longName','N/A'),
            'Market Cap':info.get('marketCap','N/A'),
            'P/E Ratio':info.get('trailingPE','N/A'),
            'Dividend Yield':info.get('dividendYield','N/A'),
            'Sector':info.get('sector','N/A'),
            'Inddustry':info.get('industry','N/A'),
            'Currency':info.get('currency','USD'),
        })
    time.sleep(1)    
    screener_df = pd.DataFrame(screener_data)

    numeric_columns = ['Market Cap','P/E Ratio','Dividend Yield']
    for col in numeric_columns:
        screener_df[col] = pd.to_numeric(screener_df[col],errors='coerce')


    # # Exchanage rate
    currency_tickers = ['JPY=X','HKD=X','INR=X']
    exchange_rate_data = yf.Tickers(currency_tickers)

    exchange_rates = {'JPY':exchange_rate_data.tickers['JPY=X'].info.get('previousClose'),
                    'HKD':exchange_rate_data.tickers['HKD=X'].info.get('previousClose'),
                    'INR':exchange_rate_data.tickers['INR=X'].info.get('previousClose')
                    }

    def convert_to_usd(row):
        if row['Currency'] in exchange_rates and row['Market Cap'] != 'N/A':
            return row['Market Cap'] / exchange_rates[row['Currency']]
        return row['Market Cap']

    screener_df['Market Cap (USD)'] = screener_df.apply(convert_to_usd,axis=1)
    # screener_df.to_csv('./global_stock_screener.csv',index=False)

    # Save file
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
    output_dir = Path(f'{dag_path}/data/raw_data/{file_date_path}')
    output_dir.mkdir(parents=True, exist_ok=True)
    # processed_data/2021-08-15/12/2021-08-15_12.csv
    # screener_df.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False, mode='a')
    screener_df.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False)


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

writecsv_dag = DAG(
    'write-csv',
    default_args=default_args,
    description='Write to csv',
    # schedule_interval=timedelta(hours=1),
    # schedule_interval='0 12 * * *',
    schedule_interval="@hourly",
    # schedule_interval="*/2 * * * *",
    catchup=False,
)

task1 = PythonOperator(
    task_id='write_csv',
    python_callable=writecsvfile,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=writecsv_dag,
)


task1
