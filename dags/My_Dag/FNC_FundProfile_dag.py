import sys
import pandas as pd
import requests
import zipfile
import os
from pathlib import Path

from datetime import datetime
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago
from airflow.models import Variable

import psycopg2
import logging

# get dag directory path
dag_path = os.getcwd()

fileType = "FundProfile"  
rawDataPath = Path(f'{dag_path}/data/raw_data/fnc')
processDataPath = Path(f'{dag_path}/data/processed_data/fnc')
extract_path = rawDataPath
# token=""

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def notify_teams(context: dict):
  print("** notify_teams.")


def T_GetToken():
  logging.info(f"getToken()")
  
  try:

    api_url = Variable.get("FC_API_URL")+"/api/auth"
    data = {
      "username": Variable.get("FC_API_USER"),
      "password": Variable.get("FC_API_PASSPOWRD")
    }

    # print(f"FC_API_URL: {api_url}")

    # api_url = Variable.get("AIRFLOW_VAR_FC_API_URL")+"/auth"
    # data = {
    #   "username": Variable.get("AIRFLOW_VAR_FC_API_USER"),
    #   "password": Variable.get("AIRFLOW_VAR_FC_API_PASSPOWRD")
    # }
    # api_url = os.environ["AIRFLOW_VAR_FC_API_URL"]+"/auth"
    # data = {
    #   "username": os.environ["AIRFLOW_VAR_FC_API_USER"],
    #   "password": os.environ["AIRFLOW_VAR_FC_API_PASSPOWRD"]
    # }

    response = requests.post(api_url, json=data)

    response.raise_for_status()  # Raise an exception for bad status codes
    response_data = response.json()
    token = response_data.get('access_token')  
    return token
  
    # return response
  except requests.exceptions.RequestException as e:
    logging.error(f"{e}")
    sys.exit(1)
    # return None


def T_DownloadFile(token,fileType):
  
  logging.info(f"downloadFile fileType:{fileType}")

  download_file_path = f"{rawDataPath}/{fileType}.zip"
  businessDate = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
  businessDate = datetime.now().strftime("%Y%m%d")

  try:
    # url = os.environ["FC_API_URL"]+f"/files/{businessDate}/{fileType}.zip" 
    url = Variable.get("FC_API_URL")+f"/api/files/{businessDate}/{fileType}.zip" 
    headers = {
    "X-Auth-Token": token,
    "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

    if not os.path.exists(rawDataPath):
      os.makedirs(os.path.dirname(rawDataPath)) 

    
    with open(download_file_path, 'wb') as f:
      for chunk in response.iter_content(chunk_size=8192): 
        f.write(chunk)

    # Save file
    headers = response.headers
    if headers:
      content_type = headers.get('Content-Type')
      content_disposition = headers.get('Content-Disposition')

      if content_type == 'application/zip' and content_disposition:
        filename = content_disposition.split(';')[1].strip().split('=')[1].strip('"') 
        print(f"Filename: {filename}") 

    # Exptrtact zip file
    # extract_path = Path(f'{dag_path}/data/raw_data')
    try:
      with zipfile.ZipFile(download_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
        extracted_names = zip_ref.namelist()  # Get the list of extracted file/dir names

        if extracted_names:
            print("Extracted files and directories:")
            for item in extracted_names:
                print(item)
                filename = item
        else:
            logging.error(f"Extraction failed.")

      # return filename
    except Exception as e:
      logging.error(f"{e}")
      sys.exit(1)

    # return download_file_path
    return filename
  except requests.exceptions.RequestException as e:
    logging.error(f"{e}")
    sys.exit(1)

def T_postgres_upsert_dataframe(fileName):
    logging.info(f"T_postgres_upsert_dataframe. {fileName}")

    try:
      df = pd.read_csv(f"{rawDataPath}/{fileName}", skiprows=1, header=None, sep='|')  # No header row initially, pipe delimited

      # Add column name
      df.columns =  ["fund_code",
      "amc_code",
      "fundname_th",
      "fundname_en",
      "fund_policy",
      "tax_type",
      "fif_flag",
      "dividend_flag",
      "registration_date",
      "fund_risk_level",
      "fx_risk_flag",
      "fatca_allow_flag",
      "buy_cut_off_time",
      "fst_lowbuy_val",
      "nxt_lowbuy_val",
      "sell_cut_off_time",
      "lowsell_val",
      "lowsell_unit",
      "lowbal_val",
      "lowbal_unit",
      "sell_settlement_day",
      "switching_settlement_day",
      "switch_out_flag",
      "switch_in_flag",
      "fund_class",
      "buy_period_flag",
      "sell_period_flag",
      "switch_in_periold_flag",
      "switch_out_periold_flag",
      "buy_pre_order_day",
      "sell_pre_order_day",
      "switch_pre_order_day",
      "auto_redeem_fund",
      "beg_ipo_date",
      "end_ipo_date",
      "plain_complex_fund",
      "derivatives_flag",
      "lag_allocation_day",
      "settlement_holiday_flag",
      "hyealth_insurrance",
      "previous_fund_code",
      "investor_alert",
      "isin",
      "lowbal_condition",
      "project_retail_type",
      "fund_compare_perfermance_description",
      "allocate_digit",
      "etf_flag",
      "trustee",
      "registrar",
      "register_id",
      "lmts_notice_period_amount",
      "lmts_notice_perios_%aum",
      "lmts_adls_amount",
      "lmts_adls_%aum",
      "lmts_liquidity_fee_amount",
      "lmts_liquidity_fee_%aum",
      "other_information_url",
      "currency",
      "complex_fund_presentation",
      "risk_acknowledgement_of_complex_fund",
      "redemption_type_condition",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler",
      "Filler"]

      # Remove column Filler
      df.drop(columns=["Filler"],inplace=True)

      # Remove duplicate data
      df.drop_duplicates(inplace=True)

      # Replace NaN is ""
      df.fillna("", inplace=True)

      try:
          postgres_fcn_host = Variable.get("POSTGRES_FCN_HOST")
          postgres_fcn_db = Variable.get("POSTGRES_FCN_DB")
          postgres_fcn_user = Variable.get("POSTGRES_FCN_USER")
          posetgres_fcn_password = Variable.get("POSTGRES_FCN_PWD")
          postgres_fcn_port = Variable.get("POSTGRES_FCN_PORT")

          # print(f"host:{postgres_fcn_host}; database:{postgres_fcn_db}; user:{postgres_fcn_user}")

          conn = psycopg2.connect(
              host=postgres_fcn_host,
              database=postgres_fcn_db,
              user=postgres_fcn_user,
              password=posetgres_fcn_password,
              port=postgres_fcn_port
          )

          cur = conn.cursor()
  
          for index, row in df.iterrows():
              fund_code = row.get('fund_code')
              amc_code = row.get('amc_code')
              fundname_th = row.get('fundname_th')
              fundname_en = row.get('fundname_en')

              if fund_code is None:
                  logging.error(f"Missing fund_code in row {index}. Skipping.")
                  continue  # Skip to the next row

              cur.execute(
                  """
                  INSERT INTO "stg_fnc_fundProfile" (fund_code, amc_code, fundname_th, fundname_en)
                  VALUES (%s, %s, %s, %s)
                  ON CONFLICT (fund_code) DO UPDATE
                  SET amc_code = EXCLUDED.amc_code,
                      fundname_th = EXCLUDED.fundname_th,
                      fundname_en = EXCLUDED.fundname_en;
                  """,(fund_code, amc_code, fundname_th, fundname_en),
              )
              logging.info(f"Upserted row {index} with fund_code: {fund_code}")

          conn.commit()
          logging.info(f"Upsert operation completed successfully.")

      except psycopg2.Error as e:
          logging.error(f" {e}")
          conn.rollback()

      finally:
          if conn:
              cur.close()
              conn.close()      

    except requests.exceptions.RequestException as e:
      logging.error(f"Error download file and save: {e}")
      sys.exit(1)


# initializing the default arguments that we'll pass to our DAG
with DAG(
        'FNC_FundProfile_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval="0 8 * * 1-5", 
        catchup=False,
        on_failure_callback=notify_teams,
) as dag:

    task1 = PythonOperator(
        task_id='getToken',
        python_callable=T_GetToken,
        dag=dag,
        do_xcom_push=True 
    )

    task2 = PythonOperator(
        task_id='downloadFiles',
        python_callable=T_DownloadFile,
        op_kwargs={'token': '{{ ti.xcom_pull(task_ids="getToken") }}', 'fileType': fileType},
        dag=dag,
        on_failure_callback=notify_teams,
    )

    task3 = PythonOperator(
        task_id='postgres_upsert',
        python_callable=T_postgres_upsert_dataframe,
        op_kwargs={'fileName': '{{ ti.xcom_pull(task_ids="downloadFiles") }}'},
        dag=dag,
        on_failure_callback=notify_teams,
    )

    task1 >> task2 >> task3