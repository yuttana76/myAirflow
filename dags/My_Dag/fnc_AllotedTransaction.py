import pandas as pd
import requests
import zipfile
import os
from pathlib import Path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable

import psycopg2
import logging

from My_Dag.utils.fundconnext_util import getFundConnextToken, getAllottedCols


# get dag directory path (this might not be needed if you use Airflow's standard DAG folder structure)
dag_path = os.getcwd()


rawDataPath = Path(f'{dag_path}/data/raw_data/fnc')
processDataPath = Path(f'{dag_path}/data/processed_data')
extract_path = rawDataPath

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def notify_teams(context: dict):
    logging.info("** notify_teams.")


def T_GetToken():
    logging.info(f"getToken()")
    try:
        token = getFundConnextToken()
        return token 
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting token: {e}")
        raise AirflowException(f"Failed to get token: {e}")


def T_DownloadFile(token, fileType):
    logging.info(f"downloadFile fileType:{fileType}")
    download_file_path = f"{rawDataPath}/{fileType}.zip"

    # If today is monday get date of friday
    to = datetime.now()
    if to.weekday() == 0:  # Monday
        businessDate = (to - timedelta(days=3)).strftime("%Y%m%d")  # Get last Friday's date
    else:
        #Download date is yesterday
        yesterday = datetime.now() - timedelta(days=1)
        businessDate = yesterday.strftime("%Y%m%d") 

    # Override businessDate if CUSTOM_FNC_DATE is set (for testing purposes)
    # check CUSTOM_FNC_DATE is exists
    if Variable.get("CUSTOM_FNC_DATE", default_var=None) is not None:
        businessDate = Variable.get("CUSTOM_FNC_DATE")
    # businessDate = datetime.now().strftime("%Y%m%d")  # Use current date for robustness

    try:
        url = Variable.get("FC_API_URL") + f"/api/files/{businessDate}/{fileType}.zip"
        headers = {
            "X-Auth-Token": token,
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        os.makedirs(rawDataPath, exist_ok=True)  # More robust directory creation

        with open(download_file_path, 'wb') as f:
            f.write(response.content)  # More efficient than iter_content for a single file

        with zipfile.ZipFile(download_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            extracted_names = zip_ref.namelist()
            if extracted_names:
                logging.info("Extracted files and directories:")
                for item in extracted_names:
                    logging.info(item)
                return extracted_names[0]  # Assuming only one CSV is extracted
            else:
                logging.error(f"Extraction failed.")
                raise AirflowException("No files extracted from zip archive.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file: {e}")
        raise AirflowException(f"Failed to download file: {e}")
    except zipfile.BadZipFile as e:
        logging.error(f"Error with zip file: {e}")
        raise AirflowException(f"Invalid zip file: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise AirflowException(f"An unexpected error occurred: {e}")


def T_postgres_upsert_dataframe(fileName):
    logging.info(f"T_postgres_upsert_dataframe. {fileName}")
    try:
        df = pd.read_csv(f"{rawDataPath}/{fileName}", skiprows=1, header=None, sep='|')
        
        # df.columns = df.iloc[0] #Get Column name from the first row
        # df = df[1:] # Remove First row
        
        df.columns =  getAllottedCols()
        # df.columns =  getAllottedCols_custom()
        
        # Remove column Filler
        df.drop(columns=["filler"],inplace=True)

        # Replace  na with empty string
        # df.fillna("", inplace=True)

        # Remove duplicate row
        # df.drop_duplicates(inplace=True)


        empty_strings_in_numeric_cols = df[df.select_dtypes(include=['number']).astype(str).apply(lambda x: x == "").any(axis=1)]
        df[empty_strings_in_numeric_cols] = df[empty_strings_in_numeric_cols].fillna(0)

        #Convert nav_date to datetime object, ensuring correct format is used and handled for null values
        # Convert to datetime
        df['transactionDT'] = pd.to_datetime(df['transactionDT'], utc=True, format='%Y%m%d%H%M%S', errors='raise')

        df['effectiveDate'] = pd.to_datetime(df['effectiveDate'], format='%Y%m%d', errors='raise')
        df['allotmentDate'] = pd.to_datetime(df['allotmentDate'], format='%Y%m%d', errors='raise')
        df['amcPayDate'] = pd.to_datetime(df['amcPayDate'], format='%Y%m%d', errors='coerce')
        df['nav_date'] = pd.to_datetime(df['nav_date'], format='%Y%m%d', errors='coerce')
        
        # Replace 'NaT' with None
        df['transactionDT'] = df['transactionDT'].replace({pd.NaT: None})
        df['effectiveDate'] = df['effectiveDate'].replace({pd.NaT: None})
        df['allotmentDate'] = df['allotmentDate'].replace({pd.NaT: None})
        df['amcPayDate'] = df['amcPayDate'].replace({pd.NaT: None})
        df['nav_date'] = df['nav_date'].replace({pd.NaT: None})

        import numpy as np

        df = df.replace({np.nan: None})


        # write df to csv file
        df.to_csv(f"{processDataPath}/unitholderBalance.csv", index=False)

        cols = tuple(df.columns)
        update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
        placeholders = ', '.join(['%s'] * len(cols))
        
        sql = f"""
            INSERT INTO "stg_fnc_allottedtrans" ({', '.join(cols)},createdt,updatedt)
            VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON CONFLICT (transactionId,fundCode) DO UPDATE
            SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
        """

        logging.info(f"SQL query: {sql}")

        conn_params = {
            "host": Variable.get("POSTGRES_FCN_HOST"),
            "database": Variable.get("POSTGRES_FCN_DB"),
            "user": Variable.get("POSTGRES_FCN_USER"),
            "password": Variable.get("POSTGRES_FCN_PWD"),
            "port": Variable.get("POSTGRES_FCN_PORT")
        }

        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                data = [tuple(row) for row in df.values]  #Convert DataFrame to list of tuple
                
                logging.info(f"** data: {data}")

                cur.executemany(sql, data)
                conn.commit()
                logging.info(f"Upsert operation completed successfully.")

    except (psycopg2.Error, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logging.error(e)
        #show error messsge
        raise AirflowException(f"Database operation failed: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise AirflowException(f"An unexpected error occurred: {e}")

with DAG(
    'fnc_dw_AllotedTransaction',
    #start_date=days_ago(1),  #More robust
    start_date=datetime.now(),
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    on_failure_callback=notify_teams,
    tags=['FundConnext',],
) as dag:

    # task1 = PythonOperator(
    #     task_id='getToken_evening',
    #     python_callable=T_GetToken,
    #     do_xcom_push=True
    # )
    
    # fileType = "AllottedTransactions"
    # task2 = PythonOperator(
    #     task_id='dwn_fnc_unitholderBalance',
    #     python_callable=T_DownloadFile,
    #     op_kwargs={'token': '{{ ti.xcom_pull(task_ids="getToken_evening") }}', 'fileType': fileType},
    #     on_failure_callback=notify_teams,
    # )

    task3 = PythonOperator(
        task_id='pg_upsert_unitholderBalance',
        python_callable=T_postgres_upsert_dataframe,
        # op_kwargs={'fileName': '{{ ti.xcom_pull(task_ids="dwn_fnc_unitholderBalance") }}'},
        op_kwargs={'fileName': '20250922_MPS_ALLOTTEDTRANSACTIONS.txt'},
        on_failure_callback=notify_teams,
    )

    task3
    # task1 >> task2 >> task3

