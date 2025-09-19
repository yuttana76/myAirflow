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

from My_Dag.utils.fundconnext_util import getFundConnextToken, getNavColsV2


# get dag directory path (this might not be needed if you use Airflow's standard DAG folder structure)
dag_path = os.getcwd()

fileType = "Nav"
rawDataPath = Path(f'{dag_path}/data/raw_data/fnc')
processDataPath = Path(f'{dag_path}/data/processed_data/fnc')
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

    yesterday = datetime.now() - timedelta(days=1)
    businessDate = yesterday.strftime("%Y%m%d") 

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
        # df.columns =  getNavColsV3()   #Support Nav V3 only
        df.columns =  getNavColsV2()
        
        # Remove column Filler
        df.drop(columns=["filler"],inplace=True)

        # df.fillna("", inplace=True)
        df.drop_duplicates(inplace=True)


        # ??????????

        empty_strings_in_numeric_cols = df[df.select_dtypes(include=['number']).astype(str).apply(lambda x: x == "").any(axis=1)]
        # print(f"*Empty strings in numeric columns:\n{empty_strings_in_numeric_cols}")  #Print rows containing empty strings in numeric columns


        # numeric_cols = ['aum', 'nav', 'offer_nav', 'bid_nav', 'switch_out_nav', 'switch_in_nav', 'total_unit', 'total_aum_all_share_class', 'total_unit_all_share_class']
        # df[empty_strings_in_numeric_cols] = df[empty_strings_in_numeric_cols].fillna(pd.NA)

        # numeric_cols = ['aum', 'nav', 'offer_nav', 'bid_nav', 'switch_out_nav', 'switch_in_nav', 'total_unit', 'total_aum_all_share_class', 'total_unit_all_share_class']
        df[empty_strings_in_numeric_cols] = df[empty_strings_in_numeric_cols].fillna(0)

        # ??????????
        #Convert nav_date to datetime object, ensuring correct format is used and handled for null values
        df['nav_date'] = pd.to_datetime(df['nav_date'], format='%Y%m%d', errors='coerce')

        #Handle potential errors from date conversion.  You might want a more sophisticated error handling strategy depending on your data quality.
        # df['nav_date'] = df['nav_date'].dt.date  #Extract date only (removes time component)

        cols = tuple(df.columns)

        update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
        placeholders = ', '.join(['%s'] * len(cols))
        
        sql = f"""
            INSERT INTO "stg_fnc_nav" ({', '.join(cols)},createdt,updatedt)
            VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON CONFLICT (amc_code,fund_code,nav_date) DO UPDATE
            SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
        """

        logging.debug(f"SQL query: {sql}")

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
                
                cur.executemany(sql, data)
                conn.commit()
                logging.info(f"Upsert operation completed successfully.")

    except (psycopg2.Error, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logging.error(f"Database error: {e}")
        raise AirflowException(f"Database operation failed: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise AirflowException(f"An unexpected error occurred: {e}")

with DAG(
    'fnc_dw_NAV_evening',
    # start_date=days_ago(1),  #More robust
    start_date=datetime.now(),
    schedule_interval="0 17 * * 1-5",
    catchup=False,
    on_failure_callback=notify_teams,
    tags=['FundConnext'], #add tags for better organization

) as dag:

    task1 = PythonOperator(
        task_id='getToken_evening',
        python_callable=T_GetToken,
        do_xcom_push=True
    )

    task2 = PythonOperator(
        task_id='downloadFiles_evening',
        python_callable=T_DownloadFile,
        op_kwargs={'token': '{{ ti.xcom_pull(task_ids="getToken_evening") }}', 'fileType': fileType},
        on_failure_callback=notify_teams,
    )

    task3 = PythonOperator(
        task_id='pg_upsert_evening',
        python_callable=T_postgres_upsert_dataframe,
        op_kwargs={'fileName': '{{ ti.xcom_pull(task_ids="downloadFiles_evening") }}'},
        on_failure_callback=notify_teams,
    )

    task1 >> task2 >> task3

