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

# get dag directory path (this might not be needed if you use Airflow's standard DAG folder structure)
dag_path = os.getcwd()

fileType = "FundProfile"
rawDataPath = Path(f'{dag_path}/data/raw_data/fnc')
processDataPath = Path(f'{dag_path}/data/processed_data/fnc')
extract_path = rawDataPath

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def notify_teams(context: dict):
    logging.info("** notify_teams.")


def T_GetToken():
    logging.info(f"getToken()")
    api_url = Variable.get("FC_API_URL") + "/api/auth"
    data = {
        "username": Variable.get("FC_API_USER"),
        "password": Variable.get("FC_API_PASSPOWRD")
    }
    try:
        response = requests.post(api_url, json=data)
        response.raise_for_status()
        response_data = response.json()
        token = response_data.get('access_token')
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting token: {e}")
        raise AirflowException(f"Failed to get token: {e}")


def T_DownloadFile(token, fileType):
    logging.info(f"downloadFile fileType:{fileType}")
    download_file_path = f"{rawDataPath}/{fileType}.zip"
    businessDate = datetime.now().strftime("%Y%m%d")  # Use current date for robustness

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
            "lmts_notice_perios_perc_aum",
            "lmts_adls_amount",
            "lmts_adls_perc_aum",
            "lmts_liquidity_fee_amount",
            "lmts_liquidity_fee_perc_aum",
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

        df.fillna("", inplace=True)
        df.drop_duplicates(inplace=True)

        cols = tuple(df.columns)
        
        # update_set_clause = ", ".join([f'"stg_fnc_fundProfile".{col} = EXCLUDED.{col}' for col in cols[1:]])
        update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])

        placeholders = ', '.join(['%s'] * len(cols))
        
        # sql = f"""
        #     INSERT INTO "stg_fnc_fundProfile" ({', '.join(cols)},createdDT, updateDT)
        #     VALUES ({placeholders},CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        #     ON CONFLICT (fund_code) DO UPDATE
        #     SET ({', '.join(cols[1:])}) = (EXCLUDED.{', '.join(cols[1:])}), updateDT = CURRENT_TIMESTAMP;
        # """

        sql = f"""
            INSERT INTO "stg_fnc_fundProfile" ({', '.join(cols)}, createdDT, updateDT)
            VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (fund_code) DO UPDATE
            SET {update_set_clause}, updateDT = CURRENT_TIMESTAMP;
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
    'fnc_fundprofile_dag_v2',
    start_date=days_ago(1),  #More robust
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    on_failure_callback=notify_teams,
) as dag:

    task1 = PythonOperator(
        task_id='getToken',
        python_callable=T_GetToken,
        do_xcom_push=True
    )

    task2 = PythonOperator(
        task_id='downloadFiles',
        python_callable=T_DownloadFile,
        op_kwargs={'token': '{{ ti.xcom_pull(task_ids="getToken") }}', 'fileType': fileType},
        on_failure_callback=notify_teams,
    )

    task3 = PythonOperator(
        task_id='postgres_upsert',
        python_callable=T_postgres_upsert_dataframe,
        op_kwargs={'fileName': '{{ ti.xcom_pull(task_ids="downloadFiles") }}'},
        on_failure_callback=notify_teams,
    )

    task1 >> task2 >> task3

