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
from psycopg2 import sql
import logging
import json

from My_Dag.utils.fundconnext_util import getCrsCols,getSuitCols,getAddressCols,replace_value_in_tuple_comp, getFundConnextToken, getCustomerINDCols, getAccountCols, getUnitholderCols, getBankAccountCols

# get dag directory path (this might not be needed if you use Airflow's standard DAG folder structure)
dag_path = os.getcwd()

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


def T_custProfile_db(fileName):
    # fileName = f"{rawDataPath}/{fileName}"
    fileName = f"{rawDataPath}/{fileName}"


    logging.info(f"T_custProfile_db. {fileName}")

    person_list = []
    account_list = []
    unitholder_list = []
    account_bank_list =[]
    person_addr_list=[]
    suitabilityForm =[]
    crsDetails = []

    try:
        with open(fileName, "r") as json_file:
            data = json.load(json_file)

            # Handle different JSON structures
            if isinstance(data, list):
                for item in data:
                    cardNumber = item.get("cardNumber")  # Get account number
                    accounts = item.get("accounts")
                    
                    if accounts:
                        bankList =[]
                        for account in accounts:
                            account_no = account.get("accountId")  # Get account number
                            unitholders = account.get("unitholders")
                            
                            if account_no and unitholders:
                                for unitholder in unitholders:
                                    unitholder["accountId"]=account_no

                                unitholder_list.extend(unitholders)

                            # ACCOUNT & BANK (SUB,RED)
                            if "redemptionBankAccounts" in account:
                                bank_red=(account.get("redemptionBankAccounts"))
                                if bank_red:
                                    bank_red[0]["Purpose"]='Subscription'
                                    bank_red[0]["accountId"]=account_no
                                    
                            if "subscriptionBankAccounts" in account:
                                bank_sub=(account.get("subscriptionBankAccounts"))
                                if bank_sub:
                                    bank_sub[0]["Purpose"]='Redemption'
                                    bank_sub[0]["accountId"]=account_no
                                
                            bankList.extend(bank_red)
                            bankList.extend(bank_sub)
                                
                            del account["unitholders"]  # Remove unitholders from account data
                            
                        account_bank_list.extend(bankList)

                     # GET PERSON ADDRESS 
                    addr_id ={}
                    addr_current ={}
                    addr_work ={}
                    if "identificationDocument" in item:
                        addr_id=(item.get("identificationDocument"))
                        addr_id["cardNumber"]=cardNumber
                        addr_id["addrType"]="idcard"
                    if "current" in item:
                        addr_current=(item.get("current"))
                        addr_current["cardNumber"]=cardNumber
                        addr_current["addrType"]="current"
                    if "work" in item:
                        addr_work=(item.get("work"))
                        addr_work["cardNumber"]=cardNumber
                        addr_work["addrType"]="work"

                    if addr_id:
                        person_addr_list.append(addr_id)
                    if addr_current:
                        person_addr_list.append(addr_current)
                    if addr_work:
                        person_addr_list.append(addr_work)

                    # SUITABILITY
                    if "suitabilityForm" in item:
                        suitData=item.get("suitabilityForm")
                        suitData["cardNumber"]=cardNumber
                        suitData["suitabilityEvaluationDate"]=item.get("suitabilityEvaluationDate")
                        suitData["suitabilityRiskLevel"]=item.get("suitabilityRiskLevel")

                        suitabilityForm.append(suitData)

                    # CRS
                    if "crsDetails" in item:
                        for crs in item.get("crsDetails"):
                            crsData=crs
                            crsData["cardNumber"]=cardNumber
                            crsData["placeOfBirthCountry"]=item.get("crsPlaceOfBirthCountry")
                            crsData["placeOfBirthCity"]=item.get("crsPlaceOfBirthCity")
                            crsData["taxResidenceInCountriesOtherThanTheUS"]=item.get("crsTaxResidenceInCountriesOtherThanTheUS")
                            crsData["declarationDate"]=item.get("crsDeclarationDate")
                            crsDetails.append(crsData)
                    
                    if "accounts" in item:
                        del item["accounts"]
                    
                    person_list.append(item)
                    account_list.extend(accounts)
                    
            else:
                logging.info("Unsupported JSON structure.")


        # Connect to PostgreSQL
        # Create fundction UPSERT 1. person_list
        # 2. account_list
        # 3. unitholder_list  finished
        # 4. account_bank_list 
        # 5. person_addr_list
        # 6. suitabilityForm
        # 7. crsDetails
        customerProfile_db(person_list,account_list,unitholder_list,account_bank_list,person_addr_list,suitabilityForm,crsDetails)

    except FileNotFoundError:
        logging.error(f"Error: File {fileName} not found.")
        raise AirflowException(f"file {fileName} not found ")
    except json.JSONDecodeError:
        logging.error(f"Error: Invalid JSON format in {fileName}.")
        raise AirflowException(f"Error: Invalid JSON format in {fileName}.")
    except KeyError as e:
        logging.error(f"Error: Missing key in JSON data: {e}")
        raise AirflowException(f"Error: Missing key in JSON data: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise AirflowException(f"An unexpected error occurred: {e}")



# def customerProfile_db(person_list, account_list, unitholder_list, account_bank_list, person_addr_list, suitabilityForm, crsDetails):
def customerProfile_db(person_list,account_list = None,unitholder_list = None,account_bank_list = None,person_addr_list = None,suitabilityForm = None,crsDetails = None):

    # Database credentials (GET FROM AIRFLOW VARIABLES)
    conn_params = {
            "host": Variable.get("POSTGRES_FCN_HOST"),
            "database": Variable.get("POSTGRES_FCN_DB"),
            "user": Variable.get("POSTGRES_FCN_USER"),
            "password": Variable.get("POSTGRES_FCN_PWD"),
            "port": Variable.get("POSTGRES_FCN_PORT")
        }

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # --- UPSERT FUNCTIONS (Adapt to your table names and column names) ---
        def upsert_person(data):
            # Assuming a 'persons' table with columns: cardNumber, ..., etc.
            colsList = getCustomerINDCols()
            cols = tuple(colsList)
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
            placeholders = ', '.join(['%s'] * len(cols))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_customerInd ({', '.join(cols)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (cardNumber) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_account(data):
            colsList = getAccountCols()
            cols = tuple(colsList)
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
            placeholders = ', '.join(['%s'] * len(cols))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_account ({', '.join(cols)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (cardNumber,accountId) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_unitholder(data):
            colsList = getUnitholderCols()
            cols = tuple(colsList)
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
            placeholders = ', '.join(['%s'] * len(cols))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_unitholder ({', '.join(cols)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (unitholderId,accountId) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_bankaccount(data):
            colsList = getBankAccountCols()
            cols = tuple(colsList)
            db_columns = replace_value_in_tuple_comp(cols,"default","isDefault")  #Convert db column name
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in db_columns[1:]])
            placeholders = ', '.join(['%s'] * len(db_columns))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_bankaccount ({', '.join(db_columns)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (accountId, bankCode, bankAccountNo, Purpose) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_address(data):
            colsList = getAddressCols()
            cols = tuple(colsList)
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
            placeholders = ', '.join(['%s'] * len(cols))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_custaddress ({', '.join(cols)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (cardNumber, addrType) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_suit(data):
            colsList = getSuitCols()
            cols = tuple(colsList)
            db_columns = replace_value_in_tuple_comp(cols,"suitabilityEvaluationDate","evaluationDate")  #Convert db column name
            db_columns = replace_value_in_tuple_comp(db_columns,"suitabilityRiskLevel","riskLevel")  #Convert db column name
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in db_columns[1:]])
            placeholders = ', '.join(['%s'] * len(db_columns))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_suitability ({', '.join(db_columns)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (cardNumber, evaluationDate) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)

        def upsert_crs(data):
            colsList = getCrsCols()
            cols = tuple(colsList)
            update_set_clause = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols[1:]])
            placeholders = ', '.join(['%s'] * len(cols))

            query = sql.SQL(f"""
                INSERT INTO stg_fnc_crs ({', '.join(cols)},createdt,updatedt) 
                VALUES ({placeholders},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                ON CONFLICT (cardNumber, tin) DO UPDATE 
                SET {update_set_clause}, updatedt = CURRENT_TIMESTAMP;
            """)

            # GET data from data dictionary    
            data_list=[]
            for col in colsList:
                data_list.append(data.get(col))

            cur.execute(query, data_list)


        # --- UPSERT IND PROFILE DATA ---
        for person in person_list:
            upsert_person(person)

        # --- UPSERT ACCOUNT DATA ---
        for account in account_list:
            upsert_account(account)

        # --- UPSERT ACCOUNT DATA ---
        for unitholder in unitholder_list:
            upsert_unitholder(unitholder)

        for bankaccount in account_bank_list:
            upsert_bankaccount(bankaccount)

        for addr in person_addr_list:
            upsert_address(addr)

        for suit in suitabilityForm:
            upsert_suit(suit)

        for crs in crsDetails:
            upsert_crs(crs)

        conn.commit()
        logging.info("Data upserted to PostgreSQL successfully.")

    except psycopg2.Error as e:
        conn.rollback()  # Rollback changes in case of error
        logging.error(f"PostgreSQL error: {e}")
        raise AirflowException(f"PostgreSQL error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise AirflowException(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

with DAG(
    'fnc_CustomerProfileIND',
    # start_date=days_ago(1),  #More robust
    start_date=datetime.now(),
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    on_failure_callback=notify_teams,
) as dag:

    # task1 = PythonOperator(
    #     task_id='getToken',
    #     python_callable=T_GetToken,
    #     do_xcom_push=True
    # )

    # task2 = PythonOperator(
    #     task_id='downloadFiles',
    #     python_callable=T_DownloadFile,
    #     op_kwargs={'token': '{{ ti.xcom_pull(task_ids="getToken") }}', 'fileType': 'CustomerProfile'},
    #     on_failure_callback=notify_teams,
    # )

    # task3 = PythonOperator(
    #     task_id='postgres_upsert',
    #     python_callable=T_custProfile_db,
    #     op_kwargs={'fileName': '{{ ti.xcom_pull(task_ids="downloadFiles") }}'},
    #     on_failure_callback=notify_teams,
    # )

    task3 = PythonOperator(
        task_id='postgres_upsert',
        python_callable=T_custProfile_db,
        op_kwargs={'fileName': '20250916_MPS_INDIVIDUAL.json'},
        on_failure_callback=notify_teams,
    )

    task3

    # task1 >> task2 >> task3
    

