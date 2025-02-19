import sys
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
from My_Dag.ms_teams_operator import MSTeamsPowerAutomateWebhookOperator


# get dag directory path
dag_path = os.getcwd()

fileType = "FundProfile"  
rawDataPpath = Path(f'{dag_path}/data/raw_data/fnc')
extract_path = rawDataPpath
# token=""

def notify_teams(context: dict):
  print("** notify_teams.")

  # op1 = MSTeamsPowerAutomateWebhookOperator(
  #       task_id="send_to_teams",
  #       http_conn_id="teams_webhook_conn",
  #       heading_title="Airflow local",
  #       header_bar_style="good",
  #       heading_subtitle='heading_subtitle',
  #       card_width_full=False,
  #       body_message="""Dag **lorem_ipsum** has completed successfully in **localhost**""",
  #       body_facts_dict={"Lorems": "184", "Dolor": "Sat", "Time taken": "2h 30m"},
  #       button_text="View logs",
  #   )


def T_GetToken():
  print("** getToken.")
  
  try:

    api_url = Variable.get("FC_API_URL")+"/api/auth"
    data = {
      "username": Variable.get("FC_API_USER"),
      "password": Variable.get("FC_API_PASSPOWRD")
    }

    print(f"FC_API_URL: {api_url}")

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
    print(f"Error making POST request: {e}")
    sys.exit(1)
    # return None


def T_DownloadFile(token,fileType):
  
  print("** downloadFile.")

  download_file_path = f"{rawDataPpath}/{fileType}.zip"

  businessDate = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
  businessDate = datetime.now().strftime("%Y%m%d")

  print(f"-businessDate: {businessDate}")
  print(f"-fileType: {fileType}")
  # print(f"-token: {token}")

  try:
    # url = os.environ["FC_API_URL"]+f"/files/{businessDate}/{fileType}.zip" 
    url = Variable.get("FC_API_URL")+f"/api/files/{businessDate}/{fileType}.zip" 
    headers = {
    "X-Auth-Token": token,
    "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

    if not os.path.exists(download_file_path):
      os.makedirs(os.path.dirname(download_file_path)) 

    
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
      return True
    except Exception as e:
      print(f"Error extracting ZIP file: {e}")
      sys.exit(1)


    return download_file_path
  except requests.exceptions.RequestException as e:
    print(f"Error download file and save: {e}")
    sys.exit(1)



# Test 
# token=T_GetToken(api_url, data)
# T_DownloadFile(token,fileType)


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

    task1 >> task2 