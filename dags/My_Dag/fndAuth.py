import sys
import requests
import zipfile
import os
from datetime import datetime
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago



def T_GetToken():
  print("** getToken.")
  
  try:

    api_url = os.environ["FC_API_URL"]+"/auth"
    data = {
      "username": os.environ["FC_API_USER"],
      "password": os.environ["FC_API_PASSPOWRD"]
    }

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
  print(f"-token: {token}")

  try:
    url = os.environ["FC_API_URL"]+f"/files/{businessDate}/{fileType}.zip" 
    headers = {
    "X-Auth-Token": token,
    "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

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
        print(f"Content-Disposition: {content_disposition}")
        print(f"Filename: {filename}") 

    # Exptrtact zip file
    extract_path = "./data/raw_data"
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


# Example usage:
fileType = "FundProfile"  
rawDataPpath = "./data/raw_data"
token=""

# Test 
# token=T_GetToken(api_url, data)
# T_DownloadFile(token,fileType)


# initializing the default arguments that we'll pass to our DAG
with DAG(
        'fundConnext_FundProfile_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval="* 8 * * 1-5", 
        catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=T_GetToken,
        dag=dag,
        do_xcom_push=True 
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=T_DownloadFile,
        op_kwargs={'token': '{{ ti.xcom_pull(task_ids="task1") }}', 'fileType': fileType},
        dag=dag
    )

    task1 >> task2 