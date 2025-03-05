

import requests
import logging
from airflow.models import Variable


def getFundConnextToken():
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