import requests
import zipfile

# Example usage:
api_url = "" 
data = {
  "username": "",
  "password": ""
}

def getToken(url, data):
  
  try:
    response = requests.post(url, json=data)

    response.raise_for_status()  # Raise an exception for bad status codes
    response_data = response.json()
    return response_data.get('access_token') 
  
    # return response
  except requests.exceptions.RequestException as e:
    print(f"Error making POST request: {e}")
    return None


def get_downloadRequest(token):
  
  print("TOKEN>> %s",token)

  try:
    url = "https://stage.fundconnext.com/api/files/20250207/FundProfile.zip" 
    headers = {
    "X-Auth-Token": token,
    "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

    with open(download_file_path, 'wb') as f:
      for chunk in response.iter_content(chunk_size=8192): 
        f.write(chunk)

    return response
  except requests.exceptions.RequestException as e:
    print(f"Error making GET request: {e}")
    return None

def extract_zip(zip_file_path, extract_path):

  try:
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
      zip_ref.extractall(extract_path)
    return True
  except Exception as e:
    print(f"Error extracting ZIP file: {e}")
    return False
  
download_file_path = "./raw_data/downloaded_file.zip" 

token = getToken(api_url, data)
response = get_downloadRequest(token)

headers = response.headers
if headers:
  content_type = headers.get('Content-Type')
  content_disposition = headers.get('Content-Disposition')

  if content_type == 'application/zip':
    print(f"Content-Type: {content_type}")

    if content_disposition:
      filename = content_disposition.split(';')[1].strip().split('=')[1].strip('"') 
      print(f"Content-Disposition: {content_disposition}")
      print(f"Filename: {filename}") 

# Example Usage:
zip_file_path = download_file_path
extract_path = "./raw_data"

if extract_zip(zip_file_path, extract_path):
  print(f"ZIP file extracted successfully to: {extract_path}")
else:
  print("Failed to extract ZIP file.")
