import requests
import tempfile
import rrdtool
import pandas as pd
import numpy as np
import datetime
import gspread
import json
import os
import time
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import GoogleDriveFile

SERVICE_ACCOUNT = {
  'type': os.environ['TYPE'],
  'project_id': os.environ["PROJECT_ID"],
  'private_key_id': os.environ['PRIVATE_KEY_ID'],
  'private_key': os.environ['PRIVATE_KEY'].replace('\\n', '\n'), # Need to do because os.environ escapes the slash
  'client_email': os.environ['CLIENT_EMAIL'],
  'client_id': os.environ['CLIENT_ID'],
  'auth_uri': os.environ['AUTH_URI'],
  'token_uri': os.environ['TOKEN_URI'],
  'auth_provider_x509_cert_url': os.environ['AUTH_PROVIDER_X509_CERT_URL'],
  'client_x509_cert_url': os.environ['CLIENT_X509_CERT_URL'],
  'universe_domain': os.environ['UNIVERSE_DOMAIN']
}

def process_worksheet(old_worksheet_id, new_worksheet_id):
  gc = gspread.service_account_from_dict(SERVICE_ACCOUNT)
  
  print('Updating old spreadsheet')
  old_worksheet = gc.open_by_key(old_worksheet_id).sheet1
  old_worksheet.update_acell('A1', 'ARCHIVED VERSION - DO NOT EDIT')

  worksheet = gc.open_by_key(new_worksheet_id).sheet1

  # Column J is the name of the entry in the factory
  cells = worksheet.get('J2:J')

  s = requests.Session()
  adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1)
  s.mount('http://', adapter)

  for idx, cell in enumerate(cells):
    if len(cell) == 0 or cell[0] == '':
        continue

    entry = cell[0]
    url = f'http://gfactory-2.opensciencegrid.org/factory/monitor/entry_{entry}/total/Status_Attributes.rrd'
    ferm_url = f'http://fermifactory02.fnal.gov:8319/factory/monitor/entry_{entry}/total/Status_Attributes.rrd'
    response = s.get(url)
    if response.status_code != 200:
      response = s.get(ferm_url)
    if response.status_code != 200:
      print(f'Error {entry}: {response.status_code}')
      continue
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(response.content)
    tmp_file.close()

    # Now read in the rrd
    info = rrdtool.info(tmp_file.name)

    # Cores at collector are ClientCoresTotal
    # Requested idle glideins are ReqIdle
    result = rrdtool.fetch(tmp_file.name, "AVERAGE")

    date_range = pd.date_range(pd.to_datetime(result[0][0], unit='s', origin='unix'), pd.to_datetime(result[0][1], unit='s', origin='unix'), freq='5min')[:-1]

    df = pd.DataFrame(np.array(result[2]), columns=result[1], index=date_range)
    df = df.fillna(0)

    # Get the average of the last 4 hours for the ClientCoresTotal column
    # RRD is every 5 minutes, so 12 entries an hour, 48 hours is 576 entries
    client_cores_avg = df['ClientCoresTotal'].tail(576).mean()
    req_idle_avg = df['ReqIdle'].tail(576).mean()
    print(f'Entry {entry} has an average of {client_cores_avg} client cores and {req_idle_avg} requested idle glideins.')

    new_value = 'Unknown'
    if client_cores_avg > 1:
      new_value = 'Production'
    elif req_idle_avg < 1:
      new_value = 'No pressure'
    else:
      new_value = 'Broken'

    status_cell = f'C{idx + 2}'
    current_value = worksheet.acell(status_cell).value
    
    if current_value not in ['Production', 'Broken', 'No pressure']:
      print(f'Entry {entry} has an invalid value of {current_value}')
    else:
      if current_value == new_value:
        print(f'Entry {entry} has not changed')

      print(f'Changing entry {entry} status from {current_value} to {new_value}')
      worksheet.update_acell(status_cell, new_value)

      print(f'Changing entry {entry} last status to {current_value}')
      last_status_cell = f'D{idx + 2}'
      worksheet.update_acell(last_status_cell, current_value)

    # We're sleeping so we don't overload the api and get rate limited
    time.sleep(10)

def create_worksheet():
  gauth = GoogleAuth(settings= {
    'client_config_backend': 'service',
    'service_config': {
        'client_json_dict': SERVICE_ACCOUNT
    }
  })
  gauth.ServiceAuth()
  drive = GoogleDrive(gauth)

  today = datetime.date.today()
  next_monday = today + datetime.timedelta(days=7-today.weekday())

  old_spreadsheet_id = None
  old_spreadsheet = None
  new_spreadsheet_id = None
  new_folder = None

  TEAM_DRIVE_ID = '0ABb7rkiXRlOpUk9PVA'
  HOSTED_CE_FOLDER_ID = '1-7ae75NshCbznXywFduBPeHDhaTRiMDO'
  hosted_ce_parent = drive.ListFile({'q':f"'{HOSTED_CE_FOLDER_ID}' in parents and trashed=false", 'corpora': 'teamDrive', 'teamDriveId': TEAM_DRIVE_ID, 'includeTeamDriveItems': True, 'supportsTeamDrives': True}).GetList()

  for drive_item in hosted_ce_parent:
    if f'{today.year} CE Status Weekly Reports' in drive_item['title']:
      old_folder_items = drive.ListFile({'q':f"'{drive_item['id']}' in parents and trashed=false", 'corpora': 'teamDrive', 'teamDriveId': TEAM_DRIVE_ID, 'includeTeamDriveItems': True, 'supportsTeamDrives': True}).GetList()
  for old_folder_item in old_folder_items:
    if f'OSPool CE Status - {today.strftime("%Y-%m-%d")}' in old_folder_item['title']:
      old_spreadsheet_id = old_folder_item['id']
      old_spreadsheet = GoogleDriveFile(auth=gauth, metadata=old_folder_item, uploaded=True)
  
  for drive_item in hosted_ce_parent:
    if f'{next_monday.year} CE Status Weekly Reports' in drive_item['title']:
      new_folder_items = drive.ListFile({'q':f"'{drive_item['id']}' in parents and trashed=false", 'corpora': 'teamDrive', 'teamDriveId': TEAM_DRIVE_ID, 'includeTeamDriveItems': True, 'supportsTeamDrives': True}).GetList()
      new_folder = GoogleDriveFile(auth=gauth, metadata=drive_item, uploaded=True)
  for new_folder_item in new_folder_items:
    if f'OSPool CE Status - {next_monday.strftime("%Y-%m-%d")}' in new_folder_item['title']:
      new_spreadsheet_id = new_folder_item['id'] 

  new_spreadsheet_id = new_spreadsheet_id or old_spreadsheet.Copy(target_folder=new_folder, new_title=f'OSPool CE Status - {next_monday.strftime("%Y-%m-%d")}').get('id')
  return old_spreadsheet_id, new_spreadsheet_id

def main():
  old_spreadsheet_id, new_spreadsheet_id = create_worksheet()
  process_worksheet(old_spreadsheet_id, new_spreadsheet_id)

if __name__ == "__main__":
  main()
