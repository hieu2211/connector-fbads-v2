import base64
import requests
import pandas as pd
import time as time_s
from datetime import datetime, timedelta, time
from baseopensdk import BaseClient
from baseopensdk.api.base.v1 import *
from dotenv import load_dotenv, find_dotenv
import os
from playground.base_function import (create_bitable_field, get_bitable_fields,
                                      create_bitable_record)

# Load environment variables
load_dotenv(find_dotenv())

APP_TOKEN = os.environ['APP_TOKEN']
PERSONAL_BASE_TOKEN = os.environ['PERSONAL_BASE_TOKEN']
TABLE_ID = os.environ['TABLE_ID']

# Define necessary variables
yesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
last_2day = (datetime.today() - timedelta(2)).strftime('%Y-%m-%d')
today = datetime.today().strftime('%Y-%m-%d')
api_url = 'https://graph.facebook.com/v20.0/'
app_id = 'YOUR_APP_ID'  # Thay bằng App ID của bạn
app_secret = 'YOUR_APP_SECRET'  # Thay bằng App Secret của bạn
access_token = 'EAAJLi0uToY8BOxHYQVW0f8KZCTDZAKNXp2l8YHIt7ciyKeBR0ZCxexlfmKZA6izn8odmxkDl85nAaZALwMsKWhYZCv1dWtN8dFCQPkhRv0uqrhDMW0O1JFomeZBxNfWDKrAAxrDjnp6YgtmW1IgyOD7yz42B9ACLq1h3SopoZAgQZA5ZCB3lCplv1ZAIEwLZASxHMTX0MbKUG11UDNMeKtYZD'  # Thay bằng Access Token của bạn
list_account = ['527991475937136']  # Thay bằng danh sách các tài khoản của bạn


# Function to create a report and get the job_id
def create_report(account):
  url = api_url + 'act_' + account + '/insights'
  params = {
      "access_token": access_token,
      "level": "ad",
      "time_range":
      '{"since": "' + last_2day + '", "until": "' + yesterday + '"}',
      "time_increment": 1,
      "fields":
      "account_id,account_name,ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,impressions,spend,clicks,reach,actions",
      "export_format": "csv"  # Request CSV format
  }
  response = requests.post(url, params=params)
  data = response.json()
  job_id = data.get('report_run_id')  # Lấy job_id từ response
  return job_id


# Function to check report status
def check_report_status(job_id):
  url = api_url + job_id
  params = {"access_token": access_token}
  response = requests.get(url, params=params)
  return response.json()


# Wait until the report is ready
def wait_for_report(job_id, interval=10, max_attempts=10):
  for attempt in range(max_attempts):
    status = check_report_status(job_id)
    if status.get('async_status') == 'Job Completed':
      print("Report is ready.")
      return True
    else:
      print(f"Waiting for report... Attempt {attempt + 1}/{max_attempts}")
      time_s.sleep(
          interval)  # Wait for the specified interval before checking again
  print("Report did not complete in time.")
  return False


# Function to download the CSV and convert it to a dataframe
def csv_to_df(job_id):
  csv_url = "https://www.facebook.com/ads/ads_insights/export_report?report_run_id=" + job_id + "&format=csv&access_token=" + access_token + "&locale=en_US"
  df = pd.read_csv(csv_url,
                   encoding='utf-8',
                   delimiter=',',
                   encoding_errors='ignore',
                   on_bad_lines='skip')
  # Clean column names
  df.columns = df.columns.str.replace('[#,@,&,:,.,-/,>,<,(,), ]',
                                      '_',
                                      regex=True)
  df.columns = df.columns.str.lower()

  # Create a new dataframe with selected columns
  dft = pd.DataFrame(columns=[
      "reporting_starts", "reporting_ends", "account_id", "account_name",
      "ad_id", "ad_name", "campaign_id", "campaign_name", "ad_set_id",
      "ad_set_name", "impressions", "amount_spent__vnd_", "clicks__all_",
      "reach", "link_clicks", "3_second_video_plays", "post_comments",
      "post_shares", "post_reactions", "messaging_conversations_started",
      "new_messaging_contacts", "blocked_messaging_contacts", "purchases"
  ])
  # Fill new dataframe with matching columns from the CSV data
  for col in dft.columns:
    if col in df.columns:
      dft[col] = df[col]
  return dft


# Main function
def mainfnc():
  # Build Lark client
  client: BaseClient = BaseClient.builder() \
      .app_token(APP_TOKEN) \
      .personal_base_token(PERSONAL_BASE_TOKEN) \
      .build()

  # Lấy danh sách các fields hiện có trong Bitable
  bitable_fields = get_bitable_fields(client, TABLE_ID)
  if not bitable_fields:
    print("Could not retrieve Bitable fields. Exiting.")
    return

  for account in list_account:
    try:
      job_id = create_report(account)  # Get job_id from API
      print(f"Report run ID for account {account}: {job_id}")
    except Exception as e:
      print(f"Failed to create report for account {account}. Error: {e}")
      continue

    # Wait for the report to be ready
    if wait_for_report(job_id):
      try:
        df = csv_to_df(job_id)  # Download CSV and convert to dataframe
      except Exception as e:
        print(f"Failed to retrieve CSV data for account {account}. Error: {e}")
        continue

      if df is None or df.empty:
        print(f"No data retrieved for account {account}.")
        continue
      print(df.head())  # Display first 5 rows of the dataframe

      # Lấy danh sách tên cột từ DataFrame
      df_columns = df.columns.tolist()

      # Kiểm tra và tạo fields trong Bitable nếu chúng chưa tồn tại
      for df_col in df_columns:
        if df_col not in bitable_fields:
          # Tạo field trong Bitable
          new_field = create_bitable_field(client, TABLE_ID, df_col)
          if new_field:
            bitable_fields[df_col] = new_field
        else:
          print(f"Field '{df_col}' already exists in Bitable.")

      # Chèn dữ liệu vào Bitable
      for index, row in df.iterrows():
        create_bitable_record(client, TABLE_ID, row, df_columns,
                              bitable_fields)
    else:
      print(f"Report for account {account} did not complete.")


# Run main function
if __name__ == "__main__":
  mainfnc()
