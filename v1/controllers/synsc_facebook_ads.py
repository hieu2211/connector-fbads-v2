import base64
import requests
import pandas as pd
import time as time_s
from datetime import datetime, timedelta, time
from baseopensdk import BaseClient
from baseopensdk.api.base.v1 import *
from dotenv import load_dotenv, find_dotenv
import requests
import json
import os
import uuid
from flask import jsonify
from flask import request
import redis
from v1.controllers.base_function import (create_bitable_field,
                                          get_bitable_fields,
                                          create_bitable_record, batch_create_bitable_records)

r = redis.StrictRedis(host='103.75.180.83', port=6379, password='******', decode_responses=True)


# Load environment variables
load_dotenv(find_dotenv())

# Lấy ngày hiện tại bằng datetime
today = datetime.today().strftime('%Y-%m-%d')

# Chuyển đổi today thành đối tượng Timestamp của pandas
today_pd = pd.to_datetime(today)

# Lấy ngày đầu tiên của tháng cách ngày hôm nay 2 tháng bằng pandas
first_day_last_2months = (today_pd - pd.DateOffset(months=1)).replace(day=1)

# Chuyển ngày đầu tiên thành chuỗi
first_day_last_2months_str = first_day_last_2months.strftime('%Y-%m-%d')

# Khởi tạo biến start_date và end_date
# end_date = first_day_last_2months_str
# start_date = today

api_url = 'https://graph.facebook.com/v20.0/'
# access_token = ''  # Thay bằng Access Token của bạn
# list_account = []  # Thay bằng danh sách các tài khoản của bạn
fields = "date_start,account_name,account_currency,account_id,campaign_name,adset_name,ad_name,campaign_id,adset_id,ad_id,objective,reach,impressions,frequency,spend,clicks,website_ctr,cpc,ctr,cpm,cpp,video_thruplay_watched_actions,video_30_sec_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_play_actions,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,engagement_rate_ranking,estimated_ad_recallers,conversion_rate_ranking,conversions,conversion_values,actions"
level_ads = ''

fb_to_bitable_mapping = {
    "reporting_starts": {
        "name": "Reporting starts",
        "type": 5,
        "id": "fbads_1"
    },  # date
    "account_name": {
        "name": "Account name",
        "type": 1,
        "id": "fbads_2"
    },  # text
    "currency": {
        "name": "Currency",
        "type": 1,
        "id": "fbads_3"
    },  # text
    "account_id": {
        "name": "Account ID",
        "type": 1,
        "id": "fbads_4"
    },  # text
    "campaign_name": {
        "name": "Campaign name",
        "type": 1,
        "id": "fbads_5"
    },  # text
    "ad_set_name": {
        "name": "Ad Set Name",
        "type": 1,
        "id": "fbads_6"
    },  # text
    "ad_name": {
        "name": "Ad name",
        "type": 1,
        "id": "fbads_7"
    },  # text
    "campaign_id": {
        "name": "Campaign ID",
        "type": 1,
        "id": "fbads_8"
    },  # text
    "ad_set_id": {
        "name": "Ad set ID",
        "type": 1,
        "id": "fbads_9"
    },  # text
    "ad_id": {
        "name": "Ad ID",
        "type": 1,
        "id": "fbads_10"
    },  # text
    "objective": {
        "name": "Objective",
        "type": 1,
        "id": "fbads_11"
    },  # text
    "reach": {
        "name": "Reach",
        "type": 2,
        "id": "fbads_12"
    },  # number
    "impressions": {
        "name": "Impressions",
        "type": 2,
        "id": "fbads_13"
    },  # number
    "frequency": {
        "name": "Frequency",
        "type": 2,
        "id": "fbads_14"
    },  # number
    "amount_spent__vnd_": {
        "name": "Amount spent",
        "type": 2,
        "id": "fbads_15"
    },  # number
    "clicks__all_": {
        "name": "Clicks (all)",
        "type": 2,
        "id": "fbads_16"
    },  # number
    "ctr__link_click_through_rate_": {
        "name": "CTR (link click-through rate)",
        "type": 2,
        "id": "fbads_34"
    },  # number
    "cpc__all___vnd_": {
        "name": "CPC (All)",
        "type": 2,
        "id": "fbads_17"
    },  # number
    "ctr__all_": {
        "name": "CTR (all)",
        "type": 2,
        "id": "fbads_18"
    },  # number
    "cpm__cost_per_1_000_impressions___vnd_": {
        "name": "CPM",
        "type": 2,
        "id": "fbads_19"
    },  # number
    "cost_per_1_000_accounts_center_accounts_reached__vnd_": {
        "name": "Cost per 1,000 people reached",
        "type": 2,
        "id": "fbads_20"
    },  # number
    "thruplays": {
        "name": "ThruPlays",
        "type": 2,
        "id": "fbads_21"
    },
    "estimated_ad_recall_lift__people_": {
        "name": "Estimated ad recall lift (people)",
        "type": 2,
        "id": "fbads_22"
    },
    "post_comments": {
        "name": "Post comments",
        "type": 2,
        "id": "fbads_23"
    },  # number
    "link_clicks": {
        "name": "Link clicks",
        "type": 2,
        "id": "fbads_24"
    },  # number
    "post_shares": {
        "name": "Post shares",
        "type": 2,
        "id": "fbads_25"
    },  # number
    "post_reactions": {
        "name": "Post reactions",
        "type": 2,
        "id": "fbads_26"
    },
    "3_second_video_plays": {
        "name": "3-second video plays",
        "type": 2,
        "id": "fbads_27"
    },  # number
    "page_engagement": {
        "name": "Page engagement",
        "type": 2,
        "id": "fbads_28"
    },  # number
    "post_engagements": {
        "name": "Post engagement",
        "type": 2,
        "id": "fbads_29"
    },  # number
    "leads": {
        "name": "Leads",
        "type": 2,
        "id": "fbads_30"
    },
    "website_content_views": {
        "name": "Content views",
        "type": 2,
        "id": "fbads_31"
    },
    "messaging_conversations_started": {
        "name": "Messaging Conversations Started",
        "type": 2,
        "id": "fbads_32"
    },  # number
    "new_messaging_contacts": {
        "name": "New messaging connections",
        "type": 2,
        "id": "fbads_33"
    }  # number
}

# Function to create a report and get the job_idx
def create_report(USER_TOKEN, category):
    
    access_token = r.get(f"{USER_TOKEN}_accesstoken")
    account = r.get(f"{USER_TOKEN}_account")
    level_ads = r.get(f"{USER_TOKEN}_level")
    
    end_date = today
    start_date = first_day_last_2months_str
    
    if category == "table":
        start_date = today
        end_date = today

    url = api_url + 'act_' + account + '/insights'
    params = {
        "access_token": access_token,
        "level": level_ads,
        "time_range":
        '{"since": "' + start_date + '", "until": "' + end_date + '"}',
        "time_increment": 1,
        "fields": fields,
        "export_format": "csv"  # Request CSV format
    }

    response = requests.post(url, params=params)
    data = response.json()
    job_id = data.get('report_run_id')  # Lấy job_id từ response
    return job_id


# Function to check report status
def check_report_status(job_id):
    USER_TOKEN = json.loads(request.json.get('context',
                                             {})).get('scriptArgs',
                                                      {}).get('baseOpenID')
    access_token = r.get(f"{USER_TOKEN}_accesstoken")
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
            print(
                f"Waiting for report... Attempt {attempt + 1}/{max_attempts}")
            time_s.sleep(
                interval
            )  # Wait for the specified interval before checking again
    print("Report did not complete in time.")
    return False


# Function to download the CSV and convert it to a dataframe
def csv_to_df(job_id):
    USER_TOKEN = json.loads(request.json.get('context',
                                             {})).get('scriptArgs',
                                                      {}).get('baseOpenID')
    access_token = r.get(f"{USER_TOKEN}_accesstoken")
    # print("job_id: ", job_id)
    
    csv_url = "https://www.facebook.com/ads/ads_insights/export_report?report_run_id=" + job_id + "&format=csv&access_token=" + access_token + "&locale=en_US"
    # print("csv_url: ", csv_url)
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

    dft = pd.DataFrame(columns=[
        "reporting_starts", "account_name", "currency", "account_id",
        "campaign_name", "ad_set_name", "ad_name", "campaign_id", "ad_set_id",
        "ad_id", "objective", "reach", "impressions", "frequency",
        "amount_spent__vnd_", "clicks__all_", "ctr__link_click_through_rate_",
        "cpc__all___vnd_", "ctr__all_",
        "cpm__cost_per_1_000_impressions___vnd_",
        "cost_per_1_000_accounts_center_accounts_reached__vnd_", "thruplays",
        "post_comments", "link_clicks", "post_shares", "post_reactions",
        "3_second_video_plays", "page_engagement", "post_engagements", "leads",
        "onsite_conversion.purchase", "website_content_views",
        "messaging_conversations_started", "messaging_first_reply",
        "mobile_app_install", "app_use", "app_install",
        "app_custom_event.fb_mobile_achievement_unlocked"
    ])
    # Fill new dataframe with matching columns from the CSV data
    for col in dft.columns:
        if col in df.columns:
            dft[col] = df[col]
    # print("dft: ", dft.head())
    return dft


def table_meta():

    USER_TOKEN = json.loads(request.json.get('context',
                                             {})).get('scriptArgs',
                                                      {}).get('baseOpenID')

    fields = []
    isPrimaryCount = 0

    try:
        job_id = create_report(USER_TOKEN, "table")  # Get job_id from API

    except Exception as e:
        return jsonify({
            "code": 1,
            "msg":
            f"Failed to create report for account {USER_TOKEN}. Error: {e}",
            "data": {}
        }), 400

    # Wait for the report to be ready
    if wait_for_report(job_id):
        # try:
        df = csv_to_df(job_id)  # Download CSV and convert to dataframe
        # except Exception as e:
        #     return jsonify({
        #         "code": 1,
        #         "msg": f"Failed to retrieve CSV data. Error: {e}",
        #         "data": {}
        #     }), 400

        if df is None or df.empty:
            return jsonify({
                "code": 1,
                "msg": f"No data retrieved.",
                "data": {}
            }), 400

        # print(df.head())  # Display first 5 rows of the dataframe

        # Lấy danh sách tên cột từ DataFrame
        df_columns = df.columns.tolist()
        field_id = 1
        # Tạo danh sách fields dựa trên các cột của DataFrame
        for df_col in df_columns:
            if df_col in fb_to_bitable_mapping:
                bitable_field_info = fb_to_bitable_mapping[df_col]
                # print(bitable_field_info)
                bitable_field_name = bitable_field_info["name"]
                bitable_field_type = bitable_field_info["type"]
                bitable_field_id = bitable_field_info["id"]

                # Thêm field vào danh sách fields
                fields.append({
                    "fieldID":
                    bitable_field_id,  # Bạn có thể lấy ID từ field mapping nếu có
                    "fieldName": bitable_field_name,
                    "fieldType": bitable_field_type,
                    "isPrimary": False if isPrimaryCount > 0 else
                    True,  # Đặt field đầu tiên là primary
                    "description": ""  # Mô tả nếu có
                })
                isPrimaryCount += 1
                field_id += 1

        level_ads = r.get(f"{USER_TOKEN}_level")
        result = {
            "code": 0,
            "msg": "",
            "data": {
                "tableName": f"facebook_{level_ads}",
                "fields": fields
            }
        }
        # print(result)
        return jsonify(result)


def records():

    USER_TOKEN = json.loads(request.json.get('context',
                                             {})).get('scriptArgs',
                                                      {}).get('baseOpenID')
    
    try:
        job_id = create_report(USER_TOKEN, "records")  # Get job_id from API
    except Exception as e:
        return jsonify({
            "code": 1,
            "msg":
            f"Failed to create report for account {USER_TOKEN}. Error: {e}",
            "data": {}
        }), 400

    # Wait for the report to be ready
    if wait_for_report(job_id):
        try:
            df = csv_to_df(job_id)  # Download CSV and convert to dataframe
        except Exception as e:
            return jsonify({
                "code": 1,
                "msg": f"Failed to retrieve CSV data. Error: {e}",
                "data": {}
            }), 400

        if df is None or df.empty:
            return jsonify({
                "code": 1,
                "msg": f"No data retrieved",
                "data": {}
            }), 400

        # print(df.head())  # Display first 5 rows of the dataframe

        # Lấy danh sách tên cột từ DataFrame
        df_columns = df.columns.tolist()

        # Chèn dữ liệu vào Bitable
        records = []

        for index, row in df.iterrows():
            # Tạo một dict để lưu trữ dữ liệu với tên trường từ Bitable (fb_to_bitable_mapping)
            record_data = {}  # Giả sử có cột 'name' trong DataFrame
            
            for df_col in df_columns:
                if df_col in fb_to_bitable_mapping:
                    bitable_field_name = fb_to_bitable_mapping[df_col]["id"]
                    bitable_field_type = fb_to_bitable_mapping[df_col]["type"]
                    # bitable_field_id = bitable_field_info["id"]

                    # For specific fields, convert the value to a string
                    if df_col in [
                            "account_id", "campaign_id", "ad_set_id", "ad_id"
                    ]:
                        record_data[bitable_field_name] = str(
                            row[df_col]) if pd.notnull(row[df_col]) else ""

                    elif bitable_field_type == 5:  # Date type
                        date_value = pd.to_datetime(row[df_col],
                                                    errors='coerce')
                        if pd.notnull(date_value):
                            record_data[bitable_field_name] = int(
                                date_value.timestamp()) * 1000
                        else:
                            record_data[bitable_field_name] = ""

                    elif bitable_field_type == 2:  # Number type
                        record_data[bitable_field_name] = row[
                            df_col] if pd.notnull(row[df_col]) else 0

                    else:  # Text or other types
                        record_data[bitable_field_name] = row[
                            df_col] if pd.notnull(row[df_col]) else ""
            # Thêm bản ghi vào danh sách records với primaryID là chỉ mục của dòng hiện tại
            
            level_ads = r.get(f"{USER_TOKEN}_level")
            primary_id = ""
            # Kiểm tra level_ads và gán giá trị cho primary_id tương ứng
            if level_ads == "campaign":
                primary_id = str(record_data["fbads_1"]) + "" + str(
                    record_data["fbads_8"])
            elif level_ads == "adset":
                primary_id = str(record_data["fbads_1"]) + "" + str(
                    record_data["fbads_9"])
            elif level_ads == "ad":
                primary_id = str(record_data["fbads_1"]) + "" + str(
                    record_data["fbads_10"])

            records.append({
                "primaryID": primary_id,  # Chuyển index thành ID
                "data": record_data
            })

        limit = 1000
        # Construct the final response giống như đoạn code mẫu
        result = {
            "code": 0,
            "msg": "",
            "data": {
                "nextPageToken": str(
                    index + 1
                ),  # Tính toán token của trang tiếp theo dựa trên số lượng bản ghi
                "hasMore": len(records) ==
                limit,  # Kiểm tra xem có nhiều bản ghi hơn không
                "records": records
            }
        }
        return jsonify(result)  # Hiển thị kết quả
