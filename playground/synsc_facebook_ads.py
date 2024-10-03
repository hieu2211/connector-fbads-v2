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
                                      create_bitable_record,
                                      batch_create_bitable_records)

# Load environment variables
load_dotenv(find_dotenv())

APP_TOKEN = ""
PERSONAL_BASE_TOKEN = ""
TABLE_ID = ""

# Define necessary variables
yesterday = ""
last_2day = ""
today = datetime.today().strftime('%Y-%m-%d')
api_url = 'https://graph.facebook.com/v20.0/'
access_token = ''  # Thay bằng Access Token của bạn
list_account = []  # Thay bằng danh sách các tài khoản của bạn
fields = "date_start,account_name,account_currency,account_id,campaign_name,adset_name,ad_name,campaign_id,adset_id,ad_id,objective,reach,impressions,frequency,spend,clicks,website_ctr,cpc,ctr,cpm,cpp,video_thruplay_watched_actions,video_30_sec_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions,video_play_actions,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,engagement_rate_ranking,estimated_ad_recallers,conversion_rate_ranking,conversions,conversion_values,actions"
level_ads = ''

fb_to_bitable_mapping = {
    "reporting_starts": {
        "name": "Reporting starts",
        "type": 5
    },  # date
    "account_name": {
        "name": "Account name",
        "type": 1
    },  # text
    "currency": {
        "name": "Currency",
        "type": 1
    },  # text
    "account_id": {
        "name": "Account ID",
        "type": 1
    },  # text
    "campaign_name": {
        "name": "Campaign name",
        "type": 1
    },  # text
    "adset_name": {
        "name": "Ad Set Name",
        "type": 1
    },  # text
    "ad_name": {
        "name": "Ad name",
        "type": 1
    },  # text
    "campaign_id": {
        "name": "Campaign ID",
        "type": 1
    },  # text
    "adset_id": {
        "name": "Ad set ID",
        "type": 1
    },  # text
    "ad_id": {
        "name": "Ad ID",
        "type": 1
    },  # text
    "objective": {
        "name": "Objective",
        "type": 1
    },  # text
    "reach": {
        "name": "Reach",
        "type": 2
    },  # number
    "impressions": {
        "name": "Impressions",
        "type": 2
    },  # number
    "frequency": {
        "name": "Frequency",
        "type": 2
    },  # number
    "amount_spent__vnd_": {
        "name": "Amount spent",
        "type": 2
    },  # number
    "clicks__all_": {
        "name": "Clicks (all)",
        "type": 2
    },  # number
    "ctr__link_click_through_rate_": {
        "name": "CTR (link click-through rate)",
        "type": 2
    },  # number
    "cpc__all___vnd_": {
        "name": "CPC (All)",
        "type": 2
    },  # number
    "ctr__all_": {
        "name": "CTR (all)",
        "type": 2
    },  # number
    "cpm__cost_per_1_000_impressions___vnd_": {
        "name": "CPM",
        "type": 2
    },  # number
    "cost_per_1_000_accounts_center_accounts_reached__vnd_": {
        "name": "Cost per 1,000 people reached",
        "type": 2
    },  # number
    "thruplays": {
        "name": "ThruPlays",
        "type": 2
    },  # number
    # "video_30_sec_watched_actions": {
    #     "name": "Video 30 sec watched actions",
    #     "type": 2
    # },  # number
    # "video_p25_watched_actions": {
    #     "name": "Video plays at 25%",
    #     "type": 2
    # },  # number
    # "video_p50_watched_actions": {
    #     "name": "Video plays at 50%",
    #     "type": 2
    # },  # number
    # "video_p75_watched_actions": {
    #     "name": "Video plays at 75%",
    #     "type": 2
    # },  # number
    # "video_p95_watched_actions": {
    #     "name": "Video plays at 95%",
    #     "type": 2
    # },  # number
    # "video_p100_watched_actions": {
    #     "name": "Video plays at 100%",
    #     "type": 2
    # },  # number
    # "video_play_actions": {
    #     "name": "Video plays",
    #     "type": 2
    # },  # number
    # "instant_experience_clicks_to_open": {
    #     "name": "Instant experience clicks to open",
    #     "type": 2
    # },  # number
    # "instant_experience_clicks_to_start": {
    #     "name": "Instant experience clicks to start",
    #     "type": 2
    # },  # number
    # "instant_experience_outbound_clicks": {
    #     "name": "Instant experience outbound clicks",
    #     "type": 2
    # },  # number
    # "engagement_rate_ranking": {
    #     "name": "Engagement rate ranking",
    #     "type": 2
    # },  # number
    "estimated_ad_recall_lift__people_": {
        "name": "Estimated ad recall lift (people)",
        "type": 2
    },  # number
    # "conversion_rate_ranking": {
    #     "name": "Conversion rate ranking",
    #     "type": 2
    # },  # number
    # "conversions": {
    #     "name": "Conversions",
    #     "type": 2
    # },  # number
    # "conversion_values": {
    #     "name": "Conversion values",
    #     "type": 2
    # },  # number
    # "outbound_clicks": {
    #     "name": "Outbound clicks",
    #     "type": 2
    # },  # number
    "post_comments": {
        "name": "Post comments",
        "type": 2
    },  # number
    # "landing_page_view": {
    #     "name": "Landing page views",
    #     "type": 2
    # },  # number
    # "like": {
    #     "name": "Page likes",
    #     "type": 2
    # },  # number
    "link_clicks": {
        "name": "Link clicks",
        "type": 2
    },  # number
    # "photo_view": {
    #     "name": "Photo views",
    #     "type": 2
    # },  # number
    "post_shares": {
        "name": "Post shares",
        "type": 2
    },  # number
    "post_reactions": {
        "name": "Post reactions",
        "type": 2
    },  # number
    # "rsvp": {
    #     "name": "Event responses",
    #     "type": 2
    # },  # number
    "3_second_video_plays": {
        "name": "3-second video plays",
        "type": 2
    },  # number
    "page_engagement": {
        "name": "Page engagement",
        "type": 2
    },  # number
    "post_engagements": {
        "name": "Post engagement",
        "type": 2
    },  # number
    # "lead": {
    #     "name": "All offsite leads plus all On-Facebook leads",
    #     "type": 2
    # },  # number
    # "checkin": {
    #     "name": "Check-ins",
    #     "type": 2
    # },  # number
    # "credit_spent": {
    #     "name": "Credit spends",
    #     "type": 2
    # },  # number
    # "games.plays": {
    #     "name": "Game plays",
    #     "type": 2
    # },  # number
    # "onsite_conversion.post_save": {
    #     "name": "Post saves",
    #     "type": 2
    # },  # number
    # "onsite_conversion.purchase": {
    #     "name": "On-Facebook Purchases",
    #     "type": 2
    # },  # number
    # "onsite_conversion.lead_grouped": {
    #     "name": "All On-Facebook leads",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_add_payment_info": {
    #     "name": "Adds of payment info",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_add_to_cart": {
    #     "name": "Adds to cart",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_add_to_wishlist": {
    #     "name": "Adds to wishlist",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_complete_registration": {
    #     "name": "Completed registration",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_initiate_checkout": {
    #     "name": "Checkouts Initiated",
    #     "type": 2
    # },  # number
    "leads": {
        "name": "Leads",
        "type": 2
    },  # number
    # "offsite_conversion.fb_pixel_purchase": {
    #     "name": "Purchases",
    #     "type": 2
    # },  # number
    # "offsite_conversion.fb_pixel_search": {
    #     "name": "Searches",
    #     "type": 2
    # },  # number
    "website_content_views": {
        "name": "Content views",
        "type": 2
    },  # number
    # "onsite_conversion.messaging_block": {
    #     "name": "Blocked messaging connections",
    #     "type": 2
    # },  # number
    "messaging_conversations_started": {
        "name": "Messaging Conversations Started",
        "type": 2
    },  # number
    "new_messaging_contacts": {
        "name": "New messaging connections",
        "type": 2
    }  # number
}


# Function to create a report and get the job_idx
def create_report(account):
    # print("h" + yesterday)x`x
    # print(last_2day)
    url = api_url + 'act_' + account + '/insights'
    params = {
        "access_token": access_token,
        "level": level_ads,
        "time_range":
        '{"since": "' + last_2day + '", "until": "' + yesterday + '"}',
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

    dft = pd.DataFrame(columns=[
        "reporting_starts", "account_name", "currency", "account_id",
        "campaign_name", "adset_name", "ad_name", "campaign_id", "adset_id",
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
    # print(dft)
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
        print("<script>showErrorMessage();</script>")
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
                print(
                    f"Failed to retrieve CSV data for account {account}. Error: {e}"
                )
                continue
            if df is None or df.empty:
                print(f"No data retrieved for account {account}.")
                continue
            print(df.head())  # Display first 5 rows of the dataframe

            # Lấy danh sách tên cột từ DataFrame
            df_columns = df.columns.tolist()

            # Kiểm tra và tạo fields trong Bitable nếu chúng chưa tồn tại
            for df_col in df_columns:
                if df_col in fb_to_bitable_mapping:
                    # Lấy tên và type của field từ fb_to_bitable_mapping
                    bitable_field_info = fb_to_bitable_mapping[df_col]
                    bitable_field_name = bitable_field_info["name"]
                    bitable_field_type = bitable_field_info["type"]

                    if bitable_field_name not in bitable_fields:
                        # Tạo field trong Bitable với tên và kiểu dữ liệu tương ứng
                        new_field = create_bitable_field(
                            client, TABLE_ID, bitable_field_name,
                            bitable_field_type)
                        if new_field:
                            bitable_fields[bitable_field_name] = new_field
                    else:
                        print(
                            f"Field '{bitable_field_name}' already exists in Bitable."
                        )

            # Chèn dữ liệu vào Bitable
            all_records_data = []

            for index, row in df.iterrows():
                # Tạo một dict để lưu trữ dữ liệu với tên trường từ Bitable (fb_to_bitable_mapping)
                record_data = {}

                for df_col in df_columns:
                    if df_col in fb_to_bitable_mapping:
                        # Lấy tên trường từ fb_to_bitable_mapping
                        bitable_field_name = fb_to_bitable_mapping[df_col][
                            "name"]
                        bitable_field_type = fb_to_bitable_mapping[df_col][
                            "type"]

                        # For specific fields, convert the value to a string
                        if df_col in [
                                "account_id", "campaign_id", "adset_id",
                                "ad_id"
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
                                df_col] if pd.notnull(
                                    row[df_col]
                                ) else ""  # Giữ nguyên giá trị nếu không phải NaN

                # Thêm record_data vào danh sách all_records_data
                all_records_data.append(record_data)
            # print(all_records_data)
            # Sau khi hoàn tất việc thu thập tất cả các bản ghi, gọi hàm batch_create_bitable_records
            batch_create_bitable_records(client, TABLE_ID, all_records_data,
                                         list(all_records_data[0].keys()),
                                         bitable_fields)

        else:
            print(f"Report for account {account} did not complete.")

    # Nếu công việc hoàn thành mà không có lỗi nào
    print("<script>showSuccessMessage();</script>")
    return "success"


# Run main function
if __name__ == "__main__":
    mainfnc()

# Create a Record
# def mainfnc():
#     # Build Lark client
#     client: BaseClient = BaseClient.builder() \
#         .app_token(APP_TOKEN) \
#         .personal_base_token(PERSONAL_BASE_TOKEN) \
#         .build()

#     # Lấy danh sách các fields hiện có trong Bitable
#     bitable_fields = get_bitable_fields(client, TABLE_ID)
#     if not bitable_fields:
#         print("Could not retrieve Bitable fields. Exiting.")
#         print("<script>showErrorMessage();</script>")
#         return

#     for account in list_account:
#         try:
#             job_id = create_report(account)  # Get job_id from API
#             print(f"Report run ID for account {account}: {job_id}")
#         except Exception as e:
#             print(f"Failed to create report for account {account}. Error: {e}")
#             continue
#         # Wait for the report to be ready
#         if wait_for_report(job_id):
#             try:
#                 df = csv_to_df(job_id)  # Download CSV and convert to dataframe
#             except Exception as e:
#                 print(
#                     f"Failed to retrieve CSV data for account {account}. Error: {e}"
#                 )
#                 continue
#             if df is None or df.empty:
#                 print(f"No data retrieved for account {account}.")
#                 continue
#             print(df.head())  # Display first 5 rows of the dataframe

#             # Lấy danh sách tên cột từ DataFrame
#             df_columns = df.columns.tolist()

#             # Kiểm tra và tạo fields trong Bitable nếu chúng chưa tồn tại
#             for df_col in df_columns:
#                 if df_col in fb_to_bitable_mapping:
#                     # Lấy tên và type của field từ fb_to_bitable_mapping
#                     bitable_field_info = fb_to_bitable_mapping[df_col]
#                     bitable_field_name = bitable_field_info["name"]
#                     bitable_field_type = bitable_field_info["type"]

#                     if bitable_field_name not in bitable_fields:
#                         # Tạo field trong Bitable với tên và kiểu dữ liệu tương ứng
#                         new_field = create_bitable_field(
#                             client, TABLE_ID, bitable_field_name,
#                             bitable_field_type)
#                         if new_field:
#                             bitable_fields[bitable_field_name] = new_field
#                     else:
#                         print(
#                             f"Field '{bitable_field_name}' already exists in Bitable."
#                         )

#             # Chèn dữ liệu vào Bitable
#             for index, row in df.iterrows():
#                 # Tạo một dict để lưu trữ dữ liệu với tên trường từ Bitable (fb_to_bitable_mapping)
#                 record_data = {}

#                 for df_col in df_columns:
#                     if df_col in fb_to_bitable_mapping:
#                         # Lấy tên trường từ fb_to_bitable_mapping
#                         bitable_field_name = fb_to_bitable_mapping[df_col][
#                             "name"]
#                         bitable_field_type = fb_to_bitable_mapping[df_col][
#                             "type"]

#                         # For specific fields, convert the value to a string
#                         if df_col in [
#                                 "account_id", "campaign_id", "adset_id",
#                                 "ad_id"
#                         ]:
#                             record_data[bitable_field_name] = str(
#                                 row[df_col]) if pd.notnull(row[df_col]) else ""

#                         elif bitable_field_type == 5:  # Date type
#                             date_value = pd.to_datetime(row[df_col],
#                                                         errors='coerce')
#                             if pd.notnull(date_value):
#                                 record_data[bitable_field_name] = int(
#                                     date_value.timestamp()) * 1000
#                             else:
#                                 record_data[bitable_field_name] = ""

#                         elif bitable_field_type == 2:  # Number type
#                             record_data[bitable_field_name] = row[
#                                 df_col] if pd.notnull(row[df_col]) else 0

#                         else:  # Text or other types
#                             record_data[bitable_field_name] = row[
#                                 df_col] if pd.notnull(
#                                     row[df_col]
#                                 ) else ""  # Giữ nguyên giá trị nếu không phải NaN
#                 # print(record_data)
#                 # Gọi hàm create_bitable_record với record_data và bitable_fields
#                 create_bitable_record(client, TABLE_ID, record_data,
#                                       list(record_data.keys()), bitable_fields)

#         else:
#             print(f"Report for account {account} did not complete.")

#     # Nếu công việc hoàn thành mà không có lỗi nào
#     print("<script>showSuccessMessage();</script>")
#     return "success"
