import base64
import requests
import pandas as pd
import time as time_s
from datetime import datetime, timedelta, time
from baseopensdk import BaseClient
from baseopensdk.api.base.v1 import *
from dotenv import load_dotenv, find_dotenv
import os


def get_bitable_fields(client, table_id):
  request = ListAppTableFieldRequest.builder() \
      .table_id(table_id) \
      .build()
  response = client.base.v1.app_table_field.list(request)
  if response.code == 0:
    fields = response.data.items
    field_names = {field.field_name: field for field in fields}
    return field_names
  else:
    print(f"Failed to get Bitable fields. Error: {response.msg}")
    return {}


# Function to create a field in Bitable
def create_bitable_field(client, table_id, field_name):
  # Xác định loại field dựa trên tên field
  # if 'date_created' in field_name:
  #     field_type = 1001
  # elif 'last_modified' in field_name:
  #     field_type = 1002
  # elif 'created_by' in field_name:
  #     field_type = 1003
  # elif 'modified_by' in field_name:
  #     field_type = 1004
  # elif 'auto_serial' in field_name:
  #     field_type = 1005
  # elif 'date' in field_name or 'time' in field_name:
  #     field_type = 5  # Date
  # elif 'id' in field_name or 'phone' in field_name or 'number' in field_name:
  #     field_type = 2  # Number
  # elif 'link' in field_name:
  #     field_type = 15  # Link
  # elif 'person' in field_name:
  #     field_type = 11  # Person
  # elif 'attachment' in field_name:
  #     field_type = 17  # Attachment
  # elif 'checkbox' in field_name:
  #     field_type = 7  # Checkbox
  # elif 'formula' in field_name:
  #     field_type = 20  # Formula
  # elif 'single_option' in field_name:
  #     field_type = 3  # Single option
  # elif 'multiple_options' in field_name:
  #     field_type = 4  # Multiple options
  # elif 'location' in field_name:
  #     field_type = 22  # Location
  # else:
  field_type = 1  # Default to Multiline for undefined fields

  request = CreateAppTableFieldRequest.builder() \
      .table_id(table_id) \
      .request_body(AppTableField.builder()
          .field_name(field_name)
          .type(field_type)
          .build()) \
      .build()

  response = client.base.v1.app_table_field.create(request)
  if response.code == 0:
    print(f"Field '{field_name}' of type '{field_type}' created successfully.")
    return response.data.field
  else:
    print(f"Failed to create field '{field_name}'. Error: {response.msg}")
    return None


def create_bitable_record(client, table_id, row, df_columns, bitable_fields):
  """
  Tạo bản ghi trong Bitable từ một dòng dữ liệu.
  """
  # Chuẩn bị dictionary fields
  fields = {}
  for col in df_columns:
      value = row.get(col)

      # Kiểm tra giá trị null và chuyển tất cả thành chuỗi (text)
      if pd.notnull(value):
          bitable_field = bitable_fields.get(col)
          if bitable_field is None:
              print(f"Warning: Field '{col}' does not exist in Bitable fields. Skipping...")
              continue  # Bỏ qua field nếu không tồn tại trong Bitable

          # Chuyển tất cả giá trị thành chuỗi
          try:
              fields[col] = str(value)
          except Exception as e:
              print(f"Error converting field '{col}' with value '{value}'. Error: {e}")
              continue

  # Tạo request để thêm bản ghi vào Bitable
  request = CreateAppTableRecordRequest.builder() \
      .table_id(table_id) \
      .request_body(AppTableRecord.builder()
          .fields(fields)
          .build()) \
      .build()

  # Gửi request tạo bản ghi
  response = client.base.v1.app_table_record.create(request)
  if response.code == 0:
      print(f"Record added successfully for ad_id: {row.get('ad_id')}")
  else:
      print(f"Failed to add record for ad_id: {row.get('ad_id')}. Error: {response.msg}")
