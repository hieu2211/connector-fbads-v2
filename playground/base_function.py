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
def create_bitable_field(client, table_id, field_name, field_type):

    request = CreateAppTableFieldRequest.builder() \
        .table_id(table_id) \
        .request_body(AppTableField.builder()
            .field_name(field_name)
            .type(field_type)
            .build()) \
        .build()

    response = client.base.v1.app_table_field.create(request)
    if response.code == 0:
        print(
            f"Field '{field_name}' of type '{field_type}' created successfully."
        )
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
                print(
                    f"Warning: Field '{col}' does not exist in Bitable fields. Skipping..."
                )
                continue  # Bỏ qua field nếu không tồn tại trong Bitable

            # Chuyển tất cả giá trị thành chuỗi
            try:
                fields[col] = value
            except Exception as e:
                print(
                    f"Error converting field '{col}' with value '{value}'. Error: {e}"
                )
                continue

    print(f"fields{fields}")
    print(f"df_columns{df_columns}")

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
        print(f"Failed to add record. Error: {response.msg}")
