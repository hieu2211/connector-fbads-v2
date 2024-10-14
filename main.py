from flask import Flask, request, render_template, jsonify, redirect, url_for, send_from_directory
from v1.controllers.synsc_facebook_ads import (table_meta,records)
import pandas as pd
from flask_cors import CORS
from replit import db
import os

app = Flask(__name__)
CORS(app)


# Router trang chủ với nút bấm chuyển trang
@app.route('/', methods=['GET', 'POST'])
def home():
    return render_template('v1-fbads.html')


@app.route('/v1', methods=['POST'])
def v1fbads():
    if request.method == 'POST':


        data = request.get_json()
        user_id = data['user_id']
        tenant_key = data['tenant_key']
        access_token = data['access_token']
        list_account = data['list_account']
        level_ads = data['level_ads']
        USER_TOKEN = user_id
        db[str(USER_TOKEN + "_accesstoken")] = access_token
        db[str(USER_TOKEN + "_account")] = list_account
        db[str(USER_TOKEN + "_level")] = level_ads

        token = db[str(USER_TOKEN + "_accesstoken")]
        acc = db[str(USER_TOKEN + "_account")]
        leve = db[str(USER_TOKEN + "_level")]
        
        return jsonify({"message": "Infomation stored successfully"})

    return render_template('v1-fbads.html')

@app.route('/table_meta', methods=['POST'])
def get_table_meta():
    result = table_meta()
    return result

@app.route('/records', methods=['POST'])
def get_record():
    result = records()
    return result

@app.route('/<path:filename>.json')
def serve_static(filename):
  return send_from_directory(app.static_folder, filename + '.json')

if __name__ == '__main__':
    cors = CORS(app,
        resources={
            r"/*": {
                "origins":
                "https://94297baf-9e9f-4ad9-89f2-5f43d9e99c92-00-10ibz39xqbg61.picard.replit.dev"
            }
        })
    app.secret_key = os.urandom(24).hex()
    app.run(host='0.0.0.0', port=81)
    # app.run(host='0.0.0.0', port=81)
