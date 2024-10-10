from flask import Flask, request, render_template, jsonify, redirect, url_for
from v1.controllers.synsc_facebook_ads import mainfnc
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


# Router trang chủ với nút bấm chuyển trang
@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        # Chuyển hướng qua trang v1
        return redirect(url_for('v1fbads'))

    return '''
    <div><h3>Các phiên bản đã được triển khai</h3></div>
        <form method="POST">
            <span>Phiên bản 1</span> <button type="submit">Chuyển qua trang v1</button>
        </form>
    '''


@app.route('/v1', methods=['GET', 'POST'])
def v1fbads():
    print(f"request {request}")
    if request.method == 'POST':

        print(f"request {request}")

        data = request.get_json()
        print(data)

        APP_TOKEN = data['app_token']
        PERSONAL_BASE_TOKEN = data['personal_base_token']
        TABLE_ID = data['table_id']
        user_id = data['user_id']
        tenant_key = data['tenant_key']
        access_token = data['access_token']
        list_account = data['list_account'].split(',')

        since = data['since']
        until = data['until']
        level_ads = data['level_ads']

        # Format dates to 'yyyy-mm-dd'
        since = pd.to_datetime(since).strftime('%Y-%m-%d')
        until = pd.to_datetime(until).strftime('%Y-%m-%d')

        # Save these values in the synsc_facebook_ads module
        from v1.controllers import synsc_facebook_ads
        synsc_facebook_ads.APP_TOKEN = APP_TOKEN
        synsc_facebook_ads.PERSONAL_BASE_TOKEN = PERSONAL_BASE_TOKEN
        synsc_facebook_ads.TABLE_ID = TABLE_ID
        synsc_facebook_ads.user_id = user_id
        synsc_facebook_ads.tenant_key = tenant_key
        synsc_facebook_ads.access_token = access_token
        synsc_facebook_ads.list_account = list_account
        synsc_facebook_ads.last_2day = since
        synsc_facebook_ads.yesterday = until
        synsc_facebook_ads.level_ads = level_ads

        result = mainfnc()
        print(f"result {result}")
        if result is not None:
            return jsonify(status='success', message=result)
        else:
            return jsonify(
                status='error',
                message=
                "Please double check the information you have configured.")

    return render_template('v1-fbads.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=81)
