from flask import Flask, request, render_template, jsonify
from playground.synsc_facebook_ads import mainfnc
import pandas as pd

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # Get form data
            APP_TOKEN = request.form['APP_TOKEN']
            PERSONAL_BASE_TOKEN = request.form['PERSONAL_BASE_TOKEN']
            TABLE_ID = request.form['TABLE_ID']
            access_token = request.form['access_token']
            list_account = request.form['list_account'].split(',')

            since = request.form['since']
            until = request.form['until']
            level_ads = request.form['level_ads']
            # Format dates to 'yyyy-mm-dd'
            since = pd.to_datetime(since).strftime('%Y-%m-%d')
            until = pd.to_datetime(until).strftime('%Y-%m-%d')

            # Save these values in the synsc_facebook_ads module
            from playground import synsc_facebook_ads
            synsc_facebook_ads.APP_TOKEN = APP_TOKEN
            synsc_facebook_ads.PERSONAL_BASE_TOKEN = PERSONAL_BASE_TOKEN
            synsc_facebook_ads.TABLE_ID = TABLE_ID
            synsc_facebook_ads.access_token = access_token
            synsc_facebook_ads.list_account = list_account
            synsc_facebook_ads.last_2day = since
            synsc_facebook_ads.yesterday = until
            synsc_facebook_ads.level_ads = level_ads
            # Call the main function
            result = mainfnc()
            if result is not None:
                return jsonify(status='success', message=result)
            else:
                return jsonify(
                    status='error',
                    message=
                    "Please double check the information you have configured.")
        except Exception as e:
            # Log và trả về thông báo lỗi
            error_message = str(e)
            return jsonify(status='error',
                           message=f"An error occurred: {error_message}")

    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=81)
