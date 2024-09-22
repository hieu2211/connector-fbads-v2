from flask import Flask, request, render_template
from playground.synsc_facebook_ads import mainfnc

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get form data
        APP_TOKEN = request.form['APP_TOKEN']
        PERSONAL_BASE_TOKEN = request.form['PERSONAL_BASE_TOKEN']
        TABLE_ID = request.form['TABLE_ID']
        access_token = request.form['access_token']
        list_account = request.form['list_account'].split(',')

        # Save these values in the synsc_facebook_ads module
        from playground import synsc_facebook_ads
        synsc_facebook_ads.APP_TOKEN = APP_TOKEN
        synsc_facebook_ads.PERSONAL_BASE_TOKEN = PERSONAL_BASE_TOKEN
        synsc_facebook_ads.TABLE_ID = TABLE_ID
        synsc_facebook_ads.access_token = access_token
        synsc_facebook_ads.list_account = list_account

        # Call the main function
        result = mainfnc()
        return result if result else "Main function executed successfully."

    return render_template('form.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=81)
