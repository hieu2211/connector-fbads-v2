from flask import Flask
from playground.synsc_facebook_ads import mainfnc

app = Flask(__name__)


@app.route('/')
def index():
    result = mainfnc()  # Giả sử hàm mainfnc trả về một giá trị nào đó
    if result:
        return result
    else:
        return "Main function executed successfully."


# @app.route('/sync_data_to_bq')
# def sync_data_to_bq():
#     sync_data_to_bq('abc','123')
#     return sync_data_to_bq('abc','123')

app.run(host='0.0.0.0', port=81)
