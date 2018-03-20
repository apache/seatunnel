from flask import Flask, request, jsonify
import json

app = Flask(__name__)


@app.route('/config/<app_name>', methods=['GET', 'POST', 'DELETE'])
def hello_world(app_name):

    if request.method == 'GET':

        f = open('config.json.template', 'r')
        config = json.load(f)
        apps = config['apps']

        flag = 0
        for app in apps:
            if app['app_name'] == app_name:
                msg = app
                flag = 1

        if flag == 0:
            msg = {
                "status": "200",
                "content": "App not in {}".format("config.json.template")
            }

        return jsonify(msg)

    # Update insert and update
    elif request.method == 'POST':
        body = json.loads(request.get_data())
        print body.keys()
        return "test"

    elif request.method == 'DELETE':
        pass

    else:
        msg = {
            "status": "200",
            "content": "Unsupported Method"
        }

        return jsonify(msg)


if __name__ == '__main__':
    app.run()
