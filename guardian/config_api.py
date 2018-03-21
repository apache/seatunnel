from flask import Flask, request, jsonify
import json

app = Flask(__name__)


@app.route('/config/<app_name>', methods=['GET', 'POST', 'DELETE'])
def hello_world(app_name):

    msg = {}
    try:
        f = open('config.json.template', 'r')
        config = json.load(f)
        apps = config['apps']
        f.close()

    except IOError:
        msg = {
            'status': '404',
            'content': 'File <{}> not found'.format('config.json.template')
        }
        return jsonify(msg)

    except Exception as e:
        msg = {
            'status': '1001',
            'content': str(e)
        }
        return jsonify(msg)

    if request.method == 'GET':

        flag = 0
        for spark_app in apps:
            if spark_app['app_name'] == app_name:
                msg = spark_app
                flag = 1

        if flag == 0:
            msg = {
                'status': '404',
                'content': '{0} not in {1}'.format(app_name, 'config.json.template')
            }

        return jsonify(msg)

    # Update insert and update
    elif request.method == 'POST':
        body = json.loads(request.get_data())

        flag = 0
        for spark_app in apps:
            if spark_app['app_name'] == app_name:
                info = spark_app
                for key in body.keys():
                    info[key] = body[key]

                flag = 1

        if flag == 0:
            body['app_name'] = app_name
            apps.append(body)

        f = open('config.json.template', 'w')
        f.write(json.dumps(config))
        f.close()

        msg = {
            "status": "200"
        }
        return jsonify(msg)

    elif request.method == 'DELETE':

        for i in range(len(apps)):
            if apps[i]['app_name'] == app_name:
                del apps[i]

                msg = {
                    'status': '200'
                }

                f = open('config.json.template', 'w')
                f.write(json.dumps(config))
                f.close()

                return jsonify(msg)

            msg = {
                'status': '404',
                'content': '{0} not in {1}'.format(app_name, 'config.json.template')
            }

            return jsonify(msg)

    else:
        msg = {
            'status': '200',
            'content': 'Unsupported Method'
        }

        return jsonify(msg)


if __name__ == '__main__':
    app.run()
