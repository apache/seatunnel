from flask import Flask, request, jsonify
import json


app = Flask(__name__)


@app.route('/config/<app_name>', methods=['GET', 'POST', 'DELETE'])
def hello_world(app_name):

    msg = {}
    try:
        f = open(app.config['config_name'], 'r')
        config = json.load(f)
        apps = config['apps']
        f.close()

    except IOError:
        msg = {
            'status': 2,
            'content': 'File <{}> not found'.format(app.config['config_name'])
        }
        return jsonify(msg)

    except Exception as e:
        msg = {
            'status': 2,
            'content': str(e)
        }
        return jsonify(msg)

    if request.method == 'GET':

        flag = 0
        for spark_app in apps:
            if spark_app['app_name'] == app_name:
                msg = {
                    'status': 0,
                    'content': spark_app

                }
                flag = 1

        if flag == 0:
            msg = {
                'status': 2,
                'content': '{0} not in {1}'.format(app_name, app.config['config_name'])
            }

        return jsonify(msg)

    # Supporting insert and update
    elif request.method == 'POST':

        try:
            body = json.loads(request.get_data())
        except Exception as e:
            msg = {
                'status': 2,
                'content': str(e)
            }

            return jsonify(msg)

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

        f = open(app.config['config_name'], 'w')
        f.write(json.dumps(config, indent=4))
        f.close()

        msg = {
            "status": 0
        }
        return jsonify(msg)

    elif request.method == 'DELETE':

        for i in range(len(apps)):
            if apps[i]['app_name'] == app_name:
                del apps[i]

                msg = {
                    'status': 0
                }

                f = open(app.config['config_name'], 'w')
                f.write(json.dumps(config, indent=4))
                f.close()

                return jsonify(msg)

            msg = {
                'status': '2',
                'content': '{0} not in {1}'.format(app_name, app.config['config_name'])
            }

            return jsonify(msg)

    else:
        msg = {
            'status': 1,
            'content': 'Unsupported Method'
        }

        return jsonify(msg)
