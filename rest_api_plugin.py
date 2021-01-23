import json
import logging
import socket
from datetime import datetime
import base64

from airflow import configuration
from airflow import settings
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from flask import Blueprint, request, Response
from airflow import settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

#requires_authentication = airflow.api.API_AUTH.api_auth.requires_authentication

airflow_api_blueprint = Blueprint('airflow_api', __name__, url_prefix='/airflow/api/v1')

# Getting configurations from airflow.cfg file
airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
airflow_base_log_folder = configuration.get('core', 'BASE_LOG_FOLDER')
airflow_dags_folder = configuration.get('core', 'DAGS_FOLDER')

# Getting Versions and Global variables
hostname = socket.gethostname()


class ApiInputException(Exception):
    pass


class ApiResponse:

    def __init__(self):
        pass

    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500

    @staticmethod
    def standard_response(status, code=0, msg='', payload=''):
        json_data = json.dumps({
            'code': code,
            'msg': msg,
            'data': payload
        })
        resp = Response(json_data, status=status, mimetype='application/json')
        return resp

    @staticmethod
    def success(payload):
        return ApiResponse.standard_response(ApiResponse.STATUS_OK, code=0, msg='success', payload=payload)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, code=1, msg=error)

    @staticmethod
    def bad_request(error):
        return ApiResponse.error(ApiResponse.STATUS_BAD_REQUEST, error)

    @staticmethod
    def not_found(error='Resource not found'):
        return ApiResponse.error(ApiResponse.STATUS_NOT_FOUND, error)

    @staticmethod
    def unauthorized(error='Not authorized to access this resource'):
        return ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED, error)

    @staticmethod
    def server_error(error='An unexpected problem occurred'):
        return ApiResponse.error(ApiResponse.STATUS_SERVER_ERROR, error)



@airflow_api_blueprint.before_request
def verify_authentication():
    """
    Verify Authentication by using airflow.contrib.auth.backends.password_auth
    """
    logging.info("validating api authentication")

    authorization = request.headers.get('authorization')    
    userpass = ''.join(authorization.split()[1:])
    username, password = base64.b64decode(userpass).decode("utf-8").split(":", 1)    
    
    session = settings.Session()    

    user = session.query(PasswordUser).filter(PasswordUser.username == username).first()

    if not user:
        return ApiResponse.unauthorized("You are not authorized to use this resource")

    if not user.authenticate(password):
        return ApiResponse.unauthorized("You are not authorized to use this resource")


@csrf.exempt
@airflow_api_blueprint.route('/helloWorld', methods=['GET'])
def sayHello():
    '''
    Say Hello
    '''
    logging.info("Executing custom 'sayHello' function")

    payload = {}

    payload['greetings'] = 'Hello World'        
    payload['message_date'] = str(datetime.now())

    return ApiResponse.success(payload)


@csrf.exempt
@airflow_api_blueprint.route('/load_data', methods=['POST'])
def create_dag_run():
    '''
    sample post function to load data
    '''
    logging.info("Executing custom 'create_dag_run' function")
    # decode input
    data = request.get_json(force=True)
    # ensure there is a dag id
    print(data)

    return ApiResponse.success({'data': data})


# Creating the AirflowRestApiPlugin which extends the AirflowPlugin so its imported into Airflow
class AirflowRestApiPlugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    flask_blueprints = [airflow_api_blueprint]
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
