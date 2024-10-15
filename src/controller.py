import json
import logging
import os
import tempfile
import traceback
import uuid
from pathlib import Path

import flask
import pandas
import yaml
from cryptography.fernet import Fernet
from flask import Flask, request, send_file, render_template
from flask_cors import CORS
from werkzeug.utils import redirect
from yaml.scanner import ScannerError

import src.controller_utility as controller_util
import src.test as test
import src.validator as validator
import src.wbr as wbr
from src.publish_utility import PublishWbr

app = Flask(__name__,
            static_url_path='',
            static_folder='web/static',
            template_folder='web/templates')

cors = CORS(app, resources={r"/*": {"origins": "*"}})

key = Fernet.generate_key()

which_env = os.environ.get("ENVIRONMENT") or 'qa'
publisher = PublishWbr(os.getenv("OBJECT_STORAGE_OPTION"), os.environ.get("OBJECT_STORAGE_BUCKET"))


@app.route('/get-wbr-metrics', methods=['POST'])
def get_wbr_metrics():
    """
    A flask endpoint, build WBR for given data csv and config yaml file.
    :return: A json response for the frontend to render the data
    """
    # Get the configuration file and CSV data file from the request
    config_file = request.files['configfile']
    csv_data_file = request.files['csvfile']

    # Create a temporary file to save the configuration file
    with tempfile.NamedTemporaryFile(mode="a", delete=False) as temp_file:
        config_file.save(temp_file.name)
        try:
            # Load the YAML configuration from the temporary file
            cfg = yaml.load(open(temp_file.name), controller_util.SafeLineLoader)
        except (ScannerError, yaml.YAMLError) as e:
            logging.error(e, exc_info=True)
            error_message = traceback.format_exc().split('.yaml')[-1].replace(',', '').replace('"', '')
            # Return an error response if there is an issue with the YAML configuration
            response = app.response_class(
                response=json.dumps(
                    {
                        "description": f"Could not create WBR metrics due to incorrect yaml, caused due to error in "
                                       f"{error_message}"
                    }
                ),
                status=500
            )
            return response
        temp_file.close()

    try:
        wbr_validator = validator.WBRValidator(csv_data_file, cfg)
        wbr_validator.validate_yaml()
    except Exception as e:
        logging.error("Yaml validation failed", e, exc_info=True)
        response = app.response_class(
            response=json.dumps({"description": "Invalid configuration provided: " + e.__str__()}),
            status=500
        )
        return response
    try:
        # Create a WBR object using the CSV data and configuration
        wbr1 = wbr.WBR(cfg, daily_df=wbr_validator.daily_df)
    except Exception as error:
        logging.error(error, exc_info=True)
        # Return an error response if there is an issue creating the WBR object
        response = app.response_class(
            response=json.dumps({"description": "Could not create WBR metrics due to: " + error.__str__()}),
            status=500
        )
        return response

    try:
        # Generate the WBR deck using the WBR object
        deck = controller_util.get_wbr_deck(wbr1)
    except Exception as err:
        logging.error(err, exc_info=True)
        # Return an error response if there is an issue creating the deck
        response = app.response_class(
            response=json.dumps({"description": "Error while creating deck, caused by: " + err.__str__()}),
            status=500
        )
        return response

    # Return the WBR deck as a JSON response
    response = app.response_class(
        response=json.dumps(deck, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )

    return response


def str_presenter(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


@app.route('/download_yaml', methods=['POST'])
def download_yaml_for_csv():
    """
    Downloads a YAML file based on the provided CSV file.

    Returns:
        The downloaded YAML file as an attachment.
    """
    csv_data_file = request.files['csvfile']
    csv_data = pandas.read_csv(csv_data_file, parse_dates=['Date'], thousands=',')

    temp_file = tempfile.NamedTemporaryFile(mode="a", dir='/tmp/')

    try:
        from wbryamlgenerator.yaml_generator import generate
        csv_data_string: str = csv_data.head(3).to_csv(index=False)
        generate(csv_data_string, temp_file)
        return send_file(temp_file.name, mimetype='application/x-yaml', as_attachment=True)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.info("Exception occurred! falling back to the default implementation")
        controller_util.generate_custom_yaml(temp_file, csv_data)
        return send_file(temp_file.name, mimetype='application/x-yaml', as_attachment=True)


@app.route('/publish-wbr-report', methods=['POST'])
def publish_report():
    """
    Fetch JSON file from the HTTP request, save the file to S3 bucket and publish the WBR to a public URL.

    Returns:
        A Flask response object with the URL to access the uploaded data.
    """
    # Parse the JSON data from the request
    data = json.loads(request.data)

    # Modify the base URL to use HTTPS instead of HTTP
    base_url = request.base_url.replace('/publish-wbr-report', '')
    if "localhost" not in base_url and "127.0.0.1" not in base_url:
        base_url = base_url.replace("http", "https")

    # Generate a unique filename for the JSON data
    filename = str(uuid.uuid4())[25:]

    # Upload the report to cloud storage
    publisher.upload(data, which_env + "/" + filename)

    # Create a response with the URL to access the uploaded data
    response = app.response_class(
        response=json.dumps({'path': base_url + '/build-wbr/publish?file=' + filename}, indent=4,
                            cls=controller_util.Encoder),
        status=200
    )

    return response


@app.route('/build-wbr/publish', methods=['GET'])
def build_wbr():
    """
    Builds unprotected WBR onto the web browser using the already saved WBR report
    :return: Rendered template of already generated report
    """
    filename = request.args['file']
    data = publisher.download(which_env + "/" + filename)
    return flask.render_template('wbr_share.html', data=data)


@app.route('/login', methods=["GET", "POST"])
def login():
    """
    A callback function when building a protected report onto web browser if successfully verify user redirected to
    build-wbr/publish/protected endpoint where protected WBR report will be rendered
    """
    # Get the file name from the request arguments
    file_name = request.args['file']

    if 'password' in request.args:
        # If password is provided in the request arguments
        auth_password = request.args['password']
        try:
            # Retrieve the JSON file from S3 bucket
            protected_data = publisher.download(which_env + "/" + file_name)
        except Exception as e:
            # Log any exceptions that occur during file retrieval
            logging.error(e, exc_info=True)
            return e.__str__()

        if auth_password == protected_data['password']:
            # If the provided password matches the password in the JSON file
            file_name = request.args['file']
            f = Fernet(key)
            # Encrypt the password and generate a token
            token = f.encrypt(bytes(auth_password, 'utf-8'))[:15]
            return redirect("/build-wbr/publish/protected?file=" + file_name +
                            "&password=" + str(token))
        else:
            # If the provided password does not match the password in the JSON file
            response = app.response_class(
                response=json.dumps({"message": "Unauthorised"}),
                status=403
            )
            return response
    else:
        # If password is not provided in the request arguments
        return render_template("login.html", fileName=file_name)


@app.route('/build-wbr/publish/protected', methods=['GET'])
def build_wbr_protected():
    """
    Builds the protected WBR report, if user is not authenticated to view report user is redirected to login page.
    :return: Rendered WBR html file
    """
    if 'file' in request.args:
        auth_file_name = request.args['file']
        if 'password' not in request.args:
            return redirect('/login?file=' + auth_file_name)
        else:
            protected_data = publisher.download(which_env + "/" + auth_file_name)
            return flask.render_template('wbr_share.html', data=protected_data["data"])


@app.route('/build-wbr/sample', methods=['GET'])
def build_sample_wbr():
    """
    Builds sample WBR files.
    :return: Rendered sample WBR report html file
    """
    filename = request.args['file']
    base_path = str(Path(os.path.dirname(__file__)).parent)
    file = base_path + '/sample/' + filename
    current_file = open(file)
    data = json.load(current_file)
    return flask.render_template('wbr_share.html', data=data)


@app.route("/get_file_name", methods=['GET'])
def get_file_name():
    """
    Retrieve the sample reference files.
    :return: reference files
    """
    data_folder = Path(os.path.dirname(__file__)) / 'web/static/demo_uploads'
    files = os.listdir(data_folder)
    files.sort()
    response = app.response_class(
        response=json.dumps(files, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/publish-protected-report", methods=['POST'])
def publish_protected_wbr():
    """
    Saves the generated WBR report with a password
    :return: Redirect URL for the published report
    """
    # Get the password from the request arguments
    password = request.args['password']

    # Load the JSON data from the request body
    data = json.loads(request.data)

    # Add the password to the JSON data
    protected_data = {"data": data, "password": password}

    # Get the base URL and replace 'http' with 'https'
    base_url = request.base_url.replace('/publish-protected-report', '')
    if "localhost" not in base_url and "127.0.0.1" not in base_url:
        base_url = base_url.replace("http", "https")

    # Generate a unique filename for the JSON data
    filename = str(uuid.uuid4())[21:]

    # Store the JSON data to cloud
    publisher.upload(protected_data, which_env + "/" + filename)

    # Create the response containing the URL to access the protected report
    response = app.response_class(
        response=json.dumps(
            {'path': base_url + '/build-wbr/publish/protected?file=' + filename}, indent=4,
            cls=controller_util.Encoder),
        status=200
    )
    return response


@app.route('/wbr-unit-test', methods=["GET"])
def run_unit_test():
    """
    Unit test endpoint
    :return: Test results
    """
    test_result = test.test_wbr()
    response = app.response_class(
        response=json.dumps(test_result, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )
    return response


def start():
    return app


if __name__ == "__main__":
    app.run(debug=False, port=5001, host='0.0.0.0')
