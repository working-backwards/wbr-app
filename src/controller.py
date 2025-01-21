import io
import json
import logging
import os
import tempfile
import uuid
from pathlib import Path

import flask
import pandas
import requests
from cryptography.fernet import Fernet
from flask import Flask, request, send_file, render_template
from flask_cors import CORS
from werkzeug.utils import redirect

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

    try:
        cfg = controller_util.load_yaml_from_stream(config_file)
    except Exception as e:
        return app.response_class(
            response=json.dumps({"description": e.__str__()}),
            status=500
        )

    try:
        deck = process_input(csv_data_file, cfg)
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"description": e.__str__()}),
            status=500
        )

    # Return the WBR deck as a JSON response
    return app.response_class(
        response=json.dumps(deck, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )


def process_input(data, cfg):
    try:
        wbr_validator = validator.WBRValidator(data, cfg)
        wbr_validator.validate_yaml()
    except Exception as e:
        logging.error("Yaml validation failed", e, exc_info=True)
        raise Exception(f"Invalid configuration provided: {e.__str__()}")

    try:
        # Create a WBR object using the CSV data and configuration
        wbr1 = wbr.WBR(cfg, daily_df=wbr_validator.daily_df)
    except Exception as error:
        logging.error(error, exc_info=True)
        raise Exception(f"Could not create WBR metrics due to: {error.__str__()}")

    try:
        # Generate the WBR deck using the WBR object
        deck = controller_util.get_wbr_deck(wbr1)
    except Exception as err:
        logging.error(err, exc_info=True)
        raise Exception(f"Error while creating deck, caused by: {err.__str__()}")

    return deck



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
def publish_report(url=None, deck=None):
    """
    Fetch JSON file from the HTTP request, save the file to S3 bucket and publish the WBR to a public URL.

    Returns:
        A Flask response object with the URL to access the uploaded data.
    """
    # Parse the JSON data from the request
    data = json.loads(deck or request.data)

    # Modify the base URL to use HTTPS instead of HTTP
    base_url = url or request.base_url.replace('/publish-wbr-report', '')
    if "localhost" not in base_url and "127.0.0.1" not in base_url:
        base_url = base_url.replace("http", "https")

    return publish_and_get(base_url, '/build-wbr/publish?file=', data)


@app.route("/publish-protected-report", methods=['POST'])
def publish_protected_wbr(url=None, deck=None):
    """
    Saves the generated WBR report with a password
    :return: Redirect URL for the published report
    """
    # Get the password from the request arguments
    password = request.args['password']

    # Load the JSON data from the request body
    data = json.loads(deck or request.data)

    # Add the password to the JSON data
    protected_data = {"data": data, "password": password}

    # Get the base URL and replace 'http' with 'https'
    base_url = url or request.base_url.replace('/publish-protected-report', '')
    if "localhost" not in base_url and "127.0.0.1" not in base_url:
        base_url = base_url.replace("http", "https")
    return publish_and_get(base_url, '/build-wbr/publish/protected?file=', protected_data)


def publish_and_get(base_url: str, trailing_url: str, data: list | dict):
    # Generate a unique filename for the JSON data
    filename = str(uuid.uuid4())[25:]
    # Upload the report to cloud storage
    try:
        publisher.upload(data, which_env + "/" + filename)
        # Create a response with the URL to access the uploaded data
        return app.response_class(
            response=json.dumps({'path': f"{base_url}{trailing_url}{filename}"}, indent=4,
                                cls=controller_util.Encoder),
            status=200
        )
    except Exception as e:
        logging.error("Error occurred while publishing the report", e, exc_info=True)
        return app.response_class(
            status=500
        )


@app.route('/build-wbr/publish', methods=['GET'])
def build_wbr():
    """
    Builds unprotected WBR onto the web browser using the already saved WBR report
    :return: Rendered template of already generated report
    """
    filename = request.args['file']
    logging.info(f"Received request to download {filename}")
    try:
        data = publisher.download(which_env + "/" + filename)
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"message": "Failed to download your report!"}),
            status=500
        )
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
            return app.response_class(
                response=json.dumps({"message": "Unauthorised"}),
                status=403
            )
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
    return app.response_class(
        response=json.dumps(files, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )


@app.route('/wbr-unit-test', methods=["GET"])
def run_unit_test():
    """
    Unit test endpoint
    :return: Test results
    """
    test_result = test.test_wbr()
    return app.response_class(
        response=json.dumps(test_result, indent=4, cls=controller_util.Encoder),
        status=200,
        mimetype='application/json'
    )


@app.route('/report', methods=["POST"])
def build_report():
    output_type = request.args["outputType"] if 'outputType' in request.args else None

    # Validate if data file or data file url is present in the request
    if 'dataUrl' not in request.args and 'dataFile' not in request.files:
        return app.response_class(
            response=json.dumps(
                {'error': 'Either dataUrl or dataFile required!'}, indent=4,
                cls=controller_util.Encoder
            ),
            status=400
        )

    # Validate if config file or config file url is present in the request
    if 'configUrl' not in request.args and 'configFile' not in request.files:
        return app.response_class(
            response=json.dumps(
                {'error': 'Either configUrl or configFile required!'}, indent=4,
                cls=controller_util.Encoder
            ),
            status=400
        )

    # Load config
    try:
        cfg = controller_util.load_yaml_from_url(request.args["configUrl"]) \
            if 'configUrl' in request.args else controller_util.load_yaml_from_stream(request.files['configFile'])
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"error": f"Failed to load yaml, due to {e.__str__()}"}),
            status=500
        )

    # Load data
    try:
        data = request.files['dataFile'] if 'dataFile' in request.files \
            else io.StringIO(requests.get(request.args["dataUrl"]).content.decode('utf-8'))
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"error": f"Failed to load the data csv, due to {e.__str__()}"}),
            status=500
        )

    # Load events data
    try:
        events_data = request.files['eventsFile'] if 'eventsFile' in request.files else (
            io.StringIO(requests.get(request.args["eventsFileUrl"]).content.decode('utf-8'))
            if "eventsFileUrl" in request.args else None
        )
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"error": f"Failed to load the events csv, due to {e.__str__()}"}),
            status=500
        )

    # Override the config setup based on the url query parameters
    if 'week_ending' in request.args:
        cfg["setup"]["week_ending"] = request.args["week_ending"]
    if 'week_number' in request.args:
        cfg["setup"]["week_number"] = int(request.args["week_number"])
    if 'title' in request.args:
        cfg["setup"]["title"] = request.args["title"]
    if 'fiscal_year_end_month' in request.args:
        cfg["setup"]["fiscal_year_end_month"] = request.args["fiscal_year_end_month"]
    if 'block_starting_number' in request.args:
        cfg["setup"]["block_starting_number"] = int(request.args["block_starting_number"])
    if 'tooltip' in request.args:
        cfg["setup"]["tooltip"] = bool(request.args["tooltip"])

    try:
        deck = process_input(data, cfg, events_data)
    except Exception as e:
        logging.error(e, exc_info=True)
        return app.response_class(
            response=json.dumps({"error": e.__str__()}),
            status=500
        )

    if output_type == "JSON":
        # Return the WBR deck as a JSON response
        return app.response_class(
            response=json.dumps([deck], indent=4, cls=controller_util.Encoder),
            status=200,
            mimetype='application/json'
        )
    elif output_type == "HTML":
        # Return the WBR deck as a JSON response
        return flask.render_template(
            'wbr_share.html',
            data=json.loads(json.dumps([deck], indent=4, cls=controller_util.Encoder))
        )
    else:
        return publish_protected_wbr(request.base_url.replace('/report', ''),
                                     json.dumps([deck], indent=4, cls=controller_util.Encoder)) \
            if "password" in request.args \
            else publish_report(request.base_url.replace('/report', ''),
                                json.dumps([deck], indent=4, cls=controller_util.Encoder))


def start():
    return app


if __name__ == "__main__":
    app.run(debug=False, port=5001, host='0.0.0.0')
