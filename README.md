## Table of Contents
- [Overview](#overview)
- [Getting Started](#getting-started)
    - [Understanding the WBR Framework](#understanding-the-wbr-framework)
    - [Hardware and Software Requirements](#hardware-and-software-requirements)
        - [Hardware](#hardware)
        - [Software](#software)
- [Set up the WBR App](#set-up-the-wbr-app)
    - [Code checkout](#code-checkout)
    - [Running the WBR App](#running-the-wbr-app)
- [Using the WBR App](#using-the-wbr-app)
    - [Features](#features)
        - [Creating the WBR Report](#creating-the-wbr-report)
        - [Downloading the JSON file](#downloading-the-json-file)
        - [Creating the WBR Report from a JSON file](#creating-the-wbr-report-from-a-json-file)
        - [Publishing the WBR Report to a URL](#publishing-the-wbr-report-to-a-url)
        - [Generating a WBR config file](#generating-a-wbr-config-file)
        - [Generating a WBR config file using AI](#generating-a-wbr-config-file-using-ai)
- [Testing](#testing)
- [Additional Information](#additional-information)
- [Toolchain used for developing WBR](#toolchain-used-for-developing-wbr)
- [License](#license)
- [Helpful Links](#helpful-links)


## Overview

The WBR App is a web application that takes your organization's business data and builds an HTML-based report
called a WBR Report, so that you can implement an Amazon-style WBR process in your organization.

Most data visualization applications such as Tableau and Looker cannot generate an Amazon-style WBR Report
out of the box. The WBR App was created to reduce the effort it takes to ingest, transform, and visualize
your business data, so you can focus on improving your business performance each week. In addition to
the HTML WBR Report, the WBR App also produces a JSON version of your WBR Report, so you can quickly import
the transformed data without reprocessing it.

The WBR Report allows you to quickly review hundreds of metrics and easily spot noteworthy variances
in the data. A well-constructed WBR provides a comprehensive, data-driven answer to the following
three questions each week:   
* How did the business do last week?  
* Are we on track to hit our targets?  
* What did our customers experience last week? 

This document explains how to install an instance of the WBR App by cloning the code repository and running it locally in your own environment. Additionally, you can customize the source code based on your needs and even contribute to the WBR App Git repository.


## Getting Started

To get started, you'll need to install the necessary software and run the application on your computer. The following steps will guide you through this.

### Understanding the WBR Framework

The WBR App requires the following input files:     
* A metrics file in **.csv** format  
* A configuration file in **.yaml** format  

The application generates the WBR Report in an HTML page. You can also download a JSON representation of your WBR Report.

### Hardware and Software Requirements
#### Hardware
* Processor - 4 Core Processor (Minimum)  
* RAM size - 4GB RAM (Minimum)  

#### Software
* Python 3.12.x
* git 2.41.0 


## Set up the WBR App

### Code checkout
#### Mac / Linux
1. Open your terminal.
2. Navigate to the directory where you want to store the project.
3. To get a copy of the application's code on your computer, run the following command to clone (download) the repository.  
    ```bash
    git clone https://github.com/working-backwards/wbr-app.git
    ```
4. Navigate into the project directory:  
    ```bash
    cd wbr-app
    ```
   Now, you have a local copy of the project on your machine!

#### Windows
1. Open command prompt.  
2. Navigate to the directory where you want to store the project.
3. Run the following command to clone the repository:  
    ```bash
    git clone https://github.com/working-backwards/wbr-app.git
    ```
4. Navigate into the project directory:  
    ```bash
    cd wbr-app
    ```
   Now, you have a local copy of the project on your machine!

### Running the WBR App
#### Mac / Linux
1. **Set Up a Python Virtual Environment**  
    Create a virtual environment in the `wbr-app` directory by running the following command:
      ```bash
      python3.12 -m venv "venv"
      ```
2. **Activate the Virtual Environment**  
    Activate the virtual environment by running:
      ```bash
      source venv/bin/activate
      ```
3. **Install Dependencies**  
    Once the virtual environment is active, install the required packages:
      ```bash
      pip install -r requirements.txt
      ```
4. **Run the Application**
    Start the app with the following command:
      ```bash
      waitress-serve --port=5001 --call src.controller:start
      ```
   After successfully running the command, you should see output similar to the following  
    ```
    INFO:waitress:Serving on http://0.0.0.0:5001
    ```
   Note: you might encounter port conflicts, so feel free to change the port.

Once the command is successful, the application will be running, and you can open your web browser and navigate to `http://localhost:5001/wbr.html` to view the WBR App.

#### Windows
1. **Set Up a Python Virtual Environment**  
    Create a virtual environment in the `wbr-app` directory by running the following command:
      ```bash
      py -m venv venv
      ```
2. **Activate the Virtual Environment**  
    To activate the virtual environment
   - Change to the venv\Scripts directory:
       ```bash
       cd venv\Scripts
       ```
   - Type activate and press enter to activate the environment:
       ```bash
       activate
       ```
3. **Return to the Project Directory**  
    Navigate back to the `wbr-app` directory:
      ```bash
      cd ..\..\
      ```
4. **Install Dependencies**  
    Once the virtual environment is active, install the required packages:
      ```bash
      pip install -r requirements.txt
      ```
5. **Run the Application**
    Run the app with the following command:
      ```bash
      waitress-serve --port=5001 --call src.controller:start
      ```
   After successfully running the command, you should see output similar to the following  
    ```
    INFO:waitress:Serving on http://0.0.0.0:5001
    ```
   Note: you might encounter port conflicts, so feel free to change the port.

Once the command is run successful, the application will be running, and you can open your web browser and navigate to `http://localhost:5001/wbr.html` to view the WBR App.


## Using the WBR App
To access the WBR App route your browser to `http[s]://<domain>/wbr.html`.

### Features
#### Creating the WBR Report
1. Click on the breadcrumb button which will open a side menu.
2. Upload the dataset csv file in the Weekly Data input section.
3. Upload the config yaml file in the Configuration input section.
4. Click on the `Generate Report` button to generate the WBR report.

#### Downloading the JSON file
To download the JSON file, follow the below steps,

1. When a WBR report is generated a JSON button will be displayed on the application.
2. Clicking on the button will download the report's JSON file on to your browser

#### Creating the WBR Report from a JSON file
This helps you create a WBR report on your browser without the data file and/or the config file. To accomplish this please follow the below steps.

1. To create a WBR report with a JSON file you should have a valid WBR supported JSON file.
2. In the side menu when clicked on `Upload JSON and generate report` link a form will be popped which will accept a valid JSON file.
3. After uploading the JSON file click the `Upload` button to generate a WBR report.

#### Publishing the WBR Report to a URL
This feature lets you publish a report, you can also publish the report with the password protection, follow the below steps to accomplish this,  

1. When the report is generated, a `PUBLISH` button is displayed on the application.  
2. When clicked on that button, a form will be popped which will let you publish the report to a URL.  
3. If you want to add the password protection to your report then click on the checkbox and publish the report or else you can just publish the report.  
4. When you click the `PUBLISH` button in the form the report will be persisted, and a URL will be generated for it.  
5. To view the report, copy the published URL, paste it into your web browser, and the report will be displayed.  

NOTE:  
If you have published the URL with password protection, the application will ask you to enter the password before you render the report onto your browser.  

**The reports will be saved to the local project directory named `publish`, if you want to save the reports to the cloud storages like s3 or gcp or azure cloud you need to use the following environment variables**

- **ENVIRONMENT**:  
  *Optional*. Specifies the environment and saves the files within this particular environment directory.

- **OBJECT_STORAGE_OPTION**:  
  *Optional*. Specifies the cloud storage service for saving reports. Supported values are `s3`, `gcp`, `azure`. If not provided, reports will be saved to the local project directory.

- **OBJECT_STORAGE_BUCKET**:  
  This is required only if you have set the **OBJECT_STORAGE_OPTION** environment variable. If you are using OBJECT_STORAGE_OPTION as `s3` or `gcp` or `azure` you must first create your own storage bucket. Set OBJECT_STORAGE_BUCKET equal to either S3 bucket name or GCP bucket name or Azure storage container name. 

Depending on your cloud provider, define the following environment variables   

###### **S3 (Amazon Web Services)**  
  * **S3_STORAGE_KEY**: Your AWS Access Key ID.  
  * **S3_STORAGE_SECRET**: Your AWS Secret Access Key.  
  * **S3_REGION_NAME**: The AWS region where your bucket is hosted.  
  * **S3_STORAGE_ENDPOINT**: [Optional] Specifies the endpoint where reports will be stored. If not provided, reports will be published to the `OBJECT_STORAGE_BUCKET`.  
    *Alternatively*, you can use the IAM that have been set up for your local system instead of setting these environment variables.  
    *NOTE*: You can also use the same environment variables for any S3 compatible storage.  
   

###### **GCP (Google Cloud Platform)**  
  * **GCP_SERVICE_ACCOUNT_PATH**: Provide the JSON token file path that you downloaded from the Google Cloud.  
  * **GCLOUD_PROJECT**: If you are using IAM to authenticate then you need to set this environment variable with the project id for the cloud storage.  

###### **Azure (Microsoft Azure)**  
  * **AZURE_CONNECTION_STRING**: The connection string from your Azure Storage Account's access keys, while setting this environment variable please encompass the value within double quotes.  
  * **AZURE_ACCOUNT_URL**: If you are using IAM to authenticate then you need to set this environment variable with the project id for the cloud storage.  

To set the environment variables on your system use the following syntax  
  ```bash
  # Mac / linux:
    export VARIABLE_NAME=value
  # Windows:
    SET VARIABLE_NAME=value
  ```

After setting the above environment variables you need to rerun the application using the following command
  ```bash
    waitress-serve --port=5001 --call src.controller:start
  ```

#### Generating a WBR config file
This feature will help you create a config file, considering all the numeric column to be a metric from your data CSV file and applying `sum` as the default aggregation method. 
To accomplish this follow the below steps,

1. Click on the `Generate YAML` button which will pop up a form.
2. Upload the CSV data file 
3. Click on `Download` button, a `wbr_config.yaml` file will be downloaded on to your browser.
4. You will have to change the `week_ending` and `week_number` fields according to your needs. You can also make changes to other fields according to your needs
5. You will be able to generate a WBR report using this config file.

#### Generating a WBR config file using AI
We have a feature where you can install our AI plugin to generate the config file using the same instructions as above.
To install the plugin you will have do the following.

1. Clone the repository https://github.com/working-backwards/wbr-ai-yaml-generator.
2. Follow the instruction to build and install the plugin from the README.md of the same repository.
3. Once you have completed the installation of the plugin add the environment variables OPENAI_API_KEY and ORGANISATION_ID.
4. Run the [controller.py](src%2Fcontroller.py)

After this plugin is installed, the AI yaml generator replaces the default rules-based yaml generator.

##### To use this feature you need to set the following environment variables,  
- **OPENAI_API_KEY**: Your API key provided by OpenAI.
- **ORGANISATION_ID**: Your organisation ID provided by OpenAI.

To set the environment variables on your system use the following syntax  
  ```bash
  # Mac / linux:
    export VARIABLE_NAME=value
  # Windows:
    SET VARIABLE_NAME=value
  ```

After setting the above environment variables you need to rerun the application using the following command
  ```bash
    waitress-serve --port=5001 --call src.controller:start
  ```


## Testing
Access our automated test suite, which scans test input files from the directory, `src/unit_test_case`.
The test suite iterates through all scenarios from all the `scenario` folders, generates WBR reports, and compares results with the **testconfig.yml** file.

**Note**: Each scenario folder must contain the **original.csv** (data), **config.yaml** (configuration), and a **testconfig.yml** (expected result) files.

A web user interface is been developed to run the test cases, route your browser to `http[s]://<domain>/unit_test_wbr.html` and click `Run Unit Tests` button.


## Additional Information
* For queries on customizing or building additional WBR metrics, contact [developers@workingbackwards.com]().

## API Documentation
For detailed information on how to use the WBR App's API, please refer to the [API Documentation](docs/API_DOCUMENTATION.md). This document provides comprehensive details on the available endpoints, request parameters, and response formats.


## Toolchain used for developing WBR
* [Python 3.12.x compiler](https://www.python.org/)
* [PIP](https://pypi.org/project/pip/) Python Package Management System
* [JetBrains PyCharm Community Edition](https://www.jetbrains.com/pycharm/) IDE


## License
The WBR App is developed and maintained by the Working Backwards engineering team.
The WBR App is released under the MIT License (https://opensource.org/licenses/MIT).
For more information on licensing, refer to the **LICENSE.md** file.


## Helpful Links
[workingbackwards.com](https://workingbackwards.com "Website about Amazon business processes including WBR.")