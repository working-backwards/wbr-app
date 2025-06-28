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

The WBR App primarily requires the following:

*   **A WBR Configuration File (`.yaml` format):** This file defines the structure of your WBR, including metrics, calculations, and how charts and tables are displayed. Crucially, it now also specifies where to get your data from using a `data_sources` section.
*   **A Database Connections File (`connections.yaml`):** This new file, placed in the root of the project, stores the connection details for your databases (e.g., PostgreSQL, Snowflake, Athena, Redshift). The WBR Configuration File references connections defined here.
*   **(Optional) CSV Data File:** While the primary data source is now expected to be a database, some functionalities like generating an initial YAML configuration might still utilize a CSV as a starting point.

The application generates the WBR Report in an HTML page. You can also download a JSON representation of your WBR Report.

---

### <a name="data-configuration"></a>Data Configuration for Database Connectivity

With the new database connectivity feature, the WBR App fetches data directly from your databases. This requires two main configuration components:

1.  **`connections.yaml` File:**
    *   **Purpose:** Defines all database connection parameters.
    *   **Location:** Must be placed in the root directory of the `wbr-app` project.
    *   **Structure:**
        ```yaml
        # connections.yaml
        version: 1.0
        connections:
          - name: "MyProdPostgres"  # Unique name for this connection
            type: "postgres"        # Database type (postgres, snowflake, athena, redshift)
            description: "Production PostgreSQL DB for core metrics." # Optional
            config:
              host: "your_postgres_host.com"
              port: 5432
              username: "db_user"
              password: "your_pg_password"  # WARNING: For local dev only. Use env vars/secrets for prod.
              database: "metrics_db"

          - name: "AnalyticsSnowflake"
            type: "snowflake"
            config:
              user: "snowflake_user"
              password: "your_sf_password" # WARNING: For local dev only.
              account: "your_snowflake_account_id"
              warehouse: "COMPUTE_WH"
              database: "ANALYTICS_DB"
              schema: "PUBLIC"

          - name: "S3DataLakeAthena"
            type: "athena"
            config:
              # aws_access_key_id: "YOUR_KEY" # Best practice: Use IAM roles or env variables
              # aws_secret_access_key: "YOUR_SECRET" # Best practice: Use IAM roles or env variables
              region_name: "us-east-1"
              s3_staging_dir: "s3://your-athena-query-results-bucket/path/"
              database: "athena_metrics_database"
              workgroup: "primary" # Optional

          - name: "LegacyRedshift"
            type: "redshift"
            config:
              host: "your_redshift_endpoint.redshift.amazonaws.com"
              port: 5439
              username: "redshift_user"
              password: "your_rs_password" # WARNING: For local dev only.
              database: "legacy_dw"
        ```
    *   **Security Note:** Storing plain text passwords in `connections.yaml` is highly discouraged for production environments. For production, use environment variables, AWS Secrets Manager, HashiCorp Vault, or other secure secret management solutions. The connector configurations will need to be adapted to read from these sources if implemented. For local development, this file provides convenience.

2.  **Updating Your WBR Configuration YAML (e.g., `config.yaml`):**
    *   The main WBR configuration file now needs a `data_sources` section to specify which connection to use and what query to run. This replaces the direct CSV file upload for the primary data.
    *   **Structure for `data_sources`:**
        ```yaml
        # Inside your main WBR config.yaml
        setup:
          week_ending: "25-SEP-2021"
          title: "My WBR Report"
          # ... other setup params

        data_sources:
          - name: "main_metrics_query"  # Optional descriptive name for this source
            connection_name: "MyProdPostgres" # MUST match a 'name' in connections.yaml
            date_column: "event_date"     # Column from your query to be used as the primary 'Date'
                                          # This column will be renamed to 'Date' and parsed.
            query: >                      # Your SQL query
              SELECT
                event_date,
                SUM(sales_value) as total_sales,
                COUNT(DISTINCT user_id) as active_users
              FROM
                daily_sales_aggregates
              -- The WBR app's internal logic filters data by week_ending from 'setup'.
              -- You can add further date filters here if needed for performance.
              GROUP BY event_date
              ORDER BY event_date ASC;

        metrics:
          # Metrics are defined based on columns returned by your query in data_sources
          TotalSales:
            column: "total_sales" # Must match a column name from the query result
            aggf: "sum" # This aggregation happens on the already grouped data if query pre-aggregates
                        # or on the raw data if query returns unaggregated daily values.
          ActiveUsers:
            column: "active_users"
            aggf: "sum"
          # ... other metrics

        deck:
          # ... deck configuration
        ```
    *   `connection_name`: Must exactly match one of the `name` attributes defined in your `connections.yaml`.
    *   `date_column`: Tells the WBR app which column from your SQL query results should be treated as the primary date. This column will be automatically parsed as a datetime object and renamed to `Date` internally, which the rest of the WBR logic relies on.
    *   `query`: The SQL query to be executed on the specified database. Ensure this query returns the `date_column` and any other columns your `metrics` definitions will use.

---

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
To create a WBR Report:
1. Ensure you have a `connections.yaml` file in the root of the `wbr-app` project, correctly configured with your database connection details.
2. Prepare your WBR Configuration YAML file. This file must now include a `data_sources` section that references a connection from `connections.yaml` and provides a SQL query.
3. In the WBR App UI, click on the breadcrumb button (menu) to open the side panel.
4. Upload your WBR Configuration YAML file in the "Configuration" input section. (The "Weekly Data" input for CSV is no longer the primary method for data loading).
5. Click on the `Generate Report` button. The app will use the configurations to fetch data from your database and generate the WBR report.

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
This feature helps you create an initial WBR configuration file (`wbr_config.yaml`) based on the columns in an uploaded CSV data file.
**Note:** With the introduction of database connectivity, the generated YAML from this feature serves as a **template**. You will need to manually edit the `data_sources` section to point to your database and provide a valid SQL query. The metrics generated will be based on the CSV columns and may need adjustment to match your query's output columns.

To use this feature:

1. Click on the `Generate YAML` button in the WBR App UI, which will pop up a form.
2. Upload a sample CSV data file that represents the structure of the data you intend to query from your database.
3. Click the `Download` button. A `wbr_config.yaml` file will be downloaded.
4. **Crucially, open and edit this downloaded `wbr_config.yaml` file:**
    *   Modify the placeholder `data_sources` section. You must:
        *   Set `connection_name` to match a connection defined in your `connections.yaml`.
        *   Write the actual `query` to fetch your data.
        *   Ensure `date_column` correctly names the date column from your query.
    *   Review and adjust the `metrics` definitions to ensure the `column` names match the columns returned by your SQL query.
    *   Update `setup` fields like `week_ending`, `week_number`, and `title` as needed.
5. Once edited, this configuration file can be used to generate a WBR report using your database.

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