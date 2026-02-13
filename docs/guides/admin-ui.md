# Admin UI

The WBR App admin interface is at `http://localhost:5001/wbr.html`.
Click the hamburger menu (☰) to access all features.

## Get Reference Samples

Download sample CSV + YAML file pairs to learn the config format. Two
sets are available: a basic example and one demonstrating filters.

## Upload & Generate Report

1. Select your CSV data file
2. Select your YAML config file
3. Click **GENERATE REPORT**

If the config uses `data_sources` and no CSV is needed, you can omit the
CSV file.

## Generate YAML

Upload a CSV file and the app generates a starter YAML config — one
basic metric and one `6_12Graph` per column. Edit the generated file to
customize.

## Upload JSON

Re-render a previously exported JSON file.
