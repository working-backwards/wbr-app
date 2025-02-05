# API Documentation:

## **Endpoint**
`POST /report`

## **Description**
This API endpoint generates a report based on the provided CSV data and YAML configuration. It supports HTML, JSON, or a custom response format for the generated report.

---

## **Request Parameters**

| Parameter               | Location  | Type    | Required | Description                                                                                    |
|-------------------------|-----------|---------|----------|------------------------------------------------------------------------------------------------|
| `dataUrl`               | Query     | String  | Optional | URL of the CSV file to be used for report generation. Either `dataUrl` or `dataFile` required. |
| `dataFile`              | Form-Data | File    | Optional | CSV file to be uploaded directly. Either `dataUrl` or `dataFile` required.                     |
| `configUrl`             | Query     | String  | Optional | URL of the YAML configuration file. Either `configUrl` or `configFile` required.               |
| `configFile`            | Form-Data | File    | Optional | YAML configuration file to be uploaded directly. Either `configUrl` or `configFile` required.  |
| `outputType`            | Query     | String  | Optional | Specifies the output format. Accepted values: `HTML` or `JSON`. Defaults to custom response.   |
| `week_ending`           | Query     | String  | Optional | Specifies the week-ending date to override the YAML setup parameter.                           |
| `week_number`           | Query     | String  | Optional | Specifies the week number to override the YAML setup parameter.                                |
| `title`                 | Query     | String  | Optional | Specifies the report title to override the YAML setup parameter.                               |
| `fiscal_year_end_month` | Query     | String  | Optional | Specifies the fiscal year-end month to override the YAML setup parameter.                      |
| `block_starting_number` | Query     | Integer | Optional | Specifies the starting number for block numbering in the report.                               |
| `tooltip`               | Query     | String  | Optional | Specifies a tooltip to override the YAML setup parameter.                                      |
| `password`              | Query     | String  | Optional | Password for your published report.                                                            |
---

## **Request Body**
- **Optional**: YAML file content (if not using `configUrl`).
- **Optional**: CSV file content (if not using `dataUrl`).

---

## **Response**

### **Success Responses**
1. **JSON Output**  
   - **Status Code**: `200 OK`  
   - **Body**: JSON representation of the generated report.  

2. **HTML Output**  
   - **Status Code**: `200 OK`  
   - **Body**: Rendered HTML report.  

3. **Custom Response**  
   - **Status Code**: `200 OK`  
   - **Body**: Published report URL or generated content, depending on implementation.

### **Error Responses**
1. **Missing CSV Data**  
   - **Status Code**: `400 Bad Request`  
   - **Body**:
     ```json
     {
         "error": "Either dataUrl or dataFile required!"
     }
     ```

2. **Missing YAML Configuration**  
   - **Status Code**: `400 Bad Request`  
   - **Body**:
     ```json
     {
         "error": "Either configUrl or configFile required!"
     }
     ```

3. **YAML Parsing Error**  
   - **Status Code**: `500 Internal Server Error`  
   - **Body**:
     ```json
     {
         "error": "Error description for YAML parsing issue."
     }
     ```

4. **Validation Error**  
   - **Status Code**: `500 Internal Server Error`  
   - **Body**:
     ```json
     {
         "error": "Invalid configuration provided: <error details>"
     }
     ```

5. **Report Generation Error**  
   - **Status Code**: `500 Internal Server Error`  
   - **Body**:
     ```json
     {
         "error": "Error while creating deck, caused by: <error details>"
     }
     ```

---

## **Examples**

### **Request Example**
Using `curl`:
```bash
curl -X POST https://<domain>/report \
-H "Content-Type: multipart/form-data" \
-d "configUrl=https://put-your-config-url.yaml" \
-d "dataUrl=https://put-your-data-url.yaml" \
-d "week_ending=18-SEP-2021" \
-d "week_number=37" \
-d "outputType=JSON" \
```

### **Response Example (JSON Output)**
```json
[
  {
    "blocks": [
      {
        "plotStyle": "6_12_chart",
        "title": "Total Page Views",
        "yLabel": "",
        "yScale": "##MM",
        "boxTotalScale": "%",
        "axes": 2,
        "xAxis": ["wk 33", "wk 34", "wk 35", "wk 36", "wk 37", "wk 38", " ", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"],
        "yAxis": [
          {
            "lineStyle": "primary",
            "legendName": "Page Views",
            "metric": {
              "current": [
                {
                  "primaryAxis": [496725868.0, 499671126.0, 464148871.0, 457477195.0, 460207741.0, 470759324.0, "", "", "", "", "", "", "", "", "", "", "", "", ""]
                },
                {
                  "secondaryAxis": ["", "", "", "", "", "", "", 1708890199.0, 1807907694.0, 1820822982.0, 1922478407.0, 2318880564.0, 1969227848.0, 2117874905.0, 2036021000.0, 2057839090.0, 2051219932.0, 2274329968.0, 2207577942.0]
                }
              ]
            }
          }
        ]
      }
    ],
    "title": "WBR Daily",
    "weekEnding": "25 September 2021",
    "blockStartingNumber": 2,
    "xAxisMonthlyDisplay": null,
    "eventErrors": null
  }
]
```

### **Response Example (HTML Output)**
```html
<!DOCTYPE html>
<html lang="en">
    <head>
    </head>
    <body>
        <div>
            <div id="charts" class="contentHolder"></div>
        </div>
    </body>
</html>
```

### **Response Example (URL Output)**
```json
{
  "path": "https://<domain>/build-wbr/publish?file=<uniqueFileName>"
}
```

## Notes
- Ensure that either csvUrl or csvfile is provided for the data source.
- YAML configuration can be supplied via yamlUrl or configfile.
- Errors during YAML validation or report generation will return detailed error messages in the response.