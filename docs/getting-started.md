# Getting Started

Generate your first WBR deck in under 10 minutes using the sample files
that ship with the app.

## Prerequisites

- Python 3.12+
- Git

## 1. Install and run the app

```bash
git clone https://github.com/working-backwards/wbr-app.git
cd wbr-app
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows
pip install -r requirements.txt
python src/wbr.py
```

Open [http://localhost:5001/wbr.html](http://localhost:5001/wbr.html).

## 2. Download the sample files

Click the hamburger menu (☰) → **Get Reference Samples**.
Download:

- `1-wbr-sample-dataset.csv`
- `1-wbr-sample-config.yaml`

## 3. Generate your first deck

Click ☰ → **Upload & Generate Report**.

1. Select the sample CSV file
2. Select the sample YAML config
3. Click **GENERATE REPORT**

You should see a WBR deck with 6/12 charts, tables, and summary
metrics. See a [sample WBR report](https://workingbackwards.com/wbr-app/sample-wbr-report/) for an example of the output.

## 4. Understand the YAML config

Open `1-wbr-sample-config.yaml` ([view on GitHub](https://github.com/working-backwards/wbr-app/blob/master/src/web/static/demo_uploads/1-wbr-sample-config.yaml)). The file has these sections, in order:

| YAML Section   | Purpose | Docs |
|----------------|---------|------|
| `setup`        | Deck metadata — title, week ending date, options | [setup](yaml/setup.md) |
| `data_sources` | Database queries and CSV file references | [data_sources](yaml/data-sources.md) |
| `annotations`  | Business events to overlay on charts | [annotations](yaml/annotations.md) |
| `metrics`      | Map data columns to named metrics with aggregation rules | [metrics](yaml/metrics.md) |
| `deck`         | Define which blocks (charts/tables) to render | [deck](yaml/deck.md) |

## 5. Publish and share

After generating a deck, click the green **PUBLISH** button in the
top-right corner. The app generates a shareable URL.

See [Publishing & Export](guides/publishing.md) for password
protection and JSON export.

## Next steps

- [Connect to a database](yaml/data-sources.md#database-queries) instead of CSV
- [Add annotations](yaml/annotations.md) to overlay events on charts
- [Auto-generate a YAML config](guides/admin-ui.md#generate-yaml) from your CSV
