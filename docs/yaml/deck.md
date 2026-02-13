# deck

The `deck` section defines the visual blocks in your WBR report. Blocks
are rendered in order, top to bottom.

```yaml
deck:
  - block:
      ui_type: 6_12Graph
      ...
  - block:
      ui_type: 6_WeeksTable
      ...
```

## Block types

| `ui_type` | Description |
|-----------|-------------|
| [`6_12Graph`](#6_12graph) | Trailing 6 weeks + 12 months chart with summary table |
| [`6_WeeksTable`](#6_weekstable) | Trailing 6 weeks data table |
| [`12_MonthsTable`](#12_monthstable) | Trailing 12 months data table |
| [`section`](#section-break) | Section header or divider |
| [`embedded_content`](#embedded-content) | iframe from external URL |

---

## 6_12Graph

The signature WBR visualization: trailing 6 weeks + trailing 12 months
on a shared x-axis, with a summary table (last week, MTD, QTD, YTD, and
period-over-period comparisons).

![6_12Graph example](../images/6-12-graph.png)

```yaml
- block:
    ui_type: 6_12Graph
    title: Ad Impressions (Millions)
    y_scaling: "##.2MM"
    x_axis_monthly_display: trailing_twelve_months
    metrics:
      Impressions:
        line_style: primary
        graph_prior_year_flag: true
        legend_name: Impressions
```

### Block-level parameters

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `title` | String | — | Text at the top of the block |
| `y_scaling` | [Format string](../reference/y-scaling.md) | — | Y-axis number formatting |
| `x_axis_monthly_display` | `trailing_twelve_months`, `fiscal_year` | `trailing_twelve_months` | Monthly axis range |

### Per-metric parameters

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `line_style` | `primary`, `secondary`, `target` | `primary` | Visual prominence. `target` shows markers only (no line). |
| `graph_prior_year_flag` | `true`, `false` | `true` for primary/secondary | Overlay prior year data |
| `legend_name` | String | Metric name | Label in chart legend |

Recommend max 3 metrics per chart (primary + secondary + target).

### Multi-metric example

```yaml
- block:
    ui_type: 6_12Graph
    title: Total Page Views (Millions)
    y_scaling: "##MM"
    metrics:
      PageViews:
        graph_prior_year_flag: true
        legend_name: Page Views
      PageViews Target:
        line_style: target
        graph_prior_year_flag: false
        legend_name: Page Views - Target
      MobilePage_Views:
        line_style: secondary
        graph_prior_year_flag: true
        legend_name: Mobile Page Views
```

---

## 6_WeeksTable

![6_WeeksTable example](../images/6-weeks-table.png)

Trailing 6 weeks of data plus QTD and YTD. Define each row explicitly.

```yaml
- block:
    ui_type: 6_WeeksTable
    title: "Page Views Actual vs Plan Summary"
    rows:
      - row:
          header: "Page Views"
          style: "font-weight: bold; background-color: LightGrey; text-align:left;"
      - row:
          header: "Actual"
          metric: PageViews
          style: "text-align:right;"
          y_scaling: "##MM"
      - row:
          header: "YOY"
          metric: PageViewsYOY
          style: "font-style: italic; text-align:right;"
          y_scaling: "##.1%"
```

### Row parameters

| Field | Required | Description |
|-------|----------|-------------|
| `header` | Yes | Row label. Leave blank for an empty spacer row. |
| `metric` | No | Metric name. Omit to create a section header row. |
| `y_scaling` | No | [Number format](../reference/y-scaling.md) for this row's values |
| `style` | No | CSS properties applied to the header cell (e.g., `font-weight: bold; text-align: right;`) |

---

## 12_MonthsTable

![12_MonthsTable example](../images/12-months-table.png)

Same structure as `6_WeeksTable` but shows trailing 12 months.
Also supports `x_axis_monthly_display`.

```yaml
- block:
    ui_type: 12_MonthsTable
    title: "Page Views Actual vs Plan Summary"
    x_axis_monthly_display: trailing_twelve_months
    rows:
      - row:
          header: "Actual"
          metric: PageViews
          style: "text-align:right;"
          y_scaling: "##MM"
```

!!! tip
    `6_WeeksTable` and `12_MonthsTable` work best when displayed
    together side-by-side for the same metrics.

---

## Section break

```yaml
- block:
    ui_type: section
    title: "Customer Experience"    # or "" for a plain divider
```

---

## Embedded content

Display external content in an iframe.

```yaml
- block:
    ui_type: embedded_content
    source: "https://your-dashboard-url"
    height: 700px
    width: 2000px
```

Use for Looker dashboards, Google Sheets, Tableau, static images, etc.
